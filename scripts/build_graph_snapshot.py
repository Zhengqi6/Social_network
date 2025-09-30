#!/usr/bin/env python3
"""
Build graph snapshot files from collected JSON under data/.

Outputs under data/graph/:
  - nodes_accounts.csv: account nodes (profile_id)
  - edges_follows.csv: user->user follow edges with timestamps
  - edges_engagements.csv: user->post engagement edges with timestamps and types

This script is idempotent and will merge all matching files under data/.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_DIR = DATA_DIR / "graph"


def _load_many(glob_pat: str) -> List[dict]:
    rows: List[dict] = []
    for p in sorted(DATA_DIR.glob(glob_pat)):
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                rows.extend([r for r in data if isinstance(r, dict)])
        except Exception:
            continue
    return rows


def build_nodes_accounts() -> pd.DataFrame:
    rows = _load_many("lens_profiles_*.json")
    # also accept partial profiles
    rows += _load_many("partial_profiles_*.json")
    if not rows:
        return pd.DataFrame(columns=["account_address", "handle", "created_at"])
    df = pd.DataFrame(rows)
    # normalize columns
    if "profile_id" in df.columns:
        df["account_address"] = df["profile_id"]
    else:
        # best-effort: try owned_by
        df["account_address"] = df.get("owned_by")
    df["handle"] = df.get("handle")
    df["created_at"] = df.get("created_at")
    df = df.dropna(subset=["account_address"]).drop_duplicates(subset=["account_address"]) \
           [["account_address", "handle", "created_at"]]
    return df


def build_edges_follows() -> pd.DataFrame:
    rows = _load_many("lens_follows_*.json")
    # partial follows if present
    rows += _load_many("partial_follows_*.json")
    if not rows:
        return pd.DataFrame(columns=["src", "dst", "edge_type", "timestamp"])
    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "follower_address": "src",
        "following_address": "dst",
        "followed_on": "timestamp",
    })
    df["edge_type"] = "FOLLOW"
    keep = [c for c in ["src", "dst", "edge_type", "timestamp"] if c in df.columns]
    df = df[keep]
    df = df.dropna(subset=["src", "dst"]).drop_duplicates()
    return df


def build_edges_engagements() -> pd.DataFrame:
    rows = _load_many("lens_engagements_*.json")
    # include probe outputs as well
    rows += _load_many("lens_engagements_probe_*.json")
    if not rows:
        return pd.DataFrame(columns=["src", "post_id", "edge_type", "timestamp", "ref_post_id"])
    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "user_address": "src",
    })
    keep = [c for c in ["src", "post_id", "ref_post_id", "engagement_type", "timestamp"] if c in df.columns]
    df = df[keep]
    df = df.rename(columns={"engagement_type": "edge_type"})
    df = df.dropna(subset=["src", "post_id"]).drop_duplicates()
    return df


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    nodes = build_nodes_accounts()
    follows = build_edges_follows()
    eng = build_edges_engagements()

    nodes_out = OUT_DIR / "nodes_accounts.csv"
    follows_out = OUT_DIR / "edges_follows.csv"
    eng_out = OUT_DIR / "edges_engagements.csv"

    nodes.to_csv(nodes_out, index=False)
    follows.to_csv(follows_out, index=False)
    eng.to_csv(eng_out, index=False)

    print({
        "nodes_accounts": str(nodes_out),
        "num_accounts": int(nodes.shape[0]),
        "edges_follows": str(follows_out),
        "num_follows": int(follows.shape[0]),
        "edges_engagements": str(eng_out),
        "num_engagements": int(eng.shape[0]),
    })


if __name__ == "__main__":
    main()



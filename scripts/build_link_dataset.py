#!/usr/bin/env python3
"""
Build a minimal link prediction dataset from Lens Parquet snapshots.

Inputs (latest partitions):
  data/lens/follows/dt=*/follows.parquet
  data/lens/profiles/dt=*/profiles.parquet (optional)

Outputs:
  data/miniset/link/train.csv
  data/miniset/link/test.csv

Each CSV has columns:
  follower, following, y, cn, jaccard, adar, ra, pref_att, deg_u, deg_v
"""
from __future__ import annotations
import os
from pathlib import Path
from typing import Tuple
import pandas as pd
import numpy as np
import networkx as nx

ROOT = Path(__file__).resolve().parents[1]


def _latest_partition(base: Path) -> Path:
    if not base.exists():
        raise FileNotFoundError(f"Not found: {base}")
    parts = [p for p in base.glob("dt=*") if p.is_dir()]
    if not parts:
        raise FileNotFoundError(f"No dt=* partitions under {base}")
    return sorted(parts)[-1]


def load_follows() -> pd.DataFrame:
    base = ROOT / "data" / "lens" / "follows"
    part = _latest_partition(base)
    df = pd.read_parquet(part / "follows.parquet")
    # normalize columns
    cols = {c.lower(): c for c in df.columns}
    # expected follower_address, following_address, followed_on
    if "follower_address" not in df.columns:
        # try infer
        pass
    # ensure timestamp parsing
    if "followed_on" in df.columns:
        df["followed_on"] = pd.to_datetime(df["followed_on"], errors="coerce")
    else:
        df["followed_on"] = pd.NaT
    return df[["follower_address", "following_address", "followed_on"]].dropna(subset=["follower_address", "following_address"]).drop_duplicates()


def build_graph(df: pd.DataFrame) -> nx.Graph:
    # undirected for similarity metrics
    g = nx.Graph()
    g.add_nodes_from(pd.unique(pd.concat([df["follower_address"], df["following_address"]], ignore_index=True)))
    g.add_edges_from(df[["follower_address", "following_address"]].itertuples(index=False, name=None))
    return g


def time_split(df: pd.DataFrame, test_ratio: float = 0.2) -> Tuple[pd.DataFrame, pd.DataFrame]:
    d = df.copy()
    d = d.sort_values(by=["followed_on"], kind="stable")
    n_test = max(1, int(len(d) * test_ratio))
    test = d.tail(n_test)
    train = d.drop(test.index)
    return train, test


def negative_samples(g: nx.Graph, pos_df: pd.DataFrame, num_neg: int) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    users = list(g.nodes)
    neg_rows = []
    existing = set(map(tuple, pos_df[["follower_address", "following_address"]].itertuples(index=False, name=None)))
    tried = 0
    while len(neg_rows) < num_neg and tried < num_neg * 50:
        u = users[rng.integers(0, len(users))]
        v = users[rng.integers(0, len(users))]
        tried += 1
        if u == v:
            continue
        if g.has_edge(u, v) or (u, v) in existing:
            continue
        neg_rows.append((u, v))
    neg = pd.DataFrame(neg_rows, columns=["follower_address", "following_address"])
    neg["y"] = 0
    return neg


def pair_features(g: nx.Graph, pairs: pd.DataFrame) -> pd.DataFrame:
    # Pre-compute degree
    deg = dict(g.degree())
    pairs = pairs.copy()
    pairs["deg_u"] = pairs["follower_address"].map(deg).fillna(0)
    pairs["deg_v"] = pairs["following_address"].map(deg).fillna(0)

    # Build ebunch
    ebunch = list(zip(pairs["follower_address"], pairs["following_address"]))

    # Common neighbors count
    cn = []
    for u, v in ebunch:
        try:
            cn.append(len(list(nx.common_neighbors(g, u, v))))
        except nx.NetworkXError:
            cn.append(0)
    pairs["cn"] = cn

    # Jaccard coefficient
    jacc = { (u,v): p for u,v,p in nx.jaccard_coefficient(g, ebunch) }
    pairs["jaccard"] = [jacc.get((u,v), 0.0) for u,v in ebunch]

    # Adamic-Adar
    adar = { (u,v): p for u,v,p in nx.adamic_adar_index(g, ebunch) }
    pairs["adar"] = [adar.get((u,v), 0.0) for u,v in ebunch]

    # Resource allocation
    ra = { (u,v): p for u,v,p in nx.resource_allocation_index(g, ebunch) }
    pairs["ra"] = [ra.get((u,v), 0.0) for u,v in ebunch]

    # Preferential attachment
    pa = { (u,v): p for u,v,p in nx.preferential_attachment(g, ebunch) }
    pairs["pref_att"] = [pa.get((u,v), 0.0) for u,v in ebunch]

    return pairs


def main():
    out_dir = ROOT / "data" / "miniset" / "link"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_follows()
    # downsample for a tiny set if too large
    if len(df) > 2000:
        df = df.sample(2000, random_state=42)

    g = build_graph(df)
    train_pos, test_pos = time_split(df, test_ratio=0.2)
    train_pos = train_pos[["follower_address", "following_address"]].copy(); train_pos["y"] = 1
    test_pos = test_pos[["follower_address", "following_address"]].copy(); test_pos["y"] = 1

    # negatives
    train_neg = negative_samples(g, train_pos, num_neg=len(train_pos))
    test_neg = negative_samples(g, test_pos, num_neg=len(test_pos))

    train = pd.concat([train_pos, train_neg], ignore_index=True)
    test = pd.concat([test_pos, test_neg], ignore_index=True)

    # features
    train = pair_features(g, train)
    test = pair_features(g, test)

    train.to_csv(out_dir / "train.csv", index=False)
    test.to_csv(out_dir / "test.csv", index=False)
    print(f"Wrote {out_dir / 'train.csv'} and {out_dir / 'test.csv'}")


if __name__ == "__main__":
    main()



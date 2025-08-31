#!/usr/bin/env python3
"""
Convert JSON snapshots produced by Lens collector into partitioned Parquet files.

Input (under data/):
  - lens_profiles_YYYYMMDD_HHMMSS.json (list of dicts)
  - lens_publications_YYYYMMDD_HHMMSS.json
  - lens_follows_YYYYMMDD_HHMMSS.json (optional)
  - lens_engagements_YYYYMMDD_HHMMSS.json (optional)

Output:
  - data/lens/profiles/dt=YYYYMMDD_HHMMSS/*.parquet
  - data/lens/posts/dt=YYYYMMDD_HHMMSS/*.parquet
  - data/lens/follows/dt=YYYYMMDD_HHMMSS/*.parquet
  - data/lens/engagements/dt=YYYYMMDD_HHMMSS/*.parquet
"""
import argparse
import json
import os
import re
from pathlib import Path
from typing import Optional

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_BASE = ROOT / "data" / "lens"


def _find_latest(pattern: str) -> Optional[Path]:
    files = sorted(DATA_DIR.glob(pattern))
    return files[-1] if files else None


def _timestamp_from_name(path: Path) -> str:
    m = re.search(r"(\d{8}_\d{6})", path.name)
    return m.group(1) if m else "unknown"


def _load_json_array(path: Path) -> pd.DataFrame:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return pd.DataFrame(data)
    # some files could be dict-like outputs
    return pd.DataFrame(data if isinstance(data, list) else [])


def convert_one(input_path: Path, kind: str) -> Path:
    ts = _timestamp_from_name(input_path)
    sub = {
        "profiles": "profiles",
        "publications": "posts",
        "follows": "follows",
        "engagements": "engagements",
    }[kind]
    out_dir = OUT_BASE / sub / f"dt={ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = _load_json_array(input_path)
    # Normalize nested columns for common fields
    if kind == "publications":
        # author.username.localName → author_username
        if "author" in df.columns:
            df["author_address"] = df["author"].apply(lambda x: (x or {}).get("address") if isinstance(x, dict) else None)
            df["author_username"] = df["author"].apply(lambda x: ((x or {}).get("username") or {}).get("localName") if isinstance(x, dict) else None)
        # keep id/timestamp/contentUri
        keep = [c for c in ["id", "timestamp", "contentUri", "author_address", "author_username", "__typename"] if c in df.columns]
        if keep:
            df = df[keep]
    elif kind == "profiles":
        keep = [c for c in ["profile_id", "handle", "name", "bio", "owned_by", "created_at"] if c in df.columns]
        if keep:
            df = df[keep]
    elif kind == "follows":
        keep = [c for c in ["follower_address", "following_address", "following_handle", "followed_on"] if c in df.columns]
        if keep:
            df = df[keep]
    elif kind == "engagements":
        keep = [c for c in ["user_address", "post_id", "ref_post_id", "engagement_type", "timestamp"] if c in df.columns]
        if keep:
            df = df[keep]
        # timestamp normalize
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    out_file = out_dir / f"{kind}.parquet"
    df.to_parquet(out_file, index=False)
    return out_file


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profiles", type=str, default=None, help="Path to lens_profiles_*.json (optional)")
    parser.add_argument("--publications", type=str, default=None, help="Path to lens_publications_*.json (optional)")
    parser.add_argument("--follows", type=str, default=None, help="Path to lens_follows_*.json (optional)")
    parser.add_argument("--engagements", type=str, default=None, help="Path to lens_engagements_*.json (optional)")
    args = parser.parse_args()

    prof = Path(args.profiles) if args.profiles else _find_latest("lens_profiles_*.json")
    pubs = Path(args.publications) if args.publications else _find_latest("lens_publications_*.json")
    foll = Path(args.follows) if args.follows else _find_latest("lens_follows_*.json")
    eng = Path(args.engagements) if args.engagements else _find_latest("lens_engagements_*.json")

    if prof and prof.exists():
        out = convert_one(prof, "profiles")
        print(f"profiles → {out}")
    else:
        print("profiles: no input found")

    if pubs and pubs.exists():
        out = convert_one(pubs, "publications")
        print(f"publications → {out}")
    else:
        print("publications: no input found")

    if foll and foll.exists():
        out = convert_one(foll, "follows")
        print(f"follows → {out}")
    else:
        print("follows: no input found")

    if eng and eng.exists():
        out = convert_one(eng, "engagements")
        print(f"engagements → {out}")
    else:
        print("engagements: no input found")


if __name__ == "__main__":
    main()



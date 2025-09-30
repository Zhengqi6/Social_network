#!/usr/bin/env python3
"""
Probe Lens engagements (reactions/bookmarks/collect/comments) for a few recent posts
and save results into data/lens_engagements_probe_*.json.
"""
import asyncio
import json
from pathlib import Path
from datetime import datetime

import sys


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def find_latest_publications() -> Path | None:
    root = project_root()
    candidates = sorted((root / "data").glob("lens_publications_*.json"))
    if candidates:
        return candidates[-1]
    # Fallback to home-level data dir if present
    home_data = Path("/home/Siunaus/data")
    candidates = sorted(home_data.glob("lens_publications_*.json"))
    return candidates[-1] if candidates else None


async def run_probe(max_posts: int = 10, per_limit: int = 25) -> dict:
    # Late import to avoid path issues
    sys.path.append(str(project_root()))
    from data_collection.blockchain.lens_collector import LensCollector

    pub_path = find_latest_publications()
    if not pub_path or not pub_path.exists():
        return {"error": "NO_PUBLICATIONS"}
    with pub_path.open("r", encoding="utf-8") as f:
        pubs = json.load(f)
    post_ids = [p.get("id") for p in pubs if isinstance(p, dict) and p.get("id")][:max_posts]
    if not post_ids:
        return {"error": "NO_POST_IDS"}

    collector = LensCollector(use_api=True)
    all_edges: list[dict] = []
    for pid in post_ids:
        try:
            rx = await collector._collect_reactions_for_post(pid, per_limit=per_limit)
        except Exception:
            rx = []
        try:
            cc = await collector._collect_collects_for_post(pid, per_limit=per_limit)
        except Exception:
            cc = []
        try:
            bm = await collector._collect_bookmarks_for_post(pid, per_limit=per_limit)
        except Exception:
            bm = []
        try:
            refs = await collector._collect_references_for_post(pid, per_type_limit=per_limit)
        except Exception:
            refs = []
        all_edges.extend(rx)
        all_edges.extend(cc)
        all_edges.extend(bm)
        all_edges.extend(refs)

    # Summary by type
    summary: dict[str, int] = {}
    for e in all_edges:
        t = e.get("engagement_type") or "UNKNOWN"
        summary[t] = summary.get(t, 0) + 1

    # Save to data/
    out_dir = project_root() / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"lens_engagements_probe_{ts}.json"
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(all_edges, f, ensure_ascii=False, indent=2)

    return {"count": len(all_edges), "by_type": summary, "out": str(out_file), "posts": len(post_ids)}


def main():
    try:
        result = asyncio.run(run_probe())
    except KeyboardInterrupt:
        print("INTERRUPTED")
        return
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()



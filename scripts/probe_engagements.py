#!/usr/bin/env python3
"""
Probe Lens engagements (reactions/bookmarks/tips/comments) for a few recent posts
and save results into data/lens_engagements_probe_*.json.

Now also produces per-post aggregated counts for 4 key metrics commonly shown in UI:
  - comments (COMMENT_ON)
  - reposts  (REPOST_OF)
  - likes    (LIKE)
  - tips     (TIP / stats.tips)

CLI options:
  --max-posts N     Number of posts to probe (default: 10)
  --per-limit N     Per-post limit for each engagement type (default: 50, 0 = unlimited)
  --counts-out PATH Optional explicit output path for counts JSON
"""
import asyncio
import json
from collections import deque
from pathlib import Path
from datetime import datetime
import argparse

import sys


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def find_latest_publications() -> Path | None:
    root = project_root()
    candidates = sorted((root / "data").glob("lens_publications_*.json"))
    if candidates:
        return candidates[-1]
    # Fallback to home-level data dir if present
    home_data = Path("/home/zhengqi/Social_network/data")
    candidates = sorted(home_data.glob("lens_publications_*.json"))
    return candidates[-1] if candidates else None


def _aggregate_counts_per_post(edges: list[dict], stats_override: dict[str, dict] | None = None) -> list[dict]:
    """Aggregate per-post counts for comments/reposts/likes/tips.
    口径：
      - references（COMMENT_ON/REPOST_OF/QUOTE_OF）归到父帖（优先 post_id，其次 ref_post_id）
      - reactions（LIKE）直接归 post_id
      - tips 默认使用 post.stats.tips，当有 TIP 边时增量累加
    返回按出现顺序稳定的列表。
    """
    counts: dict[str, dict] = {}
    ref_types = {"COMMENT_ON", "REPOST_OF", "QUOTE_OF", "MIRROR_OF"}
    for e in edges:
        if not isinstance(e, dict):
            continue
        et = (e.get("engagement_type") or "").upper()
        target = e.get("post_id") or (e.get("ref_post_id") if et in ref_types else None)
        if not target:
            continue
        row = counts.setdefault(target, {"post_id": target, "comments": 0, "reposts": 0, "likes": 0, "tips": 0})
        if et == "COMMENT_ON":
            row["comments"] += 1
        elif et == "REPOST_OF":
            row["reposts"] += 1
        elif et == "LIKE":
            row["likes"] += 1
        elif et == "TIP":
            row["tips"] += 1
    # stable order by first appearance of the target
    seen: set[str] = set()
    ordered: list[dict] = []
    for e in edges:
        et = (e.get("engagement_type") or "").upper()
        target = e.get("post_id") or (e.get("ref_post_id") if et in ref_types else None)
        if target and target in counts and target not in seen:
            ordered.append(counts[target])
            seen.add(target)
    if stats_override:
        for pid, stats in stats_override.items():
            row = counts.setdefault(pid, {"post_id": pid, "comments": 0, "reposts": 0, "likes": 0, "tips": 0})
            if isinstance(stats, dict):
                if isinstance(stats.get("comments"), int):
                    row["comments"] = stats["comments"]
                if isinstance(stats.get("reposts"), int):
                    row["reposts"] = stats["reposts"]
                likes_val = stats.get("reactions")
                if isinstance(likes_val, int):
                    row["likes"] = likes_val
                if isinstance(stats.get("tips"), int):
                    row["tips"] = stats["tips"]
    for pid, row in counts.items():
        if pid not in seen:
            ordered.append(row)
    return ordered


async def _fetch_stats_for_posts(collector, post_ids: list[str]) -> dict[str, dict]:
    stats: dict[str, dict] = {}
    for pid in post_ids:
        query = f"""
        query PostStats {{
          post(request: {{ post: "{pid}" }}) {{
            __typename
            ... on Post {{
              id
              stats {{
                comments
                reposts
                reactions
                tips
                collects
              }}
            }}
          }}
        }}
        """
        try:
            result = await collector._make_lens_api_request(query)
        except Exception:
            result = None
        if not isinstance(result, dict):
            continue
        data_node = result.get("data")
        post_node = (data_node or {}).get("post") if isinstance(data_node, dict) else None
        if not isinstance(post_node, dict):
            continue
        stats_node = post_node.get("stats")
        if not isinstance(stats_node, dict):
            continue
        stats[pid] = {
            "comments": stats_node.get("comments"),
            "reposts": stats_node.get("reposts"),
            "reactions": stats_node.get("reactions"),
            "tips": stats_node.get("tips") or stats_node.get("collects") or 0,
        }
    return stats


async def run_probe(max_posts: int = 10, per_limit: int = 50) -> dict:
    # Late import to avoid path issues
    sys.path.append(str(project_root()))
    from data_collection.blockchain.lens_collector import LensCollector

    pub_path = find_latest_publications()
    if not pub_path or not pub_path.exists():
        return {"error": "NO_PUBLICATIONS"}
    with pub_path.open("r", encoding="utf-8") as f:
        pubs = json.load(f)
    id_lookup: dict[str, dict] = {}
    seed_post_ids: list[str] = []
    seen_seed: set[str] = set()
    for entry in pubs:
        if not isinstance(entry, dict):
            continue
        pid = entry.get("id")
        if not pid:
            continue
        if pid not in id_lookup:
            id_lookup[pid] = entry
        if len(seed_post_ids) >= max_posts:
            continue
        if pid in seen_seed:
            continue
        seed_post_ids.append(pid)
        seen_seed.add(pid)

    if not seed_post_ids:
        return {"error": "NO_POST_IDS"}

    collect_post_ids: list[str] = []
    seen_collect: set[str] = set()

    def add_target(pid: str | None):
        if not pid or not isinstance(pid, str):
            return
        if pid in seen_collect:
            return
        seen_collect.add(pid)
        collect_post_ids.append(pid)

    for pid in seed_post_ids:
        add_target(pid)
        pub = id_lookup.get(pid) or {}
        extra_candidates: list[str | None] = []
        if isinstance(pub, dict):
            if pub.get("__typename") == "Repost":
                extra_candidates.append(((pub.get("repostOf") or {}) or {}).get("id"))
            for key in ("commentOn", "root", "quoteOf", "mirrorOf", "repostOf"):
                node = pub.get(key)
                if isinstance(node, dict):
                    extra_candidates.append(node.get("id"))
        for extra_id in extra_candidates:
            if extra_id and extra_id != pid:
                add_target(extra_id)

    collector = LensCollector(use_api=True)
    all_edges: list[dict] = []
    seen_edges: set[tuple[str, str | None, str | None, str]] = set()

    def append_unique(items: list[dict]):
        for e in items:
            if not isinstance(e, dict):
                continue
            post_id = e.get("post_id")
            engagement_type = (e.get("engagement_type") or "").upper()
            if not post_id or not engagement_type:
                continue
            user = (e.get("user_address") or "").lower()
            ref = e.get("ref_post_id")
            key = (user, post_id, ref, engagement_type)
            if key in seen_edges:
                continue
            seen_edges.add(key)
            all_edges.append(e)

    async def fetch_engagement_edges(pid: str, limit: int) -> list[dict]:
        edges_local: list[dict] = []
        try:
            rx = await collector._collect_reactions_for_post(pid, per_limit=limit)
        except Exception:
            rx = []
        try:
            cc = await collector._collect_collects_for_post(pid, per_limit=limit)
        except Exception:
            cc = []
        try:
            bm = await collector._collect_bookmarks_for_post(pid, per_limit=limit)
        except Exception:
            bm = []
        try:
            refs = await collector._collect_references_for_post(pid, per_type_limit=limit)
        except Exception:
            refs = []
        edges_local.extend(rx)
        edges_local.extend(cc)
        edges_local.extend(bm)
        edges_local.extend(refs)
        return edges_local

    processed_posts: set[str] = set()
    pending: deque[str] = deque()
    queued_posts: set[str] = set()

    def enqueue(pid: str | None) -> None:
        if not pid or not isinstance(pid, str):
            return
        if pid in processed_posts or pid in queued_posts:
            return
        queued_posts.add(pid)
        pending.append(pid)
    # Derive REPOST_OF edges directly from the publications list to ensure Repost counts are present
    try:
        for it in pubs:
            if not isinstance(it, dict):
                continue
            if it.get("__typename") != "Repost":
                continue
            author = it.get("author") or {}
            addr = author.get("address")
            repost_of = (it.get("repostOf") or {})
            root_post_id = repost_of.get("id")
            if not addr or not root_post_id:
                continue
            add_target(root_post_id)
            enqueue(root_post_id)
            append_unique([{
                "user_address": addr,
                "post_id": root_post_id,
                "ref_post_id": it.get("id"),
                "engagement_type": "REPOST_OF",
                "timestamp": it.get("timestamp"),
            }])
    except Exception:
        pass
    for pid in collect_post_ids:
        enqueue(pid)

    while pending:
        pid = pending.popleft()
        if pid in processed_posts:
            continue
        edges_local = await fetch_engagement_edges(pid, per_limit)
        append_unique(edges_local)
        processed_posts.add(pid)
        for edge in edges_local:
            if not isinstance(edge, dict):
                continue
            target = edge.get("post_id")
            if isinstance(target, str):
                enqueue(target)

    post_stats = await _fetch_stats_for_posts(collector, sorted(processed_posts))

    # Trim engagement edges per post to align with authoritative stats counts when available.
    desired_counts: dict[str, dict[str, int]] = {}
    for pid, stats in post_stats.items():
        if not isinstance(pid, str) or not isinstance(stats, dict):
            continue
        desired_counts[pid] = {
            "COMMENT_ON": int(stats.get("comments") or 0),
            "REPOST_OF": int(stats.get("reposts") or 0),
            "LIKE": int(stats.get("reactions") or 0),
            "TIP": int(stats.get("tips") or 0),
        }

    edges_by_key: dict[tuple[str, str], list[dict]] = {}
    for edge in all_edges:
        if not isinstance(edge, dict):
            continue
        pid = edge.get("post_id")
        et = (edge.get("engagement_type") or "").upper()
        if not pid or not et:
            continue
        edges_by_key.setdefault((pid, et), []).append(edge)

    filtered_edges: list[dict] = []
    for (pid, et), bucket in edges_by_key.items():
        bucket.sort(key=lambda e: (e.get("timestamp") or ""), reverse=True)
        limit = desired_counts.get(pid, {}).get(et)
        if limit is None or limit <= 0:
            filtered_edges.extend(bucket)
        else:
            filtered_edges.extend(bucket[:limit])

    all_edges = filtered_edges

    # Summary by type (overall)
    summary: dict[str, int] = {}
    for e in all_edges:
        t = (e.get("engagement_type") or "UNKNOWN").upper()
        summary[t] = summary.get(t, 0) + 1

    # Per-post 4-metric counts
    per_post_counts = _aggregate_counts_per_post(all_edges, stats_override=post_stats)

    # Save to data/
    out_dir = project_root() / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"lens_engagements_probe_{ts}.json"
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(all_edges, f, ensure_ascii=False, indent=2)

    counts_file = out_dir / f"lens_engagements_counts_probe_{ts}.json"
    with counts_file.open("w", encoding="utf-8") as f:
        json.dump(per_post_counts, f, ensure_ascii=False, indent=2)

    return {
        "count": len(all_edges),
        "by_type": summary,
        "out": str(out_file),
        "counts_out": str(counts_file),
        "posts": len(seed_post_ids),
        "probed_posts": len(collect_post_ids),
        "stats_fallback": post_stats,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-posts", type=int, default=10)
    parser.add_argument("--per-limit", type=int, default=50, help="Per-post fetch limit; set 0 for no limit")
    parser.add_argument("--counts-out", type=str, default="", help="Optional explicit output path for counts JSON")
    args = parser.parse_args()

    try:
        result = asyncio.run(run_probe(max_posts=args.max_posts, per_limit=args.per_limit))
    except KeyboardInterrupt:
        print("INTERRUPTED")
        return
    # If user specified explicit counts path, mirror/write there too
    if args.counts_out:
        try:
            src = Path(result.get("counts_out")) if isinstance(result.get("counts_out"), str) else None
            if src and src.exists():
                dst = Path(args.counts_out)
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
                result["counts_out"] = str(dst)
        except Exception:
            pass
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

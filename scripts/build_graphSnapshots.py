#!/usr/bin/env python3
"""
将最新的 Lens JSON 快照整合成图数据（nodes / edges）形式。

输出默认写到 data/graph/<timestamp>/ 目录，包含：
  - nodes_profiles.jsonl
  - nodes_posts.jsonl
  - edges_follows.jsonl
  - edges_engagements.jsonl
  - summary.json
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable


def find_all(pattern: str) -> list[Path]:
    return sorted(Path("data").glob(pattern))


def load_json_records(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            return []


def build_profile_nodes(profile_paths: Iterable[Path]) -> dict[str, dict]:
    nodes: dict[str, dict] = {}
    for path in profile_paths:
        for item in load_json_records(path):
            if not isinstance(item, dict):
                continue
            node_id = item.get("profile_id") or item.get("owned_by") or item.get("address")
            if not node_id:
                continue
            if node_id in nodes:
                continue
            nodes[node_id] = {
                "node_id": node_id,
                "node_type": "profile",
                "handle": item.get("handle") or ((item.get("username") or {}).get("localName")),
                "name": item.get("name") or (item.get("metadata") or {}).get("name"),
                "bio": item.get("bio") or (item.get("metadata") or {}).get("bio"),
                "collected_at": item.get("collected_at"),
                "created_at": item.get("created_at"),
                "platform": item.get("platform") or "lens_protocol",
            }
    return nodes


def build_post_nodes(publication_paths: Iterable[Path], engagement_paths: Iterable[Path]) -> dict[str, dict]:
    nodes: dict[str, dict] = {}
    for path in publication_paths:
        for item in load_json_records(path):
            if not isinstance(item, dict):
                continue
            node_id = item.get("id")
            if not node_id or node_id in nodes:
                continue
            nodes[node_id] = {
                "node_id": node_id,
                "node_type": "post",
                "post_kind": item.get("__typename"),
                "author_address": ((item.get("author") or {}).get("address")),
                "timestamp": item.get("timestamp"),
                "content_uri": item.get("contentUri"),
                "platform": "lens_protocol",
            }
    # Some评论 / 引用只出现在 engagement 中，补充最简节点
    for path in engagement_paths:
        for item in load_json_records(path):
            if not isinstance(item, dict):
                continue
            comment_post_id = item.get("ref_post_id")
            if comment_post_id and comment_post_id not in nodes:
                nodes[comment_post_id] = {
                    "node_id": comment_post_id,
                    "node_type": "post",
                    "post_kind": "unknown",
                    "author_address": None,
                    "timestamp": None,
                    "content_uri": None,
                    "platform": "lens_protocol",
                }
    return nodes


def build_follow_edges(follow_paths: Iterable[Path]) -> list[dict]:
    edges: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for path in follow_paths:
        for item in load_json_records(path):
            if not isinstance(item, dict):
                continue
            src = item.get("follower_address")
            dst = item.get("following_address")
            timestamp = item.get("followed_on")
            if not src or not dst:
                continue
            key = (src, dst, timestamp or "")
            if key in seen:
                continue
            seen.add(key)
            edges.append({
                "src": src,
                "dst": dst,
                "edge_type": "FOLLOW",
                "timestamp": timestamp,
                "src_type": "profile",
                "dst_type": "profile",
                "platform": "lens_protocol",
            })
    return edges


def build_engagement_edges(engagement_paths: Iterable[Path]) -> list[dict]:
    edges: list[dict] = []
    seen: set[tuple[str, str | None, str, str]] = set()
    for path in engagement_paths:
        for item in load_json_records(path):
            if not isinstance(item, dict):
                continue
            user = item.get("user_address")
            post_id = item.get("post_id")
            edge_type = (item.get("engagement_type") or "").upper()
            if not user or not post_id or not edge_type:
                continue
            ref = item.get("ref_post_id")
            timestamp = item.get("timestamp")
            key = (user, ref, post_id, edge_type)
            if key in seen:
                continue
            seen.add(key)
            edges.append({
                "src": user,
                "dst": post_id,
                "edge_type": edge_type,
                "timestamp": timestamp,
                "ref_post_id": ref,
                "src_type": "profile",
                "dst_type": "post",
                "platform": "lens_protocol",
            })
    return edges


def write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="将 Lens 快照转换为图数据")
    parser.add_argument("--out-dir", type=str, default="", help="输出目录（默认为 data/graph/<timestamp>）")
    parser.add_argument("--profiles", type=str, nargs="*", default=[], help="要合并的 profiles JSON 文件")
    parser.add_argument("--publications", type=str, nargs="*", default=[], help="要合并的 publications JSON 文件")
    parser.add_argument("--follows", type=str, nargs="*", default=[], help="要合并的 follows JSON 文件")
    parser.add_argument("--engagements", type=str, nargs="*", default=[], help="engagement JSON 文件（probe 输出）")
    args = parser.parse_args()

    profile_paths = [Path(p) for p in (args.profiles or [str(p) for p in find_all("lens_profiles_*.json")])]
    publication_paths = [Path(p) for p in (args.publications or [str(p) for p in find_all("lens_publications_*.json")])]
    follow_paths = [Path(p) for p in (args.follows or [str(p) for p in find_all("lens_follows_*.json")])]
    engagement_paths = [Path(p) for p in (args.engagements or [str(p) for p in find_all("lens_engagements_probe_*.json")])]

    if not profile_paths or not publication_paths or not follow_paths or not engagement_paths:
        raise SystemExit("缺少必要输入文件，请确认 data/ 目录下已有 profile/publication/follow/engagement 快照。")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir) if args.out_dir else Path("data") / "graph" / f"lens_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    profile_nodes = build_profile_nodes(profile_paths)
    post_nodes = build_post_nodes(publication_paths, engagement_paths)
    follow_edges = build_follow_edges(follow_paths)
    engagement_edges = build_engagement_edges(engagement_paths)

    write_jsonl(out_dir / "nodes_profiles.jsonl", profile_nodes.values())
    write_jsonl(out_dir / "nodes_posts.jsonl", post_nodes.values())
    write_jsonl(out_dir / "edges_follows.jsonl", follow_edges)
    write_jsonl(out_dir / "edges_engagements.jsonl", engagement_edges)

    summary = {
        "profiles": len(profile_nodes),
        "posts": len(post_nodes),
        "follow_edges": len(follow_edges),
        "engagement_edges": len(engagement_edges),
        "source": {
            "profiles": [str(p) for p in profile_paths],
            "publications": [str(p) for p in publication_paths],
            "follows": [str(p) for p in follow_paths],
            "engagements": [str(p) for p in engagement_paths],
        },
        "generated_at": timestamp,
        "output_dir": str(out_dir),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

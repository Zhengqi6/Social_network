#!/usr/bin/env python3
"""
快速可视化 Lens 图数据。

仅针对节点数量较少的子图进行绘制（默认随机采样）。
"""
import argparse
import json
import random
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import networkx as nx


def load_jsonl(path: Path, limit: int | None = None) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            rows.append(row)
            if limit is not None and len(rows) >= limit:
                break
    return rows


def sample_edges(edges: list[dict], keep_types: set[str], sample_size: int) -> list[dict]:
    filtered = [e for e in edges if (e.get("edge_type") or "").upper() in keep_types]
    if sample_size is None or sample_size <= 0 or len(filtered) <= sample_size:
        return filtered
    return random.sample(filtered, sample_size)


def build_graph(profiles_path: Path, posts_path: Path, follows_path: Path, engagements_path: Path,
                sample_size: int = 200, engagement_types: Iterable[str] | None = None, max_follow_edges: int = 100) -> nx.DiGraph:
    G = nx.DiGraph()

    profiles_map = {row.get("node_id"): row for row in load_jsonl(profiles_path, limit=None) if row.get("node_id")}
    posts_map = {row.get("node_id"): row for row in load_jsonl(posts_path, limit=None) if row.get("node_id")}
    follows = load_jsonl(follows_path, limit=None)
    engagements = load_jsonl(engagements_path, limit=None)

    def ensure_node(nid: str):
        if nid in G:
            return
        node_data = posts_map.get(nid)
        if node_data:
            label = node_data.get("post_kind") or "post"
            G.add_node(nid, node_type="post", label=label, is_post=True)
            return
        node_data = profiles_map.get(nid)
        if node_data:
            handle = node_data.get("handle")
            G.add_node(nid, node_type="profile", label=handle or nid[:8], handle=handle)
        else:
            G.add_node(nid, node_type="profile", label=nid[:8])

    random.shuffle(follows)
    keep_types = {t.upper() for t in (engagement_types or ["COMMENT_ON", "REPOST_OF", "LIKE"])}
    sampled_engagements = sample_edges(engagements, keep_types=keep_types, sample_size=sample_size)
    active_nodes: set[str] = set()
    for row in sampled_engagements:
        src = row.get("src")
        dst = row.get("dst")
        et = (row.get("edge_type") or "").upper()
        if not src or not dst:
            continue
        ensure_node(src)
        ensure_node(dst)
        ref = row.get("ref_post_id")
        if isinstance(ref, str):
            ensure_node(ref)
        G.add_edge(src, dst, edge_type=et)
        active_nodes.add(src)
        active_nodes.add(dst)
        if isinstance(ref, str):
            active_nodes.add(ref)

    if max_follow_edges and max_follow_edges > 0:
        follow_candidates = [
            row for row in follows
            if row.get("src") in active_nodes and row.get("dst") in active_nodes
        ]
        random.shuffle(follow_candidates)
        limit = max_follow_edges if max_follow_edges > 0 else len(follow_candidates)
        for row in follow_candidates[:limit]:
            src = row.get("src")
            dst = row.get("dst")
            if src and dst and src in G and dst in G:
                G.add_edge(src, dst, edge_type="FOLLOW")
    elif max_follow_edges == 0:
        pass
    else:
        for row in follows:
            src = row.get("src")
            dst = row.get("dst")
            if src and dst and src in G and dst in G:
                G.add_edge(src, dst, edge_type="FOLLOW")

    return G


def visualize(G: nx.DiGraph, out_path: Path, layout: str = "spring", seed: int = 42) -> None:
    if layout == "spring":
        pos = nx.spring_layout(G, seed=seed, k=0.6)
    elif layout == "kamada_kawai":
        pos = nx.kamada_kawai_layout(G)
    elif layout == "fruchterman":
        pos = nx.fruchterman_reingold_layout(G, seed=seed)
    elif layout == "random":
        pos = nx.random_layout(G, seed=seed)
    else:
        pos = nx.spring_layout(G, seed=seed)

    plt.figure(figsize=(12, 10))
    node_colors = []
    node_sizes = []
    labels = {}

    for node, data in G.nodes(data=True):
        if data.get("node_type") == "post":
            node_colors.append("#f39c12")
            node_sizes.append(200)
            labels[node] = data.get("label") or "post"
        else:
            node_colors.append("#3498db")
            node_sizes.append(300)
            labels[node] = data.get("label") or node[:6]

    follow_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get("edge_type") == "FOLLOW"]
    engagement_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get("edge_type") != "FOLLOW"]

    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=node_sizes, alpha=0.8)
    nx.draw_networkx_edges(G, pos, edgelist=follow_edges, width=1.0, alpha=0.4, edge_color="#95a5a6", arrows=False)
    nx.draw_networkx_edges(G, pos, edgelist=engagement_edges, width=1.5, alpha=0.7, edge_color="#e74c3c", arrows=False)
    nx.draw_networkx_labels(G, pos, labels, font_size=8)

    plt.axis("off")
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"图像已保存到: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="可视化 Lens 图数据（采样版）")
    parser.add_argument("--graph-dir", type=str, default="data/graph/lens_20251010_011456", help="图数据目录")
    parser.add_argument("--sample-size", type=int, default=200, help="抽样的互动边数量")
    parser.add_argument("--engagement-types", type=str, nargs="*", default=["COMMENT_ON", "REPOST_OF", "LIKE"],
                        help="保留的互动类型")
    parser.add_argument("--layout", type=str, default="spring", choices=["spring", "kamada_kawai", "fruchterman", "random"], help="布局算法")
    parser.add_argument("--output", type=str, default="graph_visualization.png", help="输出图片路径")
    parser.add_argument("--max-follow-edges", type=int, default=100, help="随机保留的关注边数量（0 表示不绘制关注边，负数表示全部）")
    args = parser.parse_args()

    graph_dir = Path(args.graph_dir)
    G = build_graph(
        profiles_path=graph_dir / "nodes_profiles.jsonl",
        posts_path=graph_dir / "nodes_posts.jsonl",
        follows_path=graph_dir / "edges_follows.jsonl",
        engagements_path=graph_dir / "edges_engagements.jsonl",
        sample_size=args.sample_size,
        engagement_types=args.engagement_types,
    )
    visualize(G, out_path=Path(args.output), layout=args.layout)


if __name__ == "__main__":
    main()

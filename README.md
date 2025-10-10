<div align="center">

# Lens Social Graph Pipeline (JSON ▸ Graph ▸ ML)

</div>

> **TL;DR**  
> - 一键抓取 Lens GraphQL 的用户 / 帖子 / 关注 / 互动数据  
> - 自动对齐互动计数（comments / likes / reposts / tips）  
> - 输出结构化图数据（nodes / edges）与 Parquet、支持后续图学习  
> - 内置快速可视化脚本，便于抽样检查 15k+ 互动边

---

## 🌐 项目概览

本仓库实现了一个 **Lens 协议专用** 的数据抓取与图构建流水线。所有数据通过 Lens GraphQL（`https://api.lens.xyz/graphql`）获取，输出为本地 JSON / Parquet 文件和图数据。项目默认离线运行，无需数据库，也无需回写链上。

- **目标任务**：粉丝推荐、内容推荐、互动预测、病毒式传播监测  
- **数据类型**：账号（profiles）、帖子（posts / reposts / comments）、关注（follows）、互动（likes / comments / reposts / quotes / tips）

---

## 📁 目录结构（关键文件）

```
Social_network/
├── config/
│   ├── requirements.txt            # 运行所需依赖
│   └── settings.py                 # Lens 端点、速率与重试策略
├── data/                           # 原始快照（JSON，gitignored）
│   └── graph/                      # build_graphSnapshots.py 输出的图数据
├── data_collection/
│   ├── blockchain/
│   │   ├── lens_collector.py       # 核心采集器（profiles/posts/follows/engagements）
│   │   └── ethereum_client.py      # 可选链上校验工具
│   └── main_collector.py           # 批量/持续采集调度器
├── scripts/
│   ├── build_graphSnapshots.py     # 合并快照 → 图数据（nodes/edges + summary）
│   ├── visualize_graph.py          # 互动图可视化（支持 15k+ 边）
│   ├── json_to_parquet.py          # JSON → Parquet 分区化
│   ├── build_link_dataset.py       # 构建链接预测数据集
│   ├── train_gnn_link.py           # 简易 GNN 训练示例
│   └── lens_auth.py                # 获取 Lens API Bearer token
├── logs/                           # 采集日志与报告（gitignored）
├── graph_visualization.png         # 抽样互动图示例
├── graph_engagements_all.png       # 15,315 条互动全量示例图
└── README.md
```

---

## ⚙️ 环境准备

```bash
python -m venv venv
source venv/bin/activate
pip install -r config/requirements.txt
# 若需可视化：
pip install networkx matplotlib scipy
```

### Lens API 授权（可选但推荐）

点赞 / Tips 等敏感接口需要 Bearer Token：

```bash
export PRIVATE_KEY=0x你的私钥
python scripts/lens_auth.py --write-env
# 生成后的 .env 内含 LENS_API_BEARER，重新 source 即可
```

---

## 🛰️ 数据采集

### 1. 批量抓取（Profiles / Posts / Follows / 基础互动）

```bash
venv/bin/python - <<'PY'
import asyncio
from data_collection.blockchain.lens_collector import LensCollector

async def run():
    collector = LensCollector(use_api=True)
    loops = 3          # 按需放大次数
    for i in range(loops):
        print(f"=== Batch {i+1}/{loops} ===")
        await collector.collect_all(
            profile_limit=1000,     # 每批抓取 1000 个账号
            pub_limit=2000,         # 每批抓取 2000 条帖子
            follow_per_profile=200, # 每个账号追踪 200 条关注
        )

asyncio.run(run())
PY
```

> 运行过程中会生成 `lens_profiles_*.json` / `lens_publications_*.json` / `lens_follows_*.json` 等快照，每个文件名包含时间戳，方便追加跑批。

### 2. 深度互动探针（对齐 Comments / Likes / Reposts / Tips）

采集完成后，对最新的帖子快照运行 **BFS 探针脚本**：

```bash
latest=$(ls data/lens_publications_*.json | sort | tail -n1)
echo "使用快照: $latest"
venv/bin/python scripts/probe_engagements.py \
  --max-posts 0 \    # 0 表示使用该快照内全部帖子
  --per-limit 0      # 0 表示抓全量互动直到 API 耗尽
```

脚本会：
1. 从目标帖子开始 BFS 拓展（评论 / 转发指向的上游帖子也会被纳入）  
2. 收集 Likes / Comments / Reposts / Quotes / Tips  
3. 根据 `post.stats` 做二次修剪，确保互动数与前端显示一致  
4. 输出 `lens_engagements_probe_*.json` 与 `lens_engagements_counts_probe_*.json`

### 3. 持续采集模式（可选）

```bash
venv/bin/python - <<'PY'
import asyncio
from data_collection.main_collector import MainDataCollector

async def run():
    collector = MainDataCollector()
    await collector.continuous_collection(
        interval_minutes=120,   # 每 2 小时运行一次
        max_profiles=500,
        max_posts_per_profile=25
    )

asyncio.run(run())
PY
```

---

## 🗂️ JSON → Parquet

```bash
venv/bin/python scripts/json_to_parquet.py
# 或仅针对指定文件
venv/bin/python scripts/json_to_parquet.py \
  --profiles data/lens_profiles_20251009_185100.json \
  --publications data/lens_publications_20251009_185100.json \
  --follows data/lens_follows_20251009_185100.json \
  --engagements data/lens_engagements_probe_20251010_001702.json
```

输出位于 `data/lens/<type>/dt=YYYYMMDD_HHMMSS/*.parquet`，方便后续用 Spark / pandas / Graph ML 读取。

---

## 🧱 图数据构建

最新脚本 `scripts/build_graphSnapshots.py` 会整合快照生成图：

```bash
venv/bin/python scripts/build_graphSnapshots.py
# 或仅合并指定批次
venv/bin/python scripts/build_graphSnapshots.py \
  --profiles data/lens_profiles_20251009_185100.json \
  --publications data/lens_publications_20251009_185100.json \
  --follows data/lens_follows_20251009_185100.json \
  --engagements data/lens_engagements_probe_20251010_001702.json
```

输出目录示例：`data/graph/lens_20251010_011456/`

- `nodes_profiles.jsonl`：3,000 个用户节点  
- `nodes_posts.jsonl`：14,774 个帖子节点（含评论补节点）  
- `edges_follows.jsonl`：198,851 条关注边  
- `edges_engagements.jsonl`：15,315 条互动边  
- `summary.json`：统计信息 & 来源文件记录

> JSONL 格式可直接导入 Neo4j、NetworkX、GraphBolt 等工具。

---

## 🔍 图可视化（含 15,315 条互动全量图）

- 抽样图（默认 40 条互动 + 相邻关注）：

  ```bash
  venv/bin/python scripts/visualize_graph.py \
    --graph-dir data/graph/lens_20251010_011456 \
    --sample-size 40 \
    --output graph_visualization.png
  ```

- 展示全部 15,315 条互动边（不含关注）：

  ```bash
  venv/bin/python scripts/visualize_graph.py \
    --graph-dir data/graph/lens_20251010_011456 \
    --sample-size -1 \
    --max-follow-edges 0 \
    --layout random \
    --output graph_engagements_all.png
  ```

输出示例已保存在仓库根目录 (`graph_visualization.png`, `graph_engagements_all.png`)。

---

## 🧬 下游示例：链接预测

1. 构建特征数据集：
   ```bash
   venv/bin/python scripts/build_link_dataset.py
   ```
   产生的训练/测试 CSV 位于 `data/miniset/link/`，包含常见结构特征（Jaccard、Adamic-Adar、Preferential Attachment 等）。

2. 训练 GNN：
   ```bash
   venv/bin/python scripts/train_gnn_link.py
   ```
   模型保存至 `models/link_gnn.pt`（gitignored）。

---

## 📒 运行建议

- **速率限制**：Lens GraphQL 默认 ~50 req/min，脚本包含延时策略，长时间抓取务必保留足够间隔或拆分批次。
- **断点续跑**：采集器会写入 `partial_*.json`，中途终止也不会丢失已整理的数据。
- **存储容量**：1 TB 服务器可轻松容纳千万级别边数据（多个批次合并）。
- **监控**：`logs/` 目录中保留每次批处理的统计信息（总量、错误数、耗时）。

---

## ✅ 快速复盘

```bash
source venv/bin/activate
# 1. 批次采集
python data_collection/main_collector.py --max-profiles 1000 --max-posts 2000
# 2. 深度互动探针
python scripts/probe_engagements.py --max-posts 0 --per-limit 0
# 3. 合并成图
python scripts/build_graphSnapshots.py
# 4. 随时抽样可视化
python scripts/visualize_graph.py --graph-dir data/graph/<latest> --sample-size 40
```

至此，你将拥有一个可直接用于图学习的 Lens 社交大图（节点/边/特征完整），并可根据需求持续扩展规模。欢迎在此基础上开发推荐系统、传播分析或其它 Web3 社交应用。*** End Patch

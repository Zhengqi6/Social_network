# Lens-only Decentralized Social Recommendation (JSON → Parquet)

This repository implements a Lens-only data pipeline for decentralized social recommendation. Data is collected from Lens GraphQL (`https://api.lens.xyz/graphql`, Chain ID 232) and optionally verified via Lens Chain RPC (`https://rpc.lens.xyz`). Outputs are local JSON snapshots and partitioned Parquet files. No databases are required.

## Goals

- **Account recommendation**: Follow/link prediction
- **Content recommendation**: Individual engagement prediction
- **Virality prediction**: Share/view prediction for posts/hashtags
- **(Optional) Sensor nodes**: Early detection of viral content

## Repository Layout

```
Social_Network/
├── config/
│   └── settings.py                 # Endpoints and rate limits
├── data/                           # Raw JSON snapshots (gitignored)
├── data_collection/
│   ├── blockchain/
│   │   ├── ethereum_client.py      # Web3 RPC helper (eth_getLogs)
│   │   └── lens_collector.py       # Lens GraphQL collector (profiles/posts/follows/engagements)
│   └── main_collector.py           # Orchestrates one-shot / continuous runs
├── scripts/
│   ├── json_to_parquet.py          # Convert JSON → Parquet with time partitions
│   ├── build_link_dataset.py       # Build link prediction dataset from Parquet
│   └── train_gnn_link.py           # Train a minimal GNN for link prediction
├── logs/                           # Run logs and collection reports (gitignored)
└── README.md
```

## Environment

Create and activate a virtual environment, then install dependencies.

```bash
python -m venv venv
source venv/bin/activate
pip install -r config/requirements.txt
```

Optional `.env` (values already have safe defaults):

```bash
# Lens
LENS_GRAPHQL_ENDPOINT=https://api.lens.xyz/graphql
LENS_CHAIN_RPC=https://rpc.lens.xyz
LENS_CHAIN_ID=232

# Logging
LOG_LEVEL=INFO
```

## Data Collection (Lens GraphQL)

One-shot collection:

```bash
python data_collection/main_collector.py --max-profiles 50 --max-posts 100
```

This calls `LensCollector.collect_all()` to fetch:
- `profiles` via `accounts(request:{ pageSize, cursor })`
- `publications` via `posts(request:{ pageSize, cursor })` with fragments on `Post` and `Repost`
- `follows` via `following(request:{ account, pageSize, orderBy, cursor })` for collected accounts
- `engagements` via `postReferences(request:{ referencedPost, referenceTypes, ... })` for a small set of posts

Snapshots are saved into `data/` with timestamped file names.

## File Naming and Directory Structure (VERY IMPORTANT)

Raw JSON snapshots (all saved under `data/`):

- `lens_profiles_YYYYMMDD_HHMMSS.json`
  - List of profile dicts. Minimal schema:
    - `profile_id` (string): account address
    - `handle` (string | null): `username.localName`
    - `name` (string | null): `metadata.name`
    - `bio` (string | null): `metadata.bio`
    - `owned_by` (string): address (same as `profile_id`)
    - `created_at` (string RFC3339): Lens account created time
    - `collected_at` (string RFC3339): local collection time
    - `platform` (string): `lens_protocol`

- `lens_publications_YYYYMMDD_HHMMSS.json`
  - List of publication dicts (as returned by GraphQL with minimal normalization). Key fields:
    - `id` (string): publication id
    - `timestamp` (string): on-protocol timestamp
    - `contentUri` (string | null): points to content (IPFS/HTTP)
    - `author.address` (string): author account address
    - `author.username.localName` (string | null): author handle
    - `__typename` (string): `Post` or `Repost`

- `lens_follows_YYYYMMDD_HHMMSS.json`
  - List of follow edges:
    - `follower_address` (string)
    - `following_address` (string)
    - `following_handle` (string | null)
    - `followed_on` (string)
    - `platform` (string): `lens_protocol`

- `lens_engagements_YYYYMMDD_HHMMSS.json` (optional, small sampled set per run)
  - List of engagement edges created from references:
    - `user_address` (string): who referenced
    - `post_id` (string): the referenced post id
    - `ref_post_id` (string): the referencing post id
    - `engagement_type` (string): one of `QUOTE_OF`, `COMMENT_ON`, `REPOST_OF`
    - `timestamp` (string)

Partitioned Parquet outputs (under `data/lens/`):

```
data/lens/
├── profiles/
│   └── dt=YYYYMMDD_HHMMSS/profiles.parquet
├── posts/
│   └── dt=YYYYMMDD_HHMMSS/publications.parquet
├── follows/
│   └── dt=YYYYMMDD_HHMMSS/follows.parquet
└── engagements/
    └── dt=YYYYMMDD_HHMMSS/engagements.parquet
```

Convert latest JSON snapshots to Parquet partitions:

```bash
python scripts/json_to_parquet.py
```

Or specify exact files:

```bash
python scripts/json_to_parquet.py \
  --profiles data/lens_profiles_20250831_213416.json \
  --publications data/lens_publications_20250831_213416.json \
  --follows data/lens_follows_20250831_213416.json \
  --engagements data/lens_engagements_20250831_213935.json
```

Parquet normalization details:
- `publications`: flattened columns `author_address`, `author_username` retained with `id`, `timestamp`, `contentUri`, `__typename`
- `profiles`: `profile_id`, `handle`, `name`, `bio`, `owned_by`, `created_at`
- `follows`: `follower_address`, `following_address`, `following_handle`, `followed_on`
- `engagements`: `user_address`, `post_id`, `ref_post_id`, `engagement_type`, `timestamp`

## Building a Minimal Link Prediction Dataset

From Parquet, build a small train/test dataset with structural features:

```bash
python scripts/build_link_dataset.py
```

Outputs under `data/miniset/link/`:
- `train.csv`, `test.csv` with positive/negative edges and features:
  - Common neighbors, Jaccard, Adamic–Adar, Resource Allocation, Preferential Attachment

## Training a Simple GNN for Link Prediction

Train a small GCN on the built dataset:

```bash
python scripts/train_gnn_link.py
```

Model is saved to `models/link_gnn.pt` (gitignored). The script prints AUC on the test set.

## Optional: On-chain Verification (Lens Chain RPC)

If needed for provenance checks, `data_collection/blockchain/ethereum_client.py` provides `get_logs()` to query Lens Chain events via `eth_getLogs`. You can map a publication/follow to its on-chain event window and verify timestamps. This is optional and not required for the JSON → Parquet → ML pipeline.

## Notes and Limits

- Rate limiting is enforced in `lens_collector.py` with simple pacing. If you encounter 429s, reduce `pageSize`, increase delays, or split runs.
- Engagements use references and are sampled to keep runs fast. Expand carefully with backoff.
- We intentionally do not write any data back to chain. All artifacts are local JSON/Parquet.

## Quick Start Recap

1) Install deps: `pip install -r config/requirements.txt`
2) Collect: `python data_collection/main_collector.py --max-profiles 50 --max-posts 100`
3) Convert: `python scripts/json_to_parquet.py`
4) Build dataset: `python scripts/build_link_dataset.py`
5) Train GNN: `python scripts/train_gnn_link.py`

Everything saved under `data/` and `models/` is gitignored.



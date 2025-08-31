#!/usr/bin/env python3
"""
Minimal GNN (GCN-style) for link prediction on Lens follows graph.

Inputs (produced by scripts/build_link_dataset.py):
  - data/miniset/link/train.csv
  - data/miniset/link/test.csv

Model: 2-layer GCN over train graph with trainable node embeddings.
Loss:  BCEWithLogits over given (u,v,y) pairs (pos+neg) from train.csv
Metric: ROC-AUC on test.csv
"""
from __future__ import annotations
import os
from pathlib import Path
import argparse
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn.metrics import roc_auc_score


ROOT = Path(__file__).resolve().parents[1]


def load_pairs(path: Path):
    df = pd.read_csv(path)
    # columns follower_address, following_address, y + optional features
    u = df["follower_address"].astype(str).to_numpy()
    v = df["following_address"].astype(str).to_numpy()
    y = df["y"].astype(int).to_numpy()
    return u, v, y


def build_id_map(*addr_arrays):
    s = set()
    for arr in addr_arrays:
        s.update(arr.tolist())
    idx = {a: i for i, a in enumerate(sorted(s))}
    return idx


def build_sparse_adj(num_nodes: int, edges_uv: np.ndarray, add_self_loops: bool = True):
    # undirected adjacency
    rows = np.concatenate([edges_uv[:, 0], edges_uv[:, 1]])
    cols = np.concatenate([edges_uv[:, 1], edges_uv[:, 0]])
    vals = np.ones(rows.shape[0], dtype=np.float32)
    if add_self_loops:
        diag = np.arange(num_nodes)
        rows = np.concatenate([rows, diag])
        cols = np.concatenate([cols, diag])
        vals = np.concatenate([vals, np.ones(num_nodes, dtype=np.float32)])
    indices = torch.tensor(np.vstack([rows, cols]), dtype=torch.long)
    values = torch.tensor(vals, dtype=torch.float32)
    adj = torch.sparse_coo_tensor(indices, values, (num_nodes, num_nodes))
    # normalize D^{-1/2} A D^{-1/2}
    deg = torch.sparse.sum(adj, dim=1).to_dense().clamp(min=1.0)
    d_inv_sqrt = deg.pow(-0.5)
    r, c = indices
    norm_vals = d_inv_sqrt[r] * values * d_inv_sqrt[c]
    norm_adj = torch.sparse_coo_tensor(indices, norm_vals, (num_nodes, num_nodes))
    norm_adj = norm_adj.coalesce()
    return norm_adj


class GCNLink(nn.Module):
    def __init__(self, num_nodes: int, emb_dim: int = 64, hid_dim: int = 64):
        super().__init__()
        self.emb = nn.Embedding(num_nodes, emb_dim)
        nn.init.xavier_uniform_(self.emb.weight)
        self.lin1 = nn.Linear(emb_dim, hid_dim)
        self.lin2 = nn.Linear(hid_dim, hid_dim)

    def gcn_layer(self, adj, x, lin):
        x = torch.sparse.mm(adj, x)
        x = lin(x)
        x = F.relu(x)
        return x

    def forward(self, adj):
        x = self.emb.weight
        h = self.gcn_layer(adj, x, self.lin1)
        z = self.gcn_layer(adj, h, self.lin2)
        return z  # (N, hid)

    def link_score(self, z, u_idx, v_idx):
        return (z[u_idx] * z[v_idx]).sum(dim=-1)


def train(args):
    train_path = ROOT / "data" / "miniset" / "link" / "train.csv"
    test_path = ROOT / "data" / "miniset" / "link" / "test.csv"
    assert train_path.exists() and test_path.exists(), "Run scripts/build_link_dataset.py first"

    u_tr, v_tr, y_tr = load_pairs(train_path)
    u_te, v_te, y_te = load_pairs(test_path)
    id_map = build_id_map(u_tr, v_tr, u_te, v_te)
    num_nodes = len(id_map)
    u_tr_i = np.vectorize(id_map.get)(u_tr)
    v_tr_i = np.vectorize(id_map.get)(v_tr)
    u_te_i = np.vectorize(id_map.get)(u_te)
    v_te_i = np.vectorize(id_map.get)(v_te)

    # Build train graph from positive edges only
    pos_mask = y_tr == 1
    edges = np.stack([u_tr_i[pos_mask], v_tr_i[pos_mask]], axis=1)
    adj = build_sparse_adj(num_nodes, edges)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    adj = adj.to(device)
    model = GCNLink(num_nodes=num_nodes, emb_dim=args.emb_dim, hid_dim=args.hid_dim).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=args.lr, weight_decay=1e-4)
    loss_fn = nn.BCEWithLogitsLoss()

    u_tr_t = torch.tensor(u_tr_i, dtype=torch.long, device=device)
    v_tr_t = torch.tensor(v_tr_i, dtype=torch.long, device=device)
    y_tr_t = torch.tensor(y_tr.astype(np.float32), dtype=torch.float32, device=device)

    u_te_t = torch.tensor(u_te_i, dtype=torch.long, device=device)
    v_te_t = torch.tensor(v_te_i, dtype=torch.long, device=device)
    y_te_t = torch.tensor(y_te.astype(np.float32), dtype=torch.float32, device=device)

    for epoch in range(1, args.epochs + 1):
        model.train()
        opt.zero_grad()
        z = model(adj)
        logits = model.link_score(z, u_tr_t, v_tr_t)
        loss = loss_fn(logits, y_tr_t)
        loss.backward()
        opt.step()

        if epoch % max(1, args.log_every) == 0 or epoch == args.epochs:
            model.eval()
            with torch.no_grad():
                z = model(adj)
                te_logits = model.link_score(z, u_te_t, v_te_t).detach().cpu().numpy()
                te_y = y_te_t.detach().cpu().numpy()
                auc = roc_auc_score(te_y, te_logits)
            print(f"epoch {epoch:03d} | loss {loss.item():.4f} | test AUC {auc:.4f}")

    # Save artifacts
    out_dir = ROOT / "models"
    out_dir.mkdir(exist_ok=True, parents=True)
    torch.save({
        "state_dict": model.state_dict(),
        "num_nodes": num_nodes,
        "id_map": id_map,
        "emb_dim": args.emb_dim,
        "hid_dim": args.hid_dim,
    }, out_dir / "link_gnn.pt")
    print(f"Saved model to {out_dir / 'link_gnn.pt'}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--emb-dim", type=int, default=64)
    p.add_argument("--hid-dim", type=int, default=64)
    p.add_argument("--lr", type=float, default=1e-3)
    p.add_argument("--epochs", type=int, default=20)
    p.add_argument("--log-every", type=int, default=2)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    train(args)



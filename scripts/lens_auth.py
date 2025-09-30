#!/usr/bin/env python3
"""
Obtain a Lens GraphQL access token (Bearer) via challenge → sign → authenticate.

Usage:
  # Export your EOA private key (use a burner; never commit it!)
  export PRIVATE_KEY=0x...

  # Run (will print tokens as JSON)
  python scripts/lens_auth.py

  # Optionally write LENS_API_BEARER to project .env
  python scripts/lens_auth.py --write-env
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
import requests
from eth_account import Account
from eth_account.messages import encode_defunct


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def gql_request(url: str, query: str, variables: dict | None = None, timeout: int = 20) -> dict:
    headers = {"Content-Type": "application/json"}
    # Do NOT attach Authorization here – this call is for auth bootstrap
    resp = requests.post(url, json={"query": query, "variables": variables or {}}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Invalid GraphQL response")
    if "errors" in data:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data.get("data") or {}


def get_challenge(url: str, address: str) -> str:
    # Try Lens v2 name first
    q1 = """
    query Challenge($address: EthereumAddress!) {
      challenge(request: { address: $address }) { text }
    }
    """
    try:
        data = gql_request(url, q1, {"address": address})
        challenge = ((data.get("challenge") or {}).get("text"))
        if challenge:
            return challenge
    except Exception:
        pass
    # Fallback to older naming
    q2 = """
    query GenerateChallenge($address: EthereumAddress!) {
      generateChallenge(request: { address: $address }) { text }
    }
    """
    data = gql_request(url, q2, {"address": address})
    challenge = ((data.get("generateChallenge") or {}).get("text"))
    if not challenge:
        raise RuntimeError("No challenge text returned by Lens API")
    return challenge


def authenticate(url: str, address: str, signature: str) -> dict:
    m = """
    mutation Authenticate($address: EthereumAddress!, $signature: Signature!) {
      authenticate(request: { address: $address, signature: $signature }) {
        accessToken
        refreshToken
      }
    }
    """
    data = gql_request(url, m, {"address": address, "signature": signature})
    auth = data.get("authenticate") or {}
    if not auth.get("accessToken"):
        raise RuntimeError("Lens authenticate did not return accessToken")
    return auth


def write_env_token(token: str, env_path: Path) -> None:
    # Append or replace LENS_API_BEARER in .env
    existing = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    out_lines = []
    replaced = False
    for line in existing:
        if line.startswith("LENS_API_BEARER="):
            out_lines.append(f"LENS_API_BEARER={token}")
            replaced = True
        else:
            out_lines.append(line)
    if not replaced:
        out_lines.append(f"LENS_API_BEARER={token}")
    env_path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default=os.getenv("LENS_GRAPHQL_ENDPOINT", "https://api.lens.xyz/graphql"))
    parser.add_argument("--write-env", action="store_true", help="Write LENS_API_BEARER to project .env")
    parser.add_argument("--env-path", default=str(project_root() / ".env"))
    args = parser.parse_args()

    priv = os.getenv("PRIVATE_KEY")
    if not priv:
        print("ERROR: PRIVATE_KEY env var not set", file=sys.stderr)
        sys.exit(2)
    acct = Account.from_key(priv)
    address = acct.address

    # 1) fetch challenge
    challenge = get_challenge(args.endpoint, address)

    # 2) sign challenge
    message = encode_defunct(text=challenge)
    signature = Account.sign_message(message, private_key=priv).signature.hex()

    # 3) authenticate → tokens
    tokens = authenticate(args.endpoint, address, signature)

    # optional write to .env
    if args.write_env:
        write_env_token(tokens["accessToken"], Path(args.env_path))

    print(json.dumps({
        "address": address,
        "accessToken": tokens.get("accessToken"),
        "refreshToken": tokens.get("refreshToken"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()



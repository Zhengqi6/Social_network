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
import urllib.parse


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def gql_request(url: str, query: str, variables: dict | None = None, timeout: int = 20) -> dict:
    headers = {"Content-Type": "application/json"}
    # Do NOT attach Authorization here – this call is for auth bootstrap
    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Invalid GraphQL response")
    if "errors" in data:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data.get("data") or {}


def get_challenge(url: str, address: str) -> dict:
    """Return a dict with possible keys: text, id.
    Tries multiple schema variants for Lens GraphQL.
    """
    variants = [
        # v2-like: challenge with address
        f"""
        query Challenge {{
          challenge(request: {{ address: \"{address}\" }}) {{ text id }}
        }}
        """,
        # v2-like: challenge with signedBy
        f"""
        query Challenge {{
          challenge(request: {{ signedBy: \"{address}\" }}) {{ text id }}
        }}
        """,
        # older naming: generateChallenge
        f"""
        query GenerateChallenge {{
          generateChallenge(request: {{ address: \"{address}\" }}) {{ text }}
        }}
        """,
    ]
    last_err: Exception | None = None
    for q in variants:
        try:
            data = gql_request(url, q)
            if "challenge" in data:
                node = data.get("challenge") or {}
                text = node.get("text")
                cid = node.get("id")
                if text or cid:
                    return {"text": text, "id": cid}
            if "generateChallenge" in data:
                node = data.get("generateChallenge") or {}
                text = node.get("text")
                if text:
                    return {"text": text}
        except Exception as e:
            last_err = e
            continue
    # REST fallback: /login/get-challenge
    try:
        base = url
        # strip trailing /graphql if present
        if base.endswith("/graphql"):
            base = base[:-8]
        # try api base if v2 host
        rest = urllib.parse.urljoin(base if base.endswith('/') else base+'/', 'login/get-challenge')
        r = requests.post(rest, json={"signedBy": address}, timeout=20)
        if r.status_code == 200:
            jd = r.json()
            # accept either {id,text} or {data:{challenge:{...}}}
            if isinstance(jd, dict):
                if jd.get("id") or jd.get("text"):
                    return {"id": jd.get("id"), "text": jd.get("text")}
                ch = ((jd.get("data") or {}).get("challenge") or {})
                if ch.get("id") or ch.get("text"):
                    return {"id": ch.get("id"), "text": ch.get("text")}
    except Exception as e:
        last_err = e
    raise RuntimeError(f"No challenge returned by Lens API (GraphQL+REST): {last_err}")


def authenticate(url: str, address: str, signature: str, challenge: dict | None = None) -> dict:
    """Try multiple auth variants: with challenge id or with address."""
    last_err: Exception | None = None
    # Prefer using challenge id if available (newer schemas)
    if challenge and challenge.get("id"):
        m1 = f"""
        mutation Authenticate {{
          authenticate(request: {{ id: \"{challenge['id']}\", signature: \"{signature}\" }}) {{
            accessToken
            refreshToken
          }}
        }}
        """
        try:
            data = gql_request(url, m1)
            auth = data.get("authenticate") or {}
            if auth.get("accessToken"):
                return auth
        except Exception as e:
            last_err = e
    # Fallback to address+signature (older schemas)
    m2 = f"""
    mutation Authenticate {{
      authenticate(request: {{ address: \"{address}\", signature: \"{signature}\" }}) {{
        accessToken
        refreshToken
      }}
    }}
    """
    try:
        data = gql_request(url, m2)
        auth = data.get("authenticate") or {}
        if not auth.get("accessToken"):
            raise RuntimeError("Lens authenticate did not return accessToken")
        return auth
    except Exception as e:
        last_err = e
        # REST fallback: /login/verify
        try:
            base = url
            if base.endswith("/graphql"):
                base = base[:-8]
            rest = urllib.parse.urljoin(base if base.endswith('/') else base+'/', 'login/verify')
            payload = {"id": (challenge or {}).get("id"), "signature": signature}
            # also send address for older backends
            payload["address"] = address
            r = requests.post(rest, json=payload, timeout=20)
            r.raise_for_status()
            jd = r.json()
            if isinstance(jd, dict):
                # accept flat or nested structure
                if jd.get("accessToken"):
                    return {"accessToken": jd.get("accessToken"), "refreshToken": jd.get("refreshToken")}
                auth = ((jd.get("data") or {}).get("authenticate") or {})
                if auth.get("accessToken"):
                    return auth
        except Exception as e2:
            last_err = e2
        raise RuntimeError(f"Authentication failed (GraphQL+REST): {last_err}")


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
    parser.add_argument("--endpoint", default=os.getenv("LENS_GRAPHQL_ENDPOINT", "https://api-v2.lens.dev"))
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
    ch = get_challenge(args.endpoint, address)

    # 2) sign challenge text
    message = encode_defunct(text=ch.get("text") or "")
    signature = Account.sign_message(message, private_key=priv).signature.hex()

    # 3) authenticate → tokens
    tokens = authenticate(args.endpoint, address, signature, challenge=ch)

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



#!/usr/bin/env python3
"""
Lens Protocol 最终可用的数据收集器
"""
import asyncio
import time
import json
import os
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
import aiohttp
import aiofiles
from loguru import logger
from web3 import Web3
from pathlib import Path
try:
    from web3.middleware import geth_poa_middleware
except ImportError:
    try:
        from web3.middleware import poa_middleware as geth_poa_middleware
    except ImportError:
        from web3.middleware import ExtraDataToPOAMiddleware as geth_poa_middleware
from config.settings import PLATFORM_APIS, COLLECTION_CONFIG


class LensCollector:
    """Lens Protocol 最终可用的数据收集器"""

    def __init__(self, rpc_url: str = "http://localhost:8545", use_api: bool = True):
        self.rpc_url = rpc_url
        self.use_api = use_api
        self.w3 = None

        if self.use_api:
            lens_cfg = PLATFORM_APIS.get("lens_chain", {})
            self.lens_api_url = lens_cfg.get("graphql_endpoint", "https://api.lens.xyz/graphql")
            self.api_rate_limit = int(lens_cfg.get("rate_limit", 50))
            self.last_api_request = 0
            # Optional: Bearer token for authenticated endpoints
            self.api_bearer = os.getenv("LENS_API_BEARER") or None
            # Retry/backoff config
            self.max_retries = int(COLLECTION_CONFIG.get("max_retries", 3))
            self.retry_delay = float(COLLECTION_CONFIG.get("retry_delay", 5))
            self.request_timeout = int(COLLECTION_CONFIG.get("timeout", 30))
        else:
            try:
                self.w3 = Web3(Web3.HTTPProvider(rpc_url))
                self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if not self.w3.is_connected():
                    logger.warning(f"⚠️ 无法连接到以太坊节点: {rpc_url}")
                    self.w3 = None
                else:
                    logger.info(f"✅ 连接到以太坊节点: {rpc_url}")
            except Exception as e:
                logger.warning(f"⚠️ 以太坊节点连接失败: {e}")
                self.w3 = None

        self.collected_profiles: Set[str] = set()
        self.collected_posts: Set[str] = set()
        self.collected_follows: Set[str] = set()

        self.stats = {
            "profiles_collected": 0,
            "posts_collected": 0,
            "follows_collected": 0,
            "api_requests": 0,
            "rpc_requests": 0,
            "errors": 0,
            "start_time": time.time(),
        }

        # Anchor IO paths to project root (two levels up from this file)
        try:
            self.base_dir = Path(__file__).resolve().parents[2]
        except Exception:
            self.base_dir = Path.cwd()
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        logger.info("🚀 Lens最终版数据收集器初始化完成")

        # Light concurrency guard for optional enrichment calls
        try:
            self._concurrency_limit = 8
            self._semaphore = asyncio.Semaphore(self._concurrency_limit)
        except Exception:
            self._semaphore = None

    async def _rate_limit_api(self):
        if not self.use_api:
            return
        elapsed = time.time() - self.last_api_request
        wait_time = (60 / self.api_rate_limit) - elapsed
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        self.last_api_request = time.time()

    async def _make_lens_api_request(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
        payload = {"query": query, "variables": variables or {}}
        attempt = 0
        last_error: Optional[Exception] = None
        while attempt <= getattr(self, "max_retries", 3):
            await self._rate_limit_api()
            try:
                async with aiohttp.ClientSession() as session:
                    headers = {"Content-Type": "application/json"}
                    # Attach Authorization header if a bearer token is configured
                    if self.use_api and self.api_bearer:
                        headers["Authorization"] = f"Bearer {self.api_bearer}"
                    async with session.post(
                        self.lens_api_url,
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=getattr(self, "request_timeout", 30)),
                    ) as response:
                        self.stats["api_requests"] += 1
                        if response.status == 200:
                            return await response.json()
                        last_error = RuntimeError(f"HTTP {response.status}")
                        logger.error(f"Lens API请求失败: {response.status}")
            except Exception as e:
                last_error = e
                logger.error(f"Lens API请求异常: {e}")
                self.stats["errors"] += 1
            attempt += 1
            if attempt <= getattr(self, "max_retries", 3):
                delay = getattr(self, "retry_delay", 5.0) * (2 ** (attempt - 1))
                await asyncio.sleep(delay)
        if last_error:
            logger.error(f"Lens API多次失败，放弃: {last_error}")
        return None

    async def _fetch_publication_by_id(self, post_id: str) -> Optional[Dict[str, Any]]:
        """Best-effort: fetch a single publication with richer fields (including metadata if available).
        Returns raw GraphQL node or None on failure.
        """
        # Lens v2 often exposes `publication(request: { for: ID })`
        # Keep the query tolerant to schema differences.
        query = f"""
        query PubById {{
          publication(request: {{ for: \"{post_id}\" }}) {{
            __typename
            ... on Post {{ id timestamp contentUri author {{ address username {{ localName }} }} metadata {{ content }} }}
            ... on Repost {{ id timestamp repostOf {{ ... on Post {{ id }} }} author {{ address username {{ localName }} }} }}
          }}
        }}
        """
        try:
            result = await self._make_lens_api_request(query)
            data_node = result.get("data") if isinstance(result, dict) else None
            pub = (data_node or {}).get("publication") if isinstance(data_node, dict) else None
            return pub if isinstance(pub, dict) else None
        except Exception as e:
            logger.debug(f"fetch publication by id failed: {e}")
            return None

    async def _resolve_content_from_content_uri(self, content_uri: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch content JSON using contentUri (supports ipfs:// and http(s)://).
        Returns parsed JSON dict if available, else None.
        """
        if not content_uri or not isinstance(content_uri, str):
            return None
        # Build URL for IPFS
        if content_uri.startswith("ipfs://"):
            cid = content_uri.replace("ipfs://", "").strip("/")
            url = f"https://ipfs.io/ipfs/{cid}"
        else:
            url = content_uri
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status != 200:
                        return None
                    ctype = resp.headers.get("Content-Type", "")
                    if "application/json" in ctype or url.endswith(".json"):
                        return await resp.json(content_type=None)
                    # Try to parse as JSON anyway
                    text = await resp.text()
                    try:
                        return json.loads(text)
                    except Exception:
                        # Non-JSON content; return as raw text
                        return {"raw": text}
        except Exception as e:
            logger.debug(f"resolve contentUri failed: {e}")
            return None

    async def enrich_publications_with_content(self, publications: List[Dict[str, Any]], max_items: int = 50) -> None:
        """Enrich given publications with `content` when discoverable from GraphQL metadata or contentUri.
        In-place mutation; best-effort and safe to fail silently per item.
        """
        if not publications:
            return
        tasks = []
        for i, pub in enumerate(publications):
            if i >= max_items:
                break
            if not isinstance(pub, dict):
                continue
            pub_id = pub.get("id")
            content_uri = pub.get("contentUri")
            # Skip if already has content (metadata) field
            if isinstance(pub.get("metadata"), dict) and pub["metadata"].get("content"):
                pub["content"] = pub["metadata"].get("content")
                continue
            async def _enrich_single(p: Dict[str, Any], pid: Optional[str], curi: Optional[str]):
                # Try publication() first for metadata.content
                enriched = None
                if pid:
                    enriched = await self._fetch_publication_by_id(pid)
                if isinstance(enriched, dict):
                    meta = enriched.get("metadata") or {}
                    if isinstance(meta, dict) and meta.get("content"):
                        p["content"] = meta.get("content")
                        return
                # Fallback to resolving contentUri
                meta2 = await self._resolve_content_from_content_uri(curi)
                if isinstance(meta2, dict):
                    # Heuristic common Lens metadata field names
                    content_text = meta2.get("content") or meta2.get("description") or meta2.get("name")
                    if content_text:
                        p["content"] = content_text
                    else:
                        # Store raw metadata for offline parsing
                        p["content_metadata"] = meta2
            if self._semaphore:
                async def _guarded(pub_ref=pub, pid=pub_id, curi=content_uri):
                    async with self._semaphore:
                        await _enrich_single(pub_ref, pid, curi)
                tasks.append(asyncio.create_task(_guarded()))
            else:
                tasks.append(asyncio.create_task(_enrich_single(pub, pub_id, content_uri)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def collect_profiles(self, limit: int = 100) -> List[Dict[str, Any]]:
        logger.info(f"🚀 通过API收集 {limit} 个用户资料")
        profiles = []
        # Lens v2: accounts(request: { pageSize: PageSize, cursor })
        cursor = None
        page_size_enum = "FIFTY" if limit > 10 else "TEN"
        exhausted = False
        reached_end = False
        partial_every = 500
        last_partial = 0
        while len(profiles) < limit:
            if cursor:
                query = f"""
                query GetAccounts {{
                  accounts(request: {{ pageSize: {page_size_enum}, cursor: \"{cursor}\" }}) {{
                    items {{ address createdAt username {{ localName }} metadata {{ bio name }} }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            else:
                query = f"""
                query GetAccounts {{
                  accounts(request: {{ pageSize: {page_size_enum} }}) {{
                    items {{ address createdAt username {{ localName }} metadata {{ bio name }} }}
                    pageInfo {{ next }}
                  }}
                }}
                """

            result = await self._make_lens_api_request(query, None)
            data_node = result.get("data") if isinstance(result, dict) else None
            if not result or data_node is None or not isinstance(data_node, dict) or not data_node.get("accounts"):
                logger.warning("API响应格式错误或无数据")
                if isinstance(result, dict) and "errors" in result:
                    logger.warning(f"API错误: {result['errors']}")
                break

            node = data_node["accounts"]
            items = node.get("items", [])
            for item in items:
                if len(profiles) >= limit:
                    break
                address = item.get("address")
                if not address or address in self.collected_profiles:
                    continue
                profiles.append({
                    "profile_id": address,
                    "handle": (item.get("username") or {}).get("localName"),
                    "name": (item.get("metadata") or {}).get("name"),
                    "bio": (item.get("metadata") or {}).get("bio"),
                    "owned_by": address,
                    "created_at": item.get("createdAt"),
                    "collected_at": datetime.utcnow().isoformat(),
                    "platform": "lens_protocol",
                })
                self.collected_profiles.add(address)

            page = node.get("pageInfo", {})
            cursor = page.get("next")
            if not cursor:
                reached_end = True
                break
            # partial save
            if len(profiles) - last_partial >= partial_every:
                await self._save_partial("profiles", profiles)
                last_partial = len(profiles)

        self.stats["profiles_collected"] += len(profiles)
        logger.info(f"✅ 成功处理 {len(profiles)} 个用户资料（含分页）")
        # attach exhaust flag for report
        self._profiles_exhausted = reached_end
        return profiles

    async def collect_publications(self, limit: int = 200) -> List[Dict[str, Any]]:
        logger.info(f"🚀 通过API收集 {limit} 个出版物")
        publications = []
        # Lens v2: posts(request: { pageSize, cursor })
        cursor = None
        page_size_enum = "FIFTY" if limit > 10 else "TEN"
        exhausted = False
        reached_end = False
        partial_every = 1000
        last_partial = 0
        while len(publications) < limit:
            if cursor:
                query = f"""
                query GetPosts {{
                  posts(request: {{ pageSize: {page_size_enum}, cursor: \"{cursor}\" }}) {{
                    items {{
                      __typename
                      ... on Post {{ id timestamp contentUri author {{ address username {{ localName }} }} }}
                      ... on Repost {{ id timestamp repostOf {{ ... on Post {{ id }} }} author {{ address username {{ localName }} }} }}
                    }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            else:
                query = f"""
                query GetPosts {{
                  posts(request: {{ pageSize: {page_size_enum} }}) {{
                    items {{
                      __typename
                      ... on Post {{ id timestamp contentUri author {{ address username {{ localName }} }} }}
                      ... on Repost {{ id timestamp repostOf {{ ... on Post {{ id }} }} author {{ address username {{ localName }} }} }}
                    }}
                    pageInfo {{ next }}
                  }}
                }}
                """

            result = await self._make_lens_api_request(query, None)
            data_node = result.get("data") if isinstance(result, dict) else None
            if not result or data_node is None or not isinstance(data_node, dict):
                logger.warning("API响应缺少data")
                if isinstance(result, dict) and "errors" in result:
                    logger.warning(f"API错误: {result['errors']}")
                break
            node = data_node.get("posts") if isinstance(data_node, dict) else None
            if not node:
                logger.warning("posts 节点为空，跳过本页")
                break
            items = node.get("items") or []
            for item in items:
                if len(publications) >= limit:
                    break
                if item is None:
                    continue
                pub_id = item.get("id") if isinstance(item, dict) else None
                if not pub_id or pub_id in self.collected_posts:
                    continue
                publications.append(item)
                self.collected_posts.add(pub_id)
            page = node.get("pageInfo") or {}
            cursor = page.get("next") if isinstance(page, dict) else None
            if not cursor:
                reached_end = True
                break
            if len(publications) - last_partial >= partial_every:
                await self._save_partial("publications", publications)
                last_partial = len(publications)

        self.stats["posts_collected"] += len(publications)
        logger.info(f"✅ 成功处理 {len(publications)} 个出版物（含分页）")
        self._publications_exhausted = reached_end
        return publications

    async def _collect_references_for_post(self, post_id: str, per_type_limit: int = 50) -> List[Dict[str, Any]]:
        """Collect referencing posts for a given post_id and convert to engagement edges.
        For each reference type (QUOTE_OF/COMMENT_ON/REPOST_OF) we query separately to label type.
        Returns list of { user_address, post_id, ref_post_id, engagement_type, timestamp }
        """
        engagements: List[Dict[str, Any]] = []
        ref_types = ["QUOTE_OF", "COMMENT_ON", "REPOST_OF"]
        for rtype in ref_types:
            cursor = None
            fetched = 0
            page_size_enum = "FIFTY" if per_type_limit > 10 else "TEN"
            while fetched < per_type_limit:
                if cursor:
                    query = f"""
                    query R {{
                      postReferences(request: {{ referencedPost: \"{post_id}\", referenceTypes: [{rtype}], visibilityFilter: PUBLIC, relevancyFilter: LATEST, pageSize: {page_size_enum}, cursor: \"{cursor}\" }}) {{
                        items {{
                          ... on Post {{ id timestamp author {{ address username {{ localName }} }} }}
                          ... on Repost {{ id timestamp author {{ address username {{ localName }} }} }}
                        }}
                        pageInfo {{ next }}
                      }}
                    }}
                    """
                else:
                    query = f"""
                    query R {{
                      postReferences(request: {{ referencedPost: \"{post_id}\", referenceTypes: [{rtype}], visibilityFilter: PUBLIC, relevancyFilter: LATEST, pageSize: {page_size_enum} }}) {{
                        items {{
                          ... on Post {{ id timestamp author {{ address username {{ localName }} }} }}
                          ... on Repost {{ id timestamp author {{ address username {{ localName }} }} }}
                        }}
                        pageInfo {{ next }}
                      }}
                    }}
                    """
                result = await self._make_lens_api_request(query)
                data_node = result.get("data") if isinstance(result, dict) else None
                node = (data_node or {}).get("postReferences") if isinstance(data_node, dict) else None
                if not node:
                    break
                items = node.get("items") or []
                for it in items:
                    if fetched >= per_type_limit:
                        break
                    if not isinstance(it, dict):
                        continue
                    user_addr = ((it.get("author") or {}).get("address"))
                    ts = it.get("timestamp")
                    ref_pid = it.get("id")
                    if not user_addr:
                        continue
                    engagements.append({
                        "user_address": user_addr,
                        "post_id": post_id,
                        "ref_post_id": ref_pid,
                        "engagement_type": rtype,
                        "timestamp": ts,
                    })
                    fetched += 1
                cursor = (node.get("pageInfo") or {}).get("next")
                if not cursor:
                    break
                # pace requests slightly to avoid stalls
                await asyncio.sleep(0.05)
        return engagements

    async def _collect_reactions_for_post(self, post_id: str, per_limit: int = 50) -> List[Dict[str, Any]]:
        """Collect reactions (e.g., likes/upvotes) for a post.
        Returns edges: { user_address, post_id, ref_post_id: None, engagement_type: LIKE|DISLIKE, timestamp }
        """
        edges: List[Dict[str, Any]] = []
        cursor = None
        fetched = 0
        page_size_enum = "FIFTY" if per_limit > 10 else "TEN"
        while fetched < per_limit:
            if cursor:
                query = f"""
                query W {{
                  whoReactedPublication(request: {{ on: \"{post_id}\", pageSize: {page_size_enum}, cursor: \"{cursor}\" }}) {{
                    items {{ reaction createdAt profile {{ address username {{ localName }} }} }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            else:
                query = f"""
                query W {{
                  whoReactedPublication(request: {{ on: \"{post_id}\", pageSize: {page_size_enum} }}) {{
                    items {{ reaction createdAt profile {{ address username {{ localName }} }} }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            try:
                result = await self._make_lens_api_request(query)
                data_node = result.get("data") if isinstance(result, dict) else None
                node = (data_node or {}).get("whoReactedPublication") if isinstance(data_node, dict) else None
                if not node:
                    break
                items = node.get("items") or []
                for it in items:
                    if fetched >= per_limit:
                        break
                    if not isinstance(it, dict):
                        continue
                    profile = it.get("profile") or {}
                    addr = profile.get("address")
                    if not addr:
                        continue
                    reaction = (it.get("reaction") or "").upper()
                    engagement_type = "LIKE" if reaction in ("UPVOTE", "LIKE", "UP") else ("DISLIKE" if reaction in ("DOWNVOTE", "DOWN", "DISLIKE") else "REACTION")
                    edges.append({
                        "user_address": addr,
                        "post_id": post_id,
                        "ref_post_id": None,
                        "engagement_type": engagement_type,
                        "timestamp": it.get("createdAt"),
                    })
                    fetched += 1
                cursor = (node.get("pageInfo") or {}).get("next")
                if not cursor:
                    break
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.debug(f"collect reactions failed for {post_id}: {e}")
                break
        return edges

    async def _collect_collects_for_post(self, post_id: str, per_limit: int = 50) -> List[Dict[str, Any]]:
        """Collect open action collects (donations/collects) for a post if exposed by GraphQL.
        Returns edges labeled as COLLECT.
        """
        edges: List[Dict[str, Any]] = []
        cursor = None
        fetched = 0
        page_size_enum = "FIFTY" if per_limit > 10 else "TEN"
        while fetched < per_limit:
            if cursor:
                query = f"""
                query C {{
                  whoCollectedPublication(request: {{ on: \"{post_id}\", pageSize: {page_size_enum}, cursor: \"{cursor}\" }}) {{
                    items {{ createdAt profile {{ address username {{ localName }} }} }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            else:
                query = f"""
                query C {{
                  whoCollectedPublication(request: {{ on: \"{post_id}\", pageSize: {page_size_enum} }}) {{
                    items {{ createdAt profile {{ address username {{ localName }} }} }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            try:
                result = await self._make_lens_api_request(query)
                data_node = result.get("data") if isinstance(result, dict) else None
                node = (data_node or {}).get("whoCollectedPublication") if isinstance(data_node, dict) else None
                if not node:
                    break
                items = node.get("items") or []
                for it in items:
                    if fetched >= per_limit:
                        break
                    prof = it.get("profile") or {}
                    addr = prof.get("address")
                    if not addr:
                        continue
                    edges.append({
                        "user_address": addr,
                        "post_id": post_id,
                        "ref_post_id": None,
                        "engagement_type": "COLLECT",
                        "timestamp": it.get("createdAt"),
                    })
                    fetched += 1
                cursor = (node.get("pageInfo") or {}).get("next")
                if not cursor:
                    break
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.debug(f"collect collects failed for {post_id}: {e}")
                break
        return edges

    async def _collect_bookmarks_for_post(self, post_id: str, per_limit: int = 50) -> List[Dict[str, Any]]:
        """Attempt to collect bookmarks for a post, if GraphQL exposes a per-publication bookmark list.
        Returns edges labeled as BOOKMARK. Best-effort and may return empty if unsupported.
        """
        edges: List[Dict[str, Any]] = []
        cursor = None
        fetched = 0
        page_size_enum = "FIFTY" if per_limit > 10 else "TEN"
        while fetched < per_limit:
            if cursor:
                query = f"""
                query B {{
                  whoBookmarkedPublication(request: {{ on: \"{post_id}\", pageSize: {page_size_enum}, cursor: \"{cursor}\" }}) {{
                    items {{ createdAt profile {{ address username {{ localName }} }} }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            else:
                query = f"""
                query B {{
                  whoBookmarkedPublication(request: {{ on: \"{post_id}\", pageSize: {page_size_enum} }}) {{
                    items {{ createdAt profile {{ address username {{ localName }} }} }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            try:
                result = await self._make_lens_api_request(query)
                data_node = result.get("data") if isinstance(result, dict) else None
                node = (data_node or {}).get("whoBookmarkedPublication") if isinstance(data_node, dict) else None
                if not node:
                    break
                items = node.get("items") or []
                for it in items:
                    if fetched >= per_limit:
                        break
                    prof = it.get("profile") or {}
                    addr = prof.get("address")
                    if not addr:
                        continue
                    edges.append({
                        "user_address": addr,
                        "post_id": post_id,
                        "ref_post_id": None,
                        "engagement_type": "BOOKMARK",
                        "timestamp": it.get("createdAt"),
                    })
                    fetched += 1
                cursor = (node.get("pageInfo") or {}).get("next")
                if not cursor:
                    break
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.debug(f"collect bookmarks failed for {post_id}: {e}")
                break
        return edges

    async def collect_engagements(self, base_posts: List[Dict[str, Any]], per_post_limit: int = 50) -> List[Dict[str, Any]]:
        """Collect engagements for a list of posts.
        Includes: references (QUOTE_OF/COMMENT_ON/REPOST_OF), reactions (LIKE), bookmarks, and collects when available.
        base_posts: list of publication dicts (must contain id)
        """
        post_ids = [p.get("id") for p in base_posts if isinstance(p, dict) and p.get("id")]
        # Limit scope to avoid long runtimes under strict rate limits
        post_ids = post_ids[:5]
        all_eng: List[Dict[str, Any]] = []
        # First, derive REPOST_OF edges directly from publications list (deterministic and free)
        try:
            derived = self._derive_repost_engagements(base_posts)
            if derived:
                all_eng.extend(derived)
        except Exception as e:
            logger.debug(f"derive repost engagements failed: {e}")
        for idx, pid in enumerate(post_ids, 1):
            logger.info(f"Collecting references for post {idx}/{len(post_ids)}: {pid}")
            try:
                edges_refs = await self._collect_references_for_post(pid, per_type_limit=min(per_post_limit, 10))
                all_eng.extend(edges_refs)
            except Exception as e:
                logger.warning(f"collect references failed for post {pid}: {e}")
            # Reactions (likes)
            try:
                edges_rx = await self._collect_reactions_for_post(pid, per_limit=min(per_post_limit, 20))
                all_eng.extend(edges_rx)
            except Exception as e:
                logger.debug(f"collect reactions failed for post {pid}: {e}")
            # Collects (donations/collect)
            try:
                edges_c = await self._collect_collects_for_post(pid, per_limit=min(per_post_limit, 10))
                all_eng.extend(edges_c)
            except Exception as e:
                logger.debug(f"collect collects failed for post {pid}: {e}")
            # Bookmarks
            try:
                edges_bm = await self._collect_bookmarks_for_post(pid, per_limit=min(per_post_limit, 10))
                all_eng.extend(edges_bm)
            except Exception as e:
                logger.debug(f"collect bookmarks failed for post {pid}: {e}")
        return all_eng

    def _derive_repost_engagements(self, publications: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create REPOST_OF engagements from the publications list itself.
        For any item with __typename == 'Repost', generate one edge with timestamp.
        """
        if not publications:
            return []
        edges: List[Dict[str, Any]] = []
        for it in publications:
            if not isinstance(it, dict):
                continue
            if it.get("__typename") != "Repost":
                continue
            author = it.get("author") or {}
            addr = author.get("address")
            repost_of = (it.get("repostOf") or {})
            post_id = repost_of.get("id")
            if not addr or not post_id:
                continue
            edges.append({
                "user_address": addr,
                "post_id": post_id,
                "ref_post_id": it.get("id"),
                "engagement_type": "REPOST_OF",
                "timestamp": it.get("timestamp"),
            })
        return edges

    async def _collect_following_for_account(self, address: str, per_limit: int = 200) -> List[Dict[str, Any]]:
        """Collect following edges for a given account address using GraphQL.
        Returns list of follow edges: { follower_address, following_address, followed_on }
        """
        edges: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        page_size_enum = "FIFTY" if per_limit > 10 else "TEN"
        fetched = 0
        exhausted = False
        reached_end = False
        partial_every = 2000
        last_partial = 0
        while fetched < per_limit:
            if cursor:
                query = f"""
                query F {{
                  following(request: {{ account: \"{address}\", pageSize: {page_size_enum}, orderBy: DESC, cursor: \"{cursor}\" }}) {{
                    items {{ following {{ address username {{ localName }} }} followedOn }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            else:
                query = f"""
                query F {{
                  following(request: {{ account: \"{address}\", pageSize: {page_size_enum}, orderBy: DESC }}) {{
                    items {{ following {{ address username {{ localName }} }} followedOn }}
                    pageInfo {{ next }}
                  }}
                }}
                """
            result = await self._make_lens_api_request(query)
            data_node = result.get("data") if isinstance(result, dict) else None
            if not data_node or not data_node.get("following"):
                break
            node = data_node["following"]
            items = node.get("items") or []
            for it in items:
                if fetched >= per_limit:
                    break
                following = (it or {}).get("following") or {}
                following_addr = following.get("address")
                if not following_addr:
                    continue
                edge_id = f"{address}->{following_addr}:{it.get('followedOn')}"
                if edge_id in self.collected_follows:
                    continue
                edges.append({
                    "follower_address": address,
                    "following_address": following_addr,
                    "following_handle": (following.get("username") or {}).get("localName"),
                    "followed_on": it.get("followedOn"),
                    "platform": "lens_protocol",
                })
                self.collected_follows.add(edge_id)
                fetched += 1
            cursor = (node.get("pageInfo") or {}).get("next")
            if not cursor:
                reached_end = True
                break
            if len(edges) - last_partial >= partial_every:
                await self._save_partial("follows", edges)
                last_partial = len(edges)
        # record per-account exhaust flag
        if not hasattr(self, "_follows_exhausted_map"):
            self._follows_exhausted_map = {}
        self._follows_exhausted_map[address] = reached_end
        return edges

    async def collect_follows(self, addresses: List[str], per_limit: int = 100) -> List[Dict[str, Any]]:
        """Collect follow edges for a list of addresses (following only)."""
        all_edges: List[Dict[str, Any]] = []
        self._follows_exhausted_map = {}
        for addr in addresses:
            try:
                edges = await self._collect_following_for_account(addr, per_limit=per_limit)
                all_edges.extend(edges)
            except Exception as e:
                logger.warning(f"collect following failed for {addr}: {e}")
                self._follows_exhausted_map[addr] = False
        return all_edges

    async def collect_all(self, profile_limit: int = 100, pub_limit: int = 200, follow_per_profile: int = 50):
        logger.info("🚀 开始全面数据收集 (Lens Corrected)")
        start_time = time.time()
        # run timestamp for partial files
        self._run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 先收集档案，再并行收集内容与关注边
        profiles = await self.collect_profiles(profile_limit)
        addr_list = [p.get("profile_id") for p in profiles if isinstance(p, dict) and p.get("profile_id")][:profile_limit]
        publications, follows = await asyncio.gather(
            self.collect_publications(pub_limit),
            self.collect_follows(addr_list, per_limit=follow_per_profile),
        )
        # Optional enrichment: fetch content for a subset of publications
        try:
            await self.enrich_publications_with_content(publications, max_items=50)
        except Exception as e:
            logger.debug(f"publication content enrichment failed: {e}")
        # 收集 engagement（引用/转发/评论）
        engagements = await self.collect_engagements(publications, per_post_limit=50)

        await self._save_data(profiles, publications, follows)
        # 额外保存 engagements
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if engagements:
            outp = self.data_dir / f"lens_engagements_{ts}.json"
            async with aiofiles.open(outp, "w") as f:
                await f.write(json.dumps(engagements, indent=2, ensure_ascii=False))
            logger.info(f"✅ 互动边已保存到 {outp}")

        await self._generate_report(profiles, publications, follows, start_time)

        return {"profiles": profiles, "publications": publications, "follows": follows, "engagements": engagements}

    async def _save_data(self, profiles: List, publications: List, follows: List):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if profiles:
            outp = self.data_dir / f"lens_profiles_{timestamp}.json"
            async with aiofiles.open(outp, "w") as f:
                await f.write(json.dumps(profiles, indent=2, ensure_ascii=False))
            logger.info(f"✅ 用户资料已保存到 {outp}")
        if publications:
            outp = self.data_dir / f"lens_publications_{timestamp}.json"
            async with aiofiles.open(outp, "w") as f:
                await f.write(json.dumps(publications, indent=2, ensure_ascii=False))
            logger.info(f"✅ 出版物已保存到 {outp}")
        if follows:
            outp = self.data_dir / f"lens_follows_{timestamp}.json"
            async with aiofiles.open(outp, "w") as f:
                await f.write(json.dumps(follows, indent=2, ensure_ascii=False))
            logger.info(f"✅ 关注关系已保存到 {outp}")
    
    async def _save_partial(self, kind: str, data: List[Dict[str, Any]]):
        """Write partial checkpoints to reduce data loss on long runs."""
        ts = getattr(self, "_run_ts", datetime.now().strftime("%Y%m%d_%H%M%S"))
        outp = self.data_dir / f"partial_{kind}_{ts}.json"
        try:
            async with aiofiles.open(outp, "w") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
            logger.debug(f"💾 Partial saved: {outp} ({len(data)})")
        except Exception as e:
            logger.debug(f"partial save failed for {kind}: {e}")
    
    async def _generate_report(self, profiles: List, publications: List, follows: List, start_time: float):
        duration = time.time() - start_time
        total_items = len(profiles) + len(publications) + len(follows)
        report = {
            "duration_seconds": duration,
            "profiles_collected": len(profiles),
            "publications_collected": len(publications),
            "follows_collected": len(follows),
            "exhaust": {
                "profiles": getattr(self, "_profiles_exhausted", False),
                "publications": getattr(self, "_publications_exhausted", False),
                "follows_per_account": getattr(self, "_follows_exhausted_map", {}),
            },
            "total_items": total_items,
            "api_requests": self.stats["api_requests"],
            "errors": self.stats["errors"],
            "items_per_second": total_items / duration if duration > 0 else 0,
        }
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        outp = self.logs_dir / f"lens_collection_report_{timestamp}.json"
        async with aiofiles.open(outp, "w") as f:
            await f.write(json.dumps(report, indent=2))
        logger.info(f"📊 数据收集报告已生成: {report}")

async def main():
    collector = LensCollector(use_api=True)
    await collector.collect_all(profile_limit=50, pub_limit=100)

if __name__ == "__main__":
    asyncio.run(main())

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
try:
    from web3.middleware import geth_poa_middleware
except ImportError:
    try:
        from web3.middleware import poa_middleware as geth_poa_middleware
    except ImportError:
        from web3.middleware import ExtraDataToPOAMiddleware as geth_poa_middleware
from config.settings import PLATFORM_APIS


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

        os.makedirs("data", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        logger.info("🚀 Lens最终版数据收集器初始化完成")

    async def _rate_limit_api(self):
        if not self.use_api:
            return
        elapsed = time.time() - self.last_api_request
        wait_time = (60 / self.api_rate_limit) - elapsed
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        self.last_api_request = time.time()

    async def _make_lens_api_request(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
        await self._rate_limit_api()
        payload = {"query": query, "variables": variables or {}}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.lens_api_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    self.stats["api_requests"] += 1
                    if response.status == 200:
                        return await response.json()
                    logger.error(f"Lens API请求失败: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Lens API请求异常: {e}")
            self.stats["errors"] += 1
            return None

    async def collect_profiles(self, limit: int = 100) -> List[Dict[str, Any]]:
        logger.info(f"🚀 通过API收集 {limit} 个用户资料")
        profiles = []
        # Lens v2: accounts(request: { pageSize: PageSize, cursor })
        cursor = None
        page_size_enum = "FIFTY" if limit > 10 else "TEN"
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
                break

        self.stats["profiles_collected"] += len(profiles)
        logger.info(f"✅ 成功处理 {len(profiles)} 个用户资料（含分页）")
        return profiles

    async def collect_publications(self, limit: int = 200) -> List[Dict[str, Any]]:
        logger.info(f"🚀 通过API收集 {limit} 个出版物")
        publications = []
        # Lens v2: posts(request: { pageSize, cursor })
        cursor = None
        page_size_enum = "FIFTY" if limit > 10 else "TEN"
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
                break

        self.stats["posts_collected"] += len(publications)
        logger.info(f"✅ 成功处理 {len(publications)} 个出版物（含分页）")
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

    async def collect_engagements(self, base_posts: List[Dict[str, Any]], per_post_limit: int = 50) -> List[Dict[str, Any]]:
        """Collect engagements (references) for a list of posts.
        base_posts: list of publication dicts (must contain id)
        """
        post_ids = [p.get("id") for p in base_posts if isinstance(p, dict) and p.get("id")]
        # Limit scope to avoid long runtimes under strict rate limits
        post_ids = post_ids[:5]
        all_eng: List[Dict[str, Any]] = []
        for idx, pid in enumerate(post_ids, 1):
            logger.info(f"Collecting references for post {idx}/{len(post_ids)}: {pid}")
            try:
                edges = await self._collect_references_for_post(pid, per_type_limit=min(per_post_limit, 10))
                all_eng.extend(edges)
            except Exception as e:
                logger.warning(f"collect references failed for post {pid}: {e}")
        return all_eng

    async def _collect_following_for_account(self, address: str, per_limit: int = 200) -> List[Dict[str, Any]]:
        """Collect following edges for a given account address using GraphQL.
        Returns list of follow edges: { follower_address, following_address, followed_on }
        """
        edges: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        page_size_enum = "FIFTY" if per_limit > 10 else "TEN"
        fetched = 0
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
                break
        return edges

    async def collect_follows(self, addresses: List[str], per_limit: int = 100) -> List[Dict[str, Any]]:
        """Collect follow edges for a list of addresses (following only)."""
        all_edges: List[Dict[str, Any]] = []
        for addr in addresses:
            try:
                edges = await self._collect_following_for_account(addr, per_limit=per_limit)
                all_edges.extend(edges)
            except Exception as e:
                logger.warning(f"collect following failed for {addr}: {e}")
        return all_edges

    async def collect_all(self, profile_limit: int = 100, pub_limit: int = 200, follow_per_profile: int = 50):
        logger.info("🚀 开始全面数据收集 (Lens Corrected)")
        start_time = time.time()

        # 先收集档案，再并行收集内容与关注边
        profiles = await self.collect_profiles(profile_limit)
        addr_list = [p.get("profile_id") for p in profiles if isinstance(p, dict) and p.get("profile_id")][:profile_limit]
        publications, follows = await asyncio.gather(
            self.collect_publications(pub_limit),
            self.collect_follows(addr_list, per_limit=follow_per_profile),
        )
        # 收集 engagement（引用/转发/评论）
        engagements = await self.collect_engagements(publications, per_post_limit=50)

        await self._save_data(profiles, publications, follows)
        # 额外保存 engagements
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if engagements:
            async with aiofiles.open(f"data/lens_engagements_{ts}.json", "w") as f:
                await f.write(json.dumps(engagements, indent=2, ensure_ascii=False))
            logger.info(f"✅ 互动边已保存到 data/lens_engagements_{ts}.json")

        await self._generate_report(profiles, publications, follows, start_time)

        return {"profiles": profiles, "publications": publications, "follows": follows, "engagements": engagements}

    async def _save_data(self, profiles: List, publications: List, follows: List):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if profiles:
            async with aiofiles.open(f"data/lens_profiles_{timestamp}.json", "w") as f:
                await f.write(json.dumps(profiles, indent=2, ensure_ascii=False))
            logger.info(f"✅ 用户资料已保存到 data/lens_profiles_{timestamp}.json")
        if publications:
            async with aiofiles.open(f"data/lens_publications_{timestamp}.json", "w") as f:
                await f.write(json.dumps(publications, indent=2, ensure_ascii=False))
            logger.info(f"✅ 出版物已保存到 data/lens_publications_{timestamp}.json")
        if follows:
            async with aiofiles.open(f"data/lens_follows_{timestamp}.json", "w") as f:
                await f.write(json.dumps(follows, indent=2, ensure_ascii=False))
            logger.info(f"✅ 关注关系已保存到 data/lens_follows_{timestamp}.json")
    
    async def _generate_report(self, profiles: List, publications: List, follows: List, start_time: float):
        duration = time.time() - start_time
        total_items = len(profiles) + len(publications) + len(follows)
        report = {
            "duration_seconds": duration,
            "profiles_collected": len(profiles),
            "publications_collected": len(publications),
            "follows_collected": len(follows),
            "total_items": total_items,
            "api_requests": self.stats["api_requests"],
            "errors": self.stats["errors"],
            "items_per_second": total_items / duration if duration > 0 else 0,
        }
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        async with aiofiles.open(f"logs/lens_collection_report_{timestamp}.json", "w") as f:
            await f.write(json.dumps(report, indent=2))
        logger.info(f"📊 数据收集报告已生成: {report}")

async def main():
    collector = LensCollector(use_api=True)
    await collector.collect_all(profile_limit=50, pub_limit=100)

if __name__ == "__main__":
    asyncio.run(main())

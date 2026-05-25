import asyncio
import aiohttp
import aiomysql
import time
import json
import random
from typing import Any, Dict, List, Optional

# =======================
# 数据库配置
# =======================
DB_CONFIG = {
    "host": "111.231.106.90",
    "port": 3306,
    "user": "yofun_renfantian",
    "password": "FXE4c9fCs68jn5N5",
    "db": "yofun_renfantian",
    "charset": "utf8mb4",
}

REQUEST_TIMEOUT = 15
RETRY_TIMES = 3

# =======================
# 速度控制配置（核心修改点）
# =======================
# 1. 严格限制全局最大并发请求数为 1 
GLOBAL_CONCURRENT_LIMIT = 1  
# 2. 页面抓取后的基础休眠时间（秒）
SLEEP_BASE = 1.5  
# 3. 页面抓取后的随机额外休眠时间最大值（秒），配合基础时间形成 1.5s ~ 3.0s 的随机间隔
SLEEP_RANDOM_MAX = 1.5  

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# =======================
# 平台配置
# =======================
PLATFORMS = {
    "switch": [
        {"url": "https://mpapi.yyouren.com/onsale", "params": {"limit": 10, "offset": 0, "sort": "popular", "discount_start": 0, "discount_end": 500}, "tag": "优惠促销"},
        {"url": "https://mpapi.yyouren.com/onsale", "params": {"limit": 10, "offset": 0, "sort": "popular", "discount_start": 0, "discount_end": 500, "exclusive": "true"}, "tag": "独占游戏"},
        {"url": "https://mpapi.yyouren.com/onsale", "params": {"limit": 10, "offset": 0, "sort": "popular", "discount": "false"}, "tag": "正在流行"},
        {"url": "https://mpapi.yyouren.com/onsale", "params": {"limit": 10, "offset": 0, "sort": "rating", "discount": "false"}, "tag": "高分神作"},
        {"url": "https://mpapi.yyouren.com/onsale", "params": {"limit": 10, "offset": 0, "sort": "release_date", "new_release": "true", "discount": "false"}, "tag": "最新上架"},
        {"url": "https://mpapi.yyouren.com/onsale", "params": {"limit": 10, "offset": 0, "sort": "release_date", "coming_soon": "true", "discount": "false"}, "tag": "即将推出"},
    ],
    "steam": [
        {"url": "https://mpapi.yyouren.com/steam/onsale", "params": {"limit": 10, "offset": 0, "tag": "优惠促销", "sort": "popular"}, "tag": "优惠促销"},
        {"url": "https://mpapi.yyouren.com/steam/onsale", "params": {"limit": 10, "offset": 0, "tag": "正在流行", "hide_free": "true"}, "tag": "正在流行"},
        {"url": "https://mpapi.yyouren.com/steam/onsale", "params": {"limit": 10, "offset": 0, "tag": "高分神作"}, "tag": "高分神作"},
        {"url": "https://mpapi.yyouren.com/steam/onsale", "params": {"limit": 10, "offset": 0, "tag": "最新上架"}, "tag": "最新上架"},
        {"url": "https://mpapi.yyouren.com/steam/onsale", "params": {"limit": 10, "offset": 0, "tag": "即将推出", "popular_wishlist": "true", "popular_coming_soon": "false"}, "tag": "即将推出"}
    ],
    "ps4": [
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS4", "tag": "优惠促销", "sort": "popular"}, "tag": "优惠促销"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS4", "tag": "优惠促销", "sort": "popular", "exclusive": "true"}, "tag": "独占游戏"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS4", "tag": "Plus会免", "screen": "extra"}, "tag": "Plus会免"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS4", "tag": "正在流行"}, "tag": "正在流行"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS4", "tag": "高分神作", "screen": "metacritic"}, "tag": "高分神作"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS4", "tag": "最新上架"}, "tag": "最新上架"},
    ],
    "ps5": [
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS5", "tag": "优惠促销", "sort": "popular"}, "tag": "优惠促销"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS5", "tag": "优惠促销", "sort": "popular", "exclusive": "true"}, "tag": "独占游戏"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS5", "tag": "正在流行"}, "tag": "正在流行"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS5", "tag": "高分神作", "screen": "metacritic"}, "tag": "高分神作"},
        {"url": "https://mpapi.yyouren.com/ps/onsale", "params": {"limit": 10, "offset": 0, "platform": "PS5", "tag": "最新上架"}, "tag": "最新上架"},
    ]
}

INSERT_LIST_SQL = """
INSERT INTO games_on_sale
(game_id, name, chinese_name, category, has_chinese, rating, discount_start_time, discount_end_time,
 original_price, discount_price, lowest_price, country, cover, popularity, platform,
 is_discount, is_popular, is_high_rating, is_free_plus, is_new_release, is_coming_soon, is_exclusive, plus_catalog)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    chinese_name=VALUES(chinese_name),
    category=VALUES(category),
    has_chinese=VALUES(has_chinese),
    rating=VALUES(rating),
    discount_start_time=VALUES(discount_start_time),
    discount_end_time=VALUES(discount_end_time),
    original_price=VALUES(original_price),
    discount_price=VALUES(discount_price),
    lowest_price=VALUES(lowest_price),
    country=VALUES(country),
    popularity=VALUES(popularity),
    is_discount=GREATEST(is_discount, VALUES(is_discount)),
    is_popular=GREATEST(is_popular, VALUES(is_popular)),
    is_high_rating=GREATEST(is_high_rating, VALUES(is_high_rating)),
    is_free_plus=GREATEST(is_free_plus, VALUES(is_free_plus)),
    is_new_release=GREATEST(is_new_release, VALUES(is_new_release)),
    is_coming_soon=GREATEST(is_coming_soon, VALUES(is_coming_soon)),
    is_exclusive=VALUES(is_exclusive),
    plus_catalog=GREATEST(plus_catalog, VALUES(plus_catalog))
"""

def parse_chinese_field(c) -> str:
    if isinstance(c, dict):
        return ",".join([k for k, v in c.items() if v])
    if isinstance(c, list):
        return ",".join([str(x) for x in c if x])
    if isinstance(c, str):
        return c.strip()
    return ""

async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict = None, retries: int = RETRY_TIMES) -> Optional[dict]:
    tries = 0
    while tries < retries:
        try:
            async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 429:  # 对方提示 Too Many Requests
                    print(f"⚠️ 触发频率限制(429)，将进行超长等待...")
                    await asyncio.sleep(10 + random.random() * 5)
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            tries += 1
            backoff = 2.0 * (2 ** (tries - 1)) + random.random() * 1.0  # 增加了重试的退避时间
            print(f"⚠️ HTTP 错误 try={tries}/{retries} url={url} err={e}")
            await asyncio.sleep(backoff)
    return None

class DBPool:
    def __init__(self):
        self.pool = None

    async def init_pool(self):
        self.pool = await aiomysql.create_pool(
            host=DB_CONFIG["host"], port=DB_CONFIG["port"],
            user=DB_CONFIG["user"], password=DB_CONFIG["password"],
            db=DB_CONFIG["db"], charset=DB_CONFIG["charset"],
            autocommit=True, maxsize=10
        )

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def save_list_rows(self, rows: List[Dict[str, Any]], platform: str, tag: str) -> int:
        if not rows: return 0
        saved = 0
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for item in rows:
                    name = str(item.get("name", "")).strip()
                    cover = str(item.get("cover", "")).strip()
                    if not name: continue 
                    
                    rating = item.get("rating")
                    if rating is None or rating == "":
                        rating = 0
                    
                    game_id = item.get("game_id")
                    chinese_name = parse_chinese_field(item.get("chinese"))
                    category = json.dumps(item.get("category", []), ensure_ascii=False)
                    has_chinese = json.dumps(item.get("chinese", {}), ensure_ascii=False)
                    
                    is_discount = 1 if tag == "优惠促销" else 0
                    is_popular = 1 if tag == "正在流行" else 0
                    is_high_rating = 1 if tag == "高分神作" else 0
                    is_free_plus = 1 if tag == "Plus会免" else 0
                    is_new_release = 1 if tag == "最新上架" else 0
                    is_coming_soon = 1 if tag == "即将推出" else 0

                    if platform == "steam":
                        is_exclusive = 0
                    elif tag == "独占游戏":
                        is_exclusive = 1
                    else:
                        raw_exclusive = item.get("exclusive")
                        is_exclusive = 1 if (raw_exclusive is True or str(raw_exclusive).lower() == "true") else 0

                    if platform in ("ps4", "ps5"):
                        raw_catalog = item.get("plus_catalog")
                        is_catalog = 1 if (raw_catalog is True or str(raw_catalog).lower() == "true") else 0
                    else:
                        is_catalog = 0

                    try:
                        await cur.execute(INSERT_LIST_SQL, (
                            game_id, name, chinese_name, category, has_chinese, rating,
                            item.get("discount_start_time"), item.get("discount_end_time"),
                            item.get("original_price"), item.get("discount_price"),
                            int(bool(item.get("lowest_price", False))),
                            item.get("country"), cover, item.get("popularity"), platform,
                            is_discount, is_popular, is_high_rating, is_free_plus, 
                            is_new_release, is_coming_soon, is_exclusive, is_catalog
                        ))
                        saved += 1
                    except Exception as e:
                        if "1213" in str(e): 
                            await asyncio.sleep(random.random())
                        else:
                            print(f"⚠️ 保存列表失败 {name} err={e}")
        return saved

async def process_platform(session, dbpool, platform, urls, sem):
    total_saved = 0

    async def fetch_task(u):
        nonlocal total_saved
        url = u["url"]
        base_params = u["params"].copy()
        limit = int(base_params.get("limit", 10))
        offset = int(base_params.get("offset", 0))
        tag = u.get("tag", "")

        while True:
            params = base_params.copy()
            params["offset"] = offset
            
            # 使用全局信号量，确保这一瞬间全程序只有指定数量的请求在发往服务器
            async with sem:
                resp = await fetch_json(session, url, params=params)
            
            if not resp: break

            data_rows = resp.get("data", {}).get("result", []) or []
            if not data_rows: break

            saved = await dbpool.save_list_rows(data_rows, platform, tag)
            total_saved += saved
            
            total = int(resp.get("data", {}).get("total", 0) or 0)
            offset += limit
            if offset >= total: break
            
            # 核心修改：增加随机休眠，使请求频率不可预测，降低被封几率
            sleep_time = SLEEP_BASE + random.random() * SLEEP_RANDOM_MAX
            await asyncio.sleep(sleep_time)

    # 每个平台内部的各类标签（如优惠、独占）也改为串行（一个接一个跑），进一步压低并发
    for u in urls:
        await fetch_task(u)
        
    return total_saved

async def main():
    dbpool = DBPool()
    await dbpool.init_pool()
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    # 创建全局统一的并发控制器
    global_sem = asyncio.Semaphore(GLOBAL_CONCURRENT_LIMIT)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        session._default_headers.update({"User-Agent": USER_AGENT})
        
        # 将原先的 asyncio.gather 全并发改为「串行遍历平台」
        # 这样能极大缓解同一时刻对对方服务器同一接口造成的压力
        results = []
        for plat, u in PLATFORMS.items():
            print(f"🚀 开始抓取平台: {plat}...")
            res = await process_platform(session, dbpool, plat, u, global_sem)
            results.append(res)
            # 平台与平台切换之间，也歇一口气
            await asyncio.sleep(3)

    await dbpool.close()
    print(f"\n✅ 全部抓取完毕，总计更新条数：{sum(results)}")

if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
# async_ps_details.py
# 异步抓取 PS (PS4/PS5) 游戏详情并存入 game_ps 表
# pip install aiohttp aiomysql

import asyncio
import aiohttp
import aiomysql
import json
import random
from typing import List, Dict, Any, Optional

# ========== 配置 ==========
DB_CONFIG = {
    "host": "111.231.106.90",      # 腾讯云公网 IP
    "port": 3306,                  # 数据库端口
    "user": "yofun_renfantian",    # 数据库用户名
    "password": "FXE4c9fCs68jn5N5", # 数据库密码
    "db": "yofun_renfantian",      # 数据库名
    "charset": "utf8mb4",          
}

API_TEMPLATE = "https://mpapi.yyouren.com/ps/detail?game_id={}"
REQUEST_TIMEOUT = 15
RETRY_TIMES = 3
CONCURRENT_REQUESTS = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
SLEEP_BETWEEN = 0.02

# ========== SQL ==========
SELECT_PS_GAMES = "SELECT id, game_id FROM games_on_sale WHERE platform IN ('ps4','ps5')"

INSERT_GAME_PS = """
INSERT INTO game_ps
(games_on_sale_id, name, chinese_name, genre, chinese, release_date, cover, media, color,
 rating_count, rating_5_percent, average_rating, platforms, publisher, description, language,
 origin_price, discount_price, origin_price_cn, discount_price_cn, discount_end_time, percent,
 is_free, is_lowest, plus_discount_price, plus_discount_price_cn, lowest_plus_price_percent,
 lowest_price_percent, demo, demo_text, metacritic, attr, intro)
VALUES (
 %s,%s,%s,%s,%s,%s,%s,%s,%s,
 %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
 %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
ON DUPLICATE KEY UPDATE
 name=VALUES(name),
 chinese_name=VALUES(chinese_name),
 genre=VALUES(genre),
 chinese=VALUES(chinese),
 release_date=VALUES(release_date),
 cover=VALUES(cover),
 media=VALUES(media),
 color=VALUES(color),
 rating_count=VALUES(rating_count),
 rating_5_percent=VALUES(rating_5_percent),
 average_rating=VALUES(average_rating),
 platforms=VALUES(platforms),
 publisher=VALUES(publisher),
 description=VALUES(description),
 language=VALUES(language),
 origin_price=VALUES(origin_price),
 discount_price=VALUES(discount_price),
 origin_price_cn=VALUES(origin_price_cn),
 discount_price_cn=VALUES(discount_price_cn),
 discount_end_time=VALUES(discount_end_time),
 percent=VALUES(percent),
 is_free=VALUES(is_free),
 is_lowest=VALUES(is_lowest),
 plus_discount_price=VALUES(plus_discount_price),
 plus_discount_price_cn=VALUES(plus_discount_price_cn),
 lowest_plus_price_percent=VALUES(lowest_plus_price_percent),
 lowest_price_percent=VALUES(lowest_price_percent),
 demo=VALUES(demo),
 demo_text=VALUES(demo_text),
 metacritic=VALUES(metacritic),
 attr=VALUES(attr),
 intro=VALUES(intro)
"""

# ========== 工具函数 ==========
def safe_json(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return json.dumps(str(v), ensure_ascii=False)

def to_int(v: Any, default: int = 0) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return default

def to_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default

# ========== HTTP ==========
async def fetch_json(session: aiohttp.ClientSession, url: str, retries: int = RETRY_TIMES) -> Dict[str, Any]:
    tries = 0
    while tries < retries:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            tries += 1
            backoff = 0.5 * (2 ** (tries - 1)) + random.random() * 0.5
            print(f"⚠ HTTP 请求失败 try={tries}/{retries} url={url} err={e} 等待 {backoff:.2f}s 后重试")
            await asyncio.sleep(backoff)
    return {}

# ========== 数据库辅助 ==========
class DB:
    def __init__(self):
        self.pool = None

    async def init(self):
        self.pool = await aiomysql.create_pool(**DB_CONFIG, autocommit=True, maxsize=10)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def fetch_ps_games(self) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(SELECT_PS_GAMES)
                rows = await cur.fetchall()
                return rows or []

    async def upsert_game_ps(self, games_on_sale_id: int, data: Dict[str, Any]):
        # JSON 字段
        genre = safe_json(data.get("genre"))
        media = safe_json(data.get("media"))
        platforms = safe_json(data.get("platforms"))
        language = safe_json(data.get("language"))
        attr = safe_json(data.get("attr"))

        params = (
            games_on_sale_id,
            data.get("name"),
            data.get("chinese_name"),
            genre,
            to_int(data.get("chinese")),
            to_int(data.get("release_date")),
            data.get("cover"),
            media,
            data.get("color"),
            to_int(data.get("rating_count")),
            data.get("rating_5_percent"),
            to_float(data.get("average_rating")),
            platforms,
            data.get("publisher"),
            data.get("description"),
            language,
            to_float(data.get("origin_price")),
            to_float(data.get("discount_price")),
            to_float(data.get("origin_price_cn")),
            to_float(data.get("discount_price_cn")),
            to_int(data.get("discount_end_time")),
            to_float(data.get("percent")),
            to_int(data.get("is_free")),
            to_int(data.get("is_lowest")),
            to_float(data.get("plus_discount_price")),
            to_float(data.get("plus_discount_price_cn")),
            to_float(data.get("lowest_plus_price_percent")),
            to_float(data.get("lowest_price_percent")),  # 新增字段映射
            to_int(data.get("demo")),
            data.get("demo_text"),
            to_int(data.get("metacritic")),
            attr,
            data.get("intro")
        )

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(INSERT_GAME_PS, params)
                    print(f"✅ 保存 game_ps 成功 id={games_on_sale_id} name={data.get('name')}")
                except Exception as e:
                    print(f"⚠ 保存 game_ps 失败 id={games_on_sale_id} err={e}")

# ========== 工作函数 ==========
async def process_one(session: aiohttp.ClientSession, db: DB, rec: Dict[str, Any], sem: asyncio.Semaphore):
    game_id = rec.get("game_id")
    games_on_sale_id = rec.get("id")
    if not game_id:
        return
    url = API_TEMPLATE.format(game_id)
    async with sem:
        data = await fetch_json(session, url)
        if not data or data.get("code") != 0 or "data" not in data:
            print(f"⚠ 无详情数据 game_id={game_id} 或返回异常")
            return
        payload = data["data"] or {}
        await db.upsert_game_ps(games_on_sale_id, payload)

# ========== 主函数 ==========
async def main():
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    db = DB()
    await db.init()

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        games = await db.fetch_ps_games()
        print(f"🟢 共找到 {len(games)} 个 PS4/PS5 游戏待抓取详情")

        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [process_one(session, db, g, sem) for g in games]
        await asyncio.gather(*tasks)

    await db.close()
    print("✅ 全部完成")

if __name__ == "__main__":
    asyncio.run(main())
#!/usr/bin/env python3
# fetch_game_steam_detail.py
# 异步抓取 Steam 游戏详情并存入数据库
# pip install aiohttp aiomysql

import asyncio
import aiohttp
import aiomysql
import json
import random
from typing import List, Dict, Any

# =======================
# 数据库配置
# =======================
DB_CONFIG = {
    "host": "111.231.106.90",      # 修改为你的腾讯云公网 IP
    "port": 3306,                  # 数据库端口，确保腾讯云安全组已放行 3306
    "user": "yofun_renfantian",    # 根据你 sync 脚本里的目标库用户名
    "password": "FXE4c9fCs68jn5N5", # 对应数据库密码
    "db": "yofun_renfantian",      # 对应数据库名
    "charset": "utf8mb4",          # 保持字符集不变
}

REQUEST_TIMEOUT = 15
RETRY_TIMES = 3
CONCURRENT_REQUESTS = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# =======================
# SQL 模板
# =======================
SELECT_STEAM_GAMES = "SELECT id, game_id FROM games_on_sale WHERE platform='steam'"

INSERT_GAME_STEAM = """
INSERT INTO game_steam
(games_on_sale_id, name, chinese_name, genre, chinese, review_level, review_percent,
 discount_percent, discount_end_time, is_free, release_date, image, color, deck_supported,
 cover, platforms, categories, developers, metacritic, publishers, achievements,
 about_the_game, short_description, controller_support, min_pc_requirements,
 min_mac_requirements, min_linux_requirements, recommended_pc_requirements,
 recommended_mac_requirements, recommended_linux_requirements, game_platform)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
name=VALUES(name), chinese_name=VALUES(chinese_name), genre=VALUES(genre), chinese=VALUES(chinese),
review_level=VALUES(review_level), review_percent=VALUES(review_percent), discount_percent=VALUES(discount_percent),
discount_end_time=VALUES(discount_end_time), is_free=VALUES(is_free), release_date=VALUES(release_date),
image=VALUES(image), color=VALUES(color), deck_supported=VALUES(deck_supported), cover=VALUES(cover),
platforms=VALUES(platforms), categories=VALUES(categories), developers=VALUES(developers),
metacritic=VALUES(metacritic), publishers=VALUES(publishers), achievements=VALUES(achievements),
about_the_game=VALUES(about_the_game), short_description=VALUES(short_description),
controller_support=VALUES(controller_support), min_pc_requirements=VALUES(min_pc_requirements),
min_mac_requirements=VALUES(min_mac_requirements), min_linux_requirements=VALUES(min_linux_requirements),
recommended_pc_requirements=VALUES(recommended_pc_requirements),
recommended_mac_requirements=VALUES(recommended_mac_requirements),
recommended_linux_requirements=VALUES(recommended_linux_requirements), game_platform=VALUES(game_platform)
"""

INSERT_GAME_STEAM_PRICE = """
INSERT INTO game_steam_price
(games_on_sale_id, country, original_price, discount_price, original_price_cn, discount_price_cn, end_time,
 is_lowest_price, lowest_percent, currency, percent)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
original_price=VALUES(original_price), discount_price=VALUES(discount_price),
original_price_cn=VALUES(original_price_cn), discount_price_cn=VALUES(discount_price_cn),
end_time=VALUES(end_time), is_lowest_price=VALUES(is_lowest_price),
lowest_percent=VALUES(lowest_percent), currency=VALUES(currency), percent=VALUES(percent)
"""

API_URL = "https://mpapi.yyouren.com/steam/detail?game_id={}"

# =======================
# HTTP 请求
# =======================
async def fetch_json(session: aiohttp.ClientSession, url: str, retries: int = RETRY_TIMES) -> dict:
    tries = 0
    while tries < retries:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            tries += 1
            backoff = 0.5 * (2 ** (tries - 1)) + random.random() * 0.5
            print(f"⚠️ HTTP 错误 try={tries}/{retries} url={url} err={e} backoff={backoff:.2f}s")
            await asyncio.sleep(backoff)
    return {}

# =======================
# 数据库操作
# =======================
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

    async def fetch_steam_games(self) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(SELECT_STEAM_GAMES)
                return await cur.fetchall()

    async def save_game_steam(self, games_on_sale_id: int, data: dict):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                # JSON 字段处理
                genre = json.dumps(data.get("genre") or [], ensure_ascii=False)
                image = json.dumps(data.get("image") or [], ensure_ascii=False)
                deck_supported = json.dumps(data.get("deck_supported") or {}, ensure_ascii=False)
                platforms = json.dumps(data.get("platforms") or {}, ensure_ascii=False)
                categories = json.dumps(data.get("categories") or [], ensure_ascii=False)
                developers = json.dumps(data.get("developers") or [], ensure_ascii=False)
                publishers = json.dumps(data.get("publishers") or [], ensure_ascii=False)
                achievements = json.dumps(data.get("achievements") or [], ensure_ascii=False)
                release_date = data.get("release_date")
                if isinstance(release_date, int):
                    release_date_val = release_date
                else:
                    release_date_val = 0  # 或 None
                await cur.execute(INSERT_GAME_STEAM, (
                    games_on_sale_id,
                    data.get("name"),
                    data.get("chinese_name"),
                    genre,
                    int(data.get("chinese") or 0),
                    data.get("review_level"),
                    data.get("review_percent"),
                    data.get("discount_percent"),
                    data.get("discount_end_time"),
                    int(data.get("is_free") or 0),
                    release_date_val,
                    image,
                    data.get("color"),
                    deck_supported,
                    data.get("cover"),
                    platforms,
                    categories,
                    developers,
                    data.get("metacritic"),
                    publishers,
                    achievements,
                    data.get("about_the_game"),
                    data.get("short_description"),
                    data.get("controller_support"),
                    data.get("min_pc_requirements"),
                    data.get("min_mac_requirements"),
                    data.get("min_linux_requirements"),
                    data.get("recommended_pc_requirements"),
                    data.get("recommended_mac_requirements"),
                    data.get("recommended_linux_requirements"),
                    data.get("game_platform")
                ))

                # 保存价格
                for price in data.get("game_price", []):
                    await cur.execute(INSERT_GAME_STEAM_PRICE, (
                        games_on_sale_id,
                        price.get("country"),
                        price.get("original_price"),
                        price.get("discount_price"),
                        price.get("original_price_cn"),
                        price.get("discount_price_cn"),
                        price.get("end_time"),
                        int(price.get("is_lowest_price") or 0),
                        price.get("lowest_percent"),
                        price.get("currency"),
                        price.get("percent")
                    ))

# =======================
# 异步抓取任务
# =======================
async def process_game(session: aiohttp.ClientSession, dbpool: DBPool, game: dict):
    game_id = game["game_id"]
    url = API_URL.format(game_id)
    data = await fetch_json(session, url)
    if data and data.get("code") == 0 and data.get("data"):
        await dbpool.save_game_steam(game["id"], data["data"])
        print(f"✅ 保存 Steam 游戏详情: {data['data'].get('name')}")

async def main():
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    dbpool = DBPool()
    await dbpool.init_pool()

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        games = await dbpool.fetch_steam_games()
        print(f"🟢 共找到 {len(games)} 个 Steam 游戏待抓取详情")

        tasks = []
        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        async def sem_task(game):
            async with sem:
                await process_game(session, dbpool, game)

        for g in games:
            tasks.append(sem_task(g))
        await asyncio.gather(*tasks)

    await dbpool.close()
    print("✅ 抓取完成")

if __name__ == "__main__":
    asyncio.run(main())

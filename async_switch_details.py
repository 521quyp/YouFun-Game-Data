#!/usr/bin/env python3
# fetch_game_switch_detail.py
# 异步抓取 Switch 游戏详情并存入数据库
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
SELECT_SWITCH_GAMES = "SELECT id, game_id FROM games_on_sale WHERE platform='switch'"
INSERT_GAME_SWITCH = """
INSERT INTO game_switch
(games_on_sale_id, name, chinese_name, category, chinese, rating, description, physical, key_card, players, demo, excerpt,
 size, ns2_size, release_date, publisher, developer, online, local, images, videos, cover, history_lowest_price, color, attribute, ns2, upgrade)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
name=VALUES(name), chinese_name=VALUES(chinese_name), category=VALUES(category), chinese=VALUES(chinese),
rating=VALUES(rating), description=VALUES(description), physical=VALUES(physical), key_card=VALUES(key_card),
players=VALUES(players), demo=VALUES(demo), excerpt=VALUES(excerpt), size=VALUES(size), ns2_size=VALUES(ns2_size),
release_date=VALUES(release_date), publisher=VALUES(publisher), developer=VALUES(developer), online=VALUES(online),
local=VALUES(local), images=VALUES(images), videos=VALUES(videos), cover=VALUES(cover), history_lowest_price=VALUES(history_lowest_price),
color=VALUES(color), attribute=VALUES(attribute), ns2=VALUES(ns2), upgrade=VALUES(upgrade)
"""

INSERT_GAME_SWITCH_PRICE = """
INSERT INTO game_switch_price
(games_on_sale_id, country, original_price, discount_price, original_price_cn, discount_price_cn, start_time, end_time,
 chinese, lowest_price, lowest_price_cn, is_lowest_price, is_lowest_price_v1, currency, gold, percent_)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
original_price=VALUES(original_price), discount_price=VALUES(discount_price),
original_price_cn=VALUES(original_price_cn), discount_price_cn=VALUES(discount_price_cn),
start_time=VALUES(start_time), end_time=VALUES(end_time), chinese=VALUES(chinese),
lowest_price=VALUES(lowest_price), lowest_price_cn=VALUES(lowest_price_cn),
is_lowest_price=VALUES(is_lowest_price), is_lowest_price_v1=VALUES(is_lowest_price_v1),
currency=VALUES(currency), gold=VALUES(gold), percent_=VALUES(percent_)
"""

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

    async def fetch_switch_games(self) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(SELECT_SWITCH_GAMES)
                return await cur.fetchall()

    async def save_game_switch(self, games_on_sale_id: int, data: dict):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                # 处理 JSON 字段
                category = json.dumps(data.get("category") or [], ensure_ascii=False)
                chinese = json.dumps(data.get("chinese") or {}, ensure_ascii=False)
                release_date = json.dumps(data.get("release_date") or {}, ensure_ascii=False)
                images = json.dumps(data.get("images") or [], ensure_ascii=False)
                videos = json.dumps(data.get("videos") or [], ensure_ascii=False)
                attribute = json.dumps(data.get("attribute") or {}, ensure_ascii=False)
                
                await cur.execute(INSERT_GAME_SWITCH, (
                    games_on_sale_id,
                    data.get("name"),
                    data.get("chinese_name"),
                    category,
                    chinese,
                    data.get("rating"),
                    data.get("description"),
                    int(data.get("physical") or 0),
                    data.get("key_card"),
                    data.get("players"),
                    int(data.get("demo") or 0),
                    data.get("excerpt"),
                    data.get("size"),
                    data.get("ns2_size"),
                    release_date,
                    data.get("publisher"),
                    data.get("developer"),
                    data.get("online"),
                    data.get("local"),
                    images,
                    videos,
                    data.get("cover"),
                    data.get("history_lowest_price"),
                    data.get("color"),
                    attribute,
                    int(data.get("ns2") or 0),
                    data.get("upgrade")
                ))

                # 保存价格
                for price in data.get("game_price", []):
                    await cur.execute(INSERT_GAME_SWITCH_PRICE, (
                        games_on_sale_id,
                        price.get("country"),
                        price.get("original_price"),
                        price.get("discount_price"),
                        price.get("original_price_cn"),
                        price.get("discount_price_cn"),
                        price.get("start_time"),
                        price.get("end_time"),
                        int(price.get("chinese") or 0),
                        price.get("lowest_price"),
                        price.get("lowest_price_cn"),
                        int(price.get("is_lowest_price") or 0),
                        int(price.get("is_lowest_price_v1") or 0),
                        price.get("currency"),
                        price.get("gold"),
                        price.get("percent_")
                    ))

# =======================
# 异步抓取任务
# =======================
async def process_game(session: aiohttp.ClientSession, dbpool: DBPool, game: dict):
    game_id = game["game_id"]
    url = f"https://mpapi.yyouren.com/detail?id={game_id}"
    data = await fetch_json(session, url)
    payload = data.get("data")
    
    # 【新增判断】如果 API 返回的数据里没有名字，直接跳过，不存数据库
    if not payload or not payload.get("name"):
        print(f"⚠️ 跳过 game_id={game['id']}: 名字为空，防止数据库报错")
        return
    if data and data.get("code") == 0 and data.get("data"):
        await dbpool.save_game_switch(game["id"], data["data"])
        print(f"✅ 保存游戏详情: {data['data'].get('name')}")

async def main():
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    dbpool = DBPool()
    await dbpool.init_pool()

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        games = await dbpool.fetch_switch_games()
        print(f"🟢 共找到 {len(games)} 个 Switch 游戏待抓取详情")

        tasks = []
        sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
        async def sem_task(game):
            async with sem:
                try:
                    await process_game(session, dbpool, game)
                except pymysql.err.IntegrityError as e:
                    print(f"❌ 数据库约束错误 (ID: {game.get('id')}): {e}")
                except Exception as e:
                    print(f"❌ 未知错误 (ID: {game.get('id')}): {e}")

        for g in games:
            tasks.append(sem_task(g))
        await asyncio.gather(*tasks)

    await dbpool.close()
    print("✅ 抓取完成")

if __name__ == "__main__":
    asyncio.run(main())

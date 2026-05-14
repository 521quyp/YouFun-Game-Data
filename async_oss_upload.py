import asyncio
import aiohttp
import aiomysql
import aiofiles
import oss2
import os
import json
from typing import List, Dict
from datetime import datetime

# ================= 配置 (保持不变) =================
DB_CONFIG = {
    "host": "111.231.106.90",
    "port": 3306,
    "user": "yofun_renfantian",
    "password": "FXE4c9fCs68jn5N5",
    "db": "yofun_renfantian",
    "charset": "utf8mb4",
}

OSS_ENDPOINT = "https://oss-cn-beijing.aliyuncs.com"
OSS_BUCKET = "youfanyouxi"
OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")

CDN_PREFIX = "https://cdn.yyouren.com"
CONCURRENT_REQUESTS = 10 
RETRY = 3

# ================= 工具函数 =================
def log_status(table, pk, status, icon="✅"):
    t = datetime.now().strftime("%H:%M:%S")
    print(f"[{t}] {table.ljust(15)} | ID: {str(pk).ljust(10)} | {icon} {status}")

class OSSUploader:
    def __init__(self):
        auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
        self.bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)

    async def upload_file(self, file_path: str, oss_path: str) -> bool:
        def _put():
            try:
                with open(file_path, "rb") as f:
                    self.bucket.put_object(oss_path, f)
                return True
            except Exception as e:
                print(f"❌ OSS 上传失败: {e}")
                return False
        return await asyncio.to_thread(_put)

async def download_file(session: aiohttp.ClientSession, url: str, save_path: str) -> bool:
    if not url: return False
    url = url.lstrip("/")
    url = f"{CDN_PREFIX}/{url}" if not url.startswith("http") else url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://servicewechat.com/wx7c8d593b2c3a7703/devtools/page-frame.html"
    }
    for _ in range(RETRY):
        try:
            async with session.get(url, timeout=15, headers=headers) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    async with aiofiles.open(save_path, "wb") as f:
                        await f.write(content)
                    return True
                elif resp.status == 404: return False
        except Exception:
            await asyncio.sleep(0.1)
    return False

# ================= 核心处理逻辑 =================
async def handle_record(session, pool, uploader, sem, table: str, rec: Dict):
    async with sem:
        pk = rec.get('game_id') or rec.get('id')
        safe_id = str(pk).replace("/", "_").replace(":", "_")
        
        mapping = {
            "game_ps": {"raw": "media", "oss": "media_oss", "dir": "ps", "key": "ps"},
            "game_steam": {"raw": "image", "oss": "image_oss", "dir": "steam", "key": "steam"},
            "game_switch": {"raw": "images", "oss": "images_oss", "dir": "switch", "key": "switch"},
            "games_on_sale": {"raw": None, "oss": None, "dir": "games_on_sale", "key": "games_on_sale"}
        }
        cfg = mapping[table]
        updated_fields = {}
        
        # 1. 处理 Cover (判断路径是否合法)
        cover_oss = rec.get('cover_oss') or ""
        if rec.get('cover') and cfg['key'] not in cover_oss:
            local_tmp = f"tmp/c_{safe_id}.jpg"
            oss_path = f"{cfg['dir']}/cover/{safe_id}.jpg"
            if await download_file(session, rec['cover'], local_tmp):
                if await uploader.upload_file(local_tmp, oss_path):
                    updated_fields["cover_oss"] = oss_path
                if os.path.exists(local_tmp): os.remove(local_tmp)

        # 2. 处理 Media 组
        if cfg["raw"]:
            m_oss = rec.get(cfg["oss"]) or ""
            if rec.get(cfg["raw"]) and cfg['key'] not in m_oss:
                try:
                    urls = json.loads(rec[cfg["raw"]]) if isinstance(rec[cfg["raw"]], str) else rec[cfg["raw"]]
                    if isinstance(urls, list) and len(urls) > 0:
                        new_oss_list = []
                        for idx, url in enumerate(urls):
                            l_tmp = f"tmp/m_{safe_id}_{idx}.jpg"
                            o_path = f"{cfg['dir']}/media/{safe_id}_{idx}.jpg"
                            if await download_file(session, url, l_tmp):
                                if await uploader.upload_file(l_tmp, o_path):
                                    new_oss_list.append(o_path)
                                if os.path.exists(l_tmp): os.remove(l_tmp)
                        if new_oss_list:
                            updated_fields[cfg["oss"]] = json.dumps(new_oss_list, ensure_ascii=False)
                except Exception: pass

        # 3. 入库
        if updated_fields:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    id_col = "game_id" if table == "games_on_sale" else "id"
                    set_str = ", ".join([f"{k}=%s" for k in updated_fields.keys()])
                    sql = f"UPDATE {table} SET {set_str} WHERE {id_col}=%s"
                    await cur.execute(sql, list(updated_fields.values()) + [pk])
            log_status(table, pk, f"更新成功 ({len(updated_fields)}字段)")

# ================= 主程序 (根据关键词精准搜索) =================
async def main():
    if not os.path.exists("tmp"): os.makedirs("tmp")
    pool = await aiomysql.create_pool(**DB_CONFIG, autocommit=True, maxsize=20)
    uploader = OSSUploader()
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    # 搜索配置映射
    search_map = {
        "game_ps": {"key": "ps", "m_oss": "media_oss"},
        "game_steam": {"key": "steam", "m_oss": "image_oss"},
        "game_switch": {"key": "switch", "m_oss": "images_oss"},
        "games_on_sale": {"key": "games_on_sale", "m_oss": None}
    }

    async with aiohttp.ClientSession() as session:
        for table, conf in search_map.items():
            print(f"\n📡 正在按路径关键词 [{conf['key']}] 扫描表: {table}")
            
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    # 使用 NOT LIKE 查找不包含正确路径的数据
                    sql = f"SELECT * FROM {table} WHERE (cover_oss NOT LIKE '%%{conf['key']}%%' OR cover_oss IS NULL)"
                    
                    if conf['m_oss']:
                        sql += f" OR ({conf['m_oss']} NOT LIKE '%%{conf['key']}%%' OR {conf['m_oss']} IS NULL)"
                    
                    await cur.execute(sql)
                    records = await cur.fetchall()

            if records:
                print(f"🚀 发现 {len(records)} 条路径不匹配或缺失的数据，开始处理...")
                tasks = [handle_record(session, pool, uploader, sem, table, r) for r in records]
                await asyncio.gather(*tasks)
            else:
                print(f"✅ {table} 表路径校验全部通过。")

    pool.close(); await pool.wait_closed()
    print("\n✨ 路径专项清理任务已完成!")

if __name__ == "__main__":
    asyncio.run(main())
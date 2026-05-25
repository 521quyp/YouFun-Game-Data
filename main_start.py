import subprocess
import os
import sys
import requests  # 🚀 新增：需要执行 pip install requests

# ================= 配置区 =================
SCRIPTS = [
    "async_fetch_games.py",
    # "async_ps_details.py",
    # "async_steam_details.py",
    # "async_switch_details.py",
    # "async_oss_upload.py"
]

# 服务器接口配置
REMOTE_API_URL = "https://yofun.renfantiantang.cn/api/task/sync_all"
REMOTE_TOKEN = os.getenv("REMOTE_TOKEN")
# ==========================================

def run_script(script_name):
    print(f"\n" + "="*50)
    print(f"🚀 正在启动子任务: {script_name}")
    print("="*50)
    python_exe = sys.executable
    try:
        # 加上 -u 参数确保日志实时刷新显示
        result = subprocess.run([python_exe, "-u", script_name], check=False)
        if result.returncode == 0:
            print(f"\n✅ {script_name} 任务圆满完成！")
            return True
        else:
            print(f"\n❌ {script_name} 运行异常 (退出码: {result.returncode})")
            choice = input("⚠️ 是否忽略错误继续运行下一个脚本？(y/n): ")
            return choice.lower() == 'y'
    except Exception as e:
        print(f"🚨 启动脚本时发生严重错误: {e}")
        return False

def trigger_remote_sync():
    """🚀 新增：本地任务完成后，呼叫服务器接口"""
    print("\n" + "🌐"*20)
    print("📡 正在通知服务器进行数据清理与同步...")
    print("🌐"*20)
    
    try:
        payload = {'token': REMOTE_TOKEN}
        # 设置较长超时，因为服务器执行 php think 比较耗时
        response = requests.get(REMOTE_API_URL, params=payload, timeout=60)
        
        if response.status_code == 200:
            res_data = response.json()
            if res_data.get('code') == 1:
                print("✅ 服务器同步清理成功！")
            else:
                print(f"❌ 服务器返回错误: {res_data.get('msg')}")
        else:
            print(f"❌ 远程接口请求失败，状态码: {response.status_code}")
    except Exception as e:
        print(f"🚨 无法触发服务器同步: {e}")

if __name__ == "__main__":
    print("========================================")
    print("      YouFun 游戏数据自动化同步系统      ")
    print("========================================\n")
    
    for folder in ["tmp", "log"]:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"📁 已自动创建 {folder} 文件夹")

    total_scripts = len(SCRIPTS)
    all_success = True
    
    for index, script in enumerate(SCRIPTS, 1):
        if os.path.exists(script):
            print(f"\n[任务 {index}/{total_scripts}]")
            if not run_script(script):
                all_success = False
                print("\n⛔ 流程已中止。")
                break
        else:
            print(f"\n⚠️ 跳过: 找不到脚本文件 '{script}'")

    # --- 核心改动：本地脚本跑完后，触发服务器同步 ---
    if all_success:
        trigger_remote_sync()

    print("\n" + "*"*40)
    print("🎉 所有任务处理完毕！")
    print("*"*40)
    

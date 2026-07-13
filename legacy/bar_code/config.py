"""扫码枪应用程序配置常量"""

import os
import sys


# 判断是否在 PyInstaller 打包环境中运行
# 如果是，文件路径基于 sys.executable（exe 所在目录）
# 否则基于源码文件 __file__（.py 文件所在目录）
def _get_base_dir():
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        # PyInstaller 打包环境：exe 所在目录
        return os.path.dirname(os.path.abspath(sys.executable))
    else:
        # 源码运行：.py 文件所在目录
        return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = _get_base_dir()

# 数据文件路径（与 exe 或源码同目录）
OUTPUT_FILE = os.path.join(BASE_DIR, "号码.txt")

# 验证规则
ICCID_PREFIX = "8944"
ICCID_LENGTH = 20

# 电话号码支持多个前缀：077 / 071 / 073，均为 11 位
PHONE_PREFIX = ("077", "071", "073", "074", "075", "078", "079")
PHONE_LENGTH = 11

# Tkinter 颜色与样式
COLOR_BG_NORMAL = "white"
COLOR_BG_DISABLED = "lightgray"
COLOR_TEXT_VALID = "green"
COLOR_TEXT_NORMAL = "black"
COLOR_TEXT_ERROR = "red"

# 字体（使用系统默认等宽字体保证数字对齐体验）
FONT_ENTRY = ("Consolas", 14)
FONT_LABEL = ("Microsoft YaHei", 12)
FONT_BUTTON = ("Microsoft YaHei", 12, "bold")

# 界面尺寸
WINDOW_WIDTH = 500
WINDOW_HEIGHT = 360

# 试用期配置
TRIAL_DAYS = 30
LICENSE_FILE = os.path.join(BASE_DIR, ".trial")  # 隐藏文件，存储首次运行日期


def check_trial():
    """
    检查软件试用期。
    首次运行时记录当前日期，计算到期日（首次运行日 + 30天）。
    运行日期超过到期日则禁止运行。
    
    返回: (is_valid: bool, days_left: int, message: str)
    """
    from datetime import datetime, date, timedelta

    today = date.today()

    if os.path.exists(LICENSE_FILE):
        try:
            with open(LICENSE_FILE, "r", encoding="utf-8") as f:
                first_run_str = f.read().strip()
            first_run = datetime.strptime(first_run_str, "%Y-%m-%d").date()
        except Exception:
            # 文件损坏，重置为今天
            first_run = today
            with open(LICENSE_FILE, "w", encoding="utf-8") as f:
                f.write(today.strftime("%Y-%m-%d"))
    else:
        # 首次运行，记录日期
        first_run = today
        with open(LICENSE_FILE, "w", encoding="utf-8") as f:
            f.write(today.strftime("%Y-%m-%d"))

    expiry_date = first_run + timedelta(days=TRIAL_DAYS)
    days_left = (expiry_date - today).days

    if today > expiry_date:
        return False, 0, f"试用期已结束（到期日：{expiry_date.strftime('%Y-%m-%d')}）。"
    elif days_left == 0:
        return True, 0, f"今天是试用期最后一天（到期日：{expiry_date.strftime('%Y-%m-%d')}）。"
    else:
        return True, days_left, f"试用期剩余 {days_left} 天（到期日：{expiry_date.strftime('%Y-%m-%d')}）。"


# 设备检测配置（启动前必须连接）
REQUIRED_DEVICE_VID = "VID_1A86"
REQUIRED_DEVICE_PID = "PID_55D9"
REQUIRED_DEVICE_NAME = "串口设备"


def check_required_device() -> bool:
    """检测必需的 USB 设备是否已连接（使用 ConfigManagerErrorCode 排除 ghost device 误报）。
    
    WMI Win32_PnPEntity 会返回所有曾经安装过的设备（包括 ghost device），
    设备拔掉后记录仍残留。ConfigManagerErrorCode=0 表示设备当前正常连接，
    ghost device 的 ErrorCode 为 45 或其他非零值，借此可精确区分。
    """
    try:
        import subprocess
        # 方法1: WMI Win32_PnPEntity，严格检查 ConfigManagerErrorCode=0（排除 ghost device）
        cmd = [
            "wmic", "path", "Win32_PnPEntity",
            "where", "DeviceID like '%VID_1A86&PID_55D9%' and ConfigManagerErrorCode=0",
            "get", "DeviceID", "/format:csv"
        ]
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30,
            encoding="gbk", errors="ignore",
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        output = proc.stdout.strip()
        if "VID_1A86" in output and "PID_55D9" in output:
            return True
        
        # 方法2: 如果 WMI 未找到，尝试 Win32_SerialPort（当前可用串口，设备拔掉后立即消失）
        cmd2 = [
            "wmic", "path", "Win32_SerialPort",
            "where", "PNPDeviceID like '%VID_1A86&PID_55D9%'",
            "get", "PNPDeviceID", "/format:csv"
        ]
        proc2 = subprocess.run(
            cmd2, capture_output=True, text=True, timeout=30,
            encoding="gbk", errors="ignore",
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        output2 = proc2.stdout.strip()
        if "VID_1A86" in output2 and "PID_55D9" in output2:
            return True
        
        return False
    except Exception:
        return False


# 扫码次数限制配置
MAX_SCANS = 150
COUNTER_FILE = os.path.join(BASE_DIR, ".cnt")  # 隐藏文件，存储扫码次数


def get_scan_count() -> int:
    """获取当前已扫码次数。"""
    try:
        if os.path.exists(COUNTER_FILE):
            with open(COUNTER_FILE, "r", encoding="utf-8") as f:
                return int(f.read().strip())
        # 计数文件不存在时，统计 号码.txt 已有记录数作为初始值
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                return sum(1 for line in f if line.strip())
        return 0
    except Exception:
        return 0


def increment_scan_count() -> int:
    """增加扫码次数并返回新的次数。"""
    count = get_scan_count() + 1
    try:
        with open(COUNTER_FILE, "w", encoding="utf-8") as f:
            f.write(str(count))
    except Exception:
        pass
    return count


# 提示消息持续时间（毫秒）
TOAST_DURATION = 1000

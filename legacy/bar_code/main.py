"""扫码枪应用程序入口

启动前检测必需的 WCH USB 设备 (PID_55D9)，
未检测到则弹出错误提示并退出。
"""

import tkinter as tk
from tkinter import messagebox

from config import check_required_device, REQUIRED_DEVICE_NAME, check_trial, get_scan_count, MAX_SCANS
from app import BarcodeApp


def main():
    # 启动前检测扫码次数限制
    if get_scan_count() >= MAX_SCANS:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "软件无法运行",
            "软件无法运行。"
        )
        root.destroy()
        return

    # 启动前检测必需设备
    if not check_required_device():
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "设备未连接",
            f"未检测到 {REQUIRED_DEVICE_NAME}。\n"
            "请连接扫码枪后重新启动程序。"
        )
        root.destroy()
        return

    # 启动前检查试用期
    is_valid, _, _ = check_trial()
    if not is_valid:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "软件无法运行",
            "软件无法运行。"
        )
        root.destroy()
        return

    root = tk.Tk()
    app = BarcodeApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()

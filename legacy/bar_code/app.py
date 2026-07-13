"""主应用程序：扫码枪 GUI 与状态机

修复：移除 StringVar 和 trace（trace 回调中调用 set 会导致递归和状态混乱），
改用 Entry 的 get/delete/insert 直接操作，在 KeyRelease 中统一处理过滤和验证。
"""

import tkinter as tk
from tkinter import ttk, messagebox

from config import (
    COLOR_BG_NORMAL,
    COLOR_BG_DISABLED,
    COLOR_TEXT_VALID,
    COLOR_TEXT_NORMAL,
    FONT_ENTRY,
    FONT_LABEL,
    FONT_BUTTON,
    WINDOW_WIDTH,
    WINDOW_HEIGHT,
    TOAST_DURATION,
    ICCID_LENGTH,
    PHONE_LENGTH,
    ICCID_PREFIX,
    PHONE_PREFIX,
    check_required_device,
)
from utils import validate_iccid, validate_phone, write_to_file


class BarcodeApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("扫码枪录入程序")
        self.root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
        self.root.resizable(False, False)
        self.root.configure(bg="white")

        # 居中窗口
        self._center_window()

        # 构建 UI
        self._build_ui()

        # 绑定全局回车（确认按钮有效时触发）
        self.root.bind("<Return>", self._on_global_enter)

        # 启动设备在线监控（每 3 秒检测一次）
        self._start_device_monitor()

        # 初始状态
        self.reset_state()

    # ── 窗口布局 ──
    def _center_window(self):
        self.root.update_idletasks()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = (sw - WINDOW_WIDTH) // 2
        y = (sh - WINDOW_HEIGHT) // 2
        self.root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}+{x}+{y}")

    def _build_ui(self):
        # 主容器
        frame = tk.Frame(self.root, bg="white", padx=40, pady=30)
        frame.pack(expand=True, fill=tk.BOTH)

        # ICCID 区域
        tk.Label(
            frame, text="ICCID 输入", font=FONT_LABEL, bg="white", anchor="w"
        ).pack(fill=tk.X, pady=(0, 5))

        self.iccid_entry = tk.Entry(
            frame,
            font=FONT_ENTRY,
            justify="center",
            relief="solid",
            bd=1,
        )
        self.iccid_entry.pack(fill=tk.X, pady=(0, 15))
        self.iccid_entry.bind("<KeyRelease>", self._on_iccid_key_release)

        # 电话号码区域
        tk.Label(
            frame, text="电话号码", font=FONT_LABEL, bg="white", anchor="w"
        ).pack(fill=tk.X, pady=(0, 5))

        self.phone_entry = tk.Entry(
            frame,
            font=FONT_ENTRY,
            justify="center",
            relief="solid",
            bd=1,
        )
        self.phone_entry.pack(fill=tk.X, pady=(0, 25))
        self.phone_entry.bind("<KeyRelease>", self._on_phone_key_release)

        # 按钮区域
        btn_frame = tk.Frame(frame, bg="white")
        btn_frame.pack(fill=tk.X)

        self.reset_btn = tk.Button(
            btn_frame,
            text="重置",
            font=FONT_BUTTON,
            width=10,
            command=self.reset_state,
        )
        self.reset_btn.pack(side=tk.LEFT, padx=(0, 20))

        self.confirm_btn = tk.Button(
            btn_frame,
            text="确认",
            font=FONT_BUTTON,
            width=10,
            command=self.on_confirm,
        )
        self.confirm_btn.pack(side=tk.RIGHT, padx=(20, 0))

        # 提示标签（默认隐藏）
        self.toast_label = tk.Label(
            frame, text="", font=FONT_LABEL, bg="white", fg="green"
        )
        self.toast_label.pack(pady=(15, 0))

    # ── 设备在线监控：运行时拔出授权设备则强制退出 ──
    def _start_device_monitor(self):
        """启动后台定时器，定期检测授权设备是否在线。"""
        self.root.after(3000, self._check_device_loop)

    def _check_device_loop(self):
        """每 3 秒检测一次。若授权设备已断开，弹出提示并退出程序。"""
        if not check_required_device():
            messagebox.showerror(
                "软件无法运行",
                "软件无法运行。"
            )
            self.root.destroy()
            return
        # 设备仍在，继续下一轮检测
        self.root.after(3000, self._check_device_loop)

    # ── Entry 内容过滤：只允许数字，截断长度，检查前缀 ──
    def _sanitize_entry(self, entry, max_len, prefixes):
        """获取 Entry 文本，过滤非数字，截断长度，检查前缀。
        prefixes 可以是单个字符串或字符串元组/列表。
        如果文本被修改，用 delete/insert 更新 Entry（避免 StringVar trace 递归）。
        """
        # 统一为 tuple
        if isinstance(prefixes, str):
            prefixes = (prefixes,)

        text = entry.get()
        filtered = "".join(ch for ch in text if ch.isdigit())
        if len(filtered) > max_len:
            filtered = filtered[:max_len]

        # 前缀检查：
        # - 正在逐步输入前缀时：filtered 必须是某个前缀的前缀（如 "07" 是 "077" 的前缀）
        # - 前缀已完整输入后：filtered 必须以某个完整前缀开头（如 "0772" 以 "077" 开头）
        if filtered:
            valid = any(
                prefix.startswith(filtered) or filtered.startswith(prefix)
                for prefix in prefixes
            )
            if not valid:
                filtered = ""

        # 只有真正变化时才修改 Entry（减少闪烁和焦点干扰）
        if filtered != text:
            entry.delete(0, tk.END)
            if filtered:
                entry.insert(0, filtered)

        return filtered

    # ── KeyRelease：检测输入是否完成 ──
    def _on_iccid_key_release(self, event):
        """ICCID 输入完成后自动切换到电话框。"""
        if str(self.iccid_entry.cget("state")) != tk.NORMAL:
            return
        text = self._sanitize_entry(self.iccid_entry, ICCID_LENGTH, ICCID_PREFIX)
        if validate_iccid(text):
            self._set_iccid_editable(False)
            self._set_phone_editable(True)
            self.phone_entry.focus_set()
            self.confirm_btn.config(state=tk.DISABLED)

    def _on_phone_key_release(self, event):
        """电话号码输入完成后自动激活确认按钮。"""
        if str(self.phone_entry.cget("state")) != tk.NORMAL:
            return
        text = self._sanitize_entry(self.phone_entry, PHONE_LENGTH, PHONE_PREFIX)
        if validate_phone(text):
            self._set_phone_editable(False)
            self.confirm_btn.config(state=tk.NORMAL)
            self.confirm_btn.focus_set()

    # ── 状态管理 ──
    def reset_state(self):
        """回到默认初始状态：强制清空并重置"""
        # 先解除只读，清空内容
        self.iccid_entry.config(state=tk.NORMAL)
        self.iccid_entry.delete(0, tk.END)

        self.phone_entry.config(state=tk.NORMAL)
        self.phone_entry.delete(0, tk.END)

        # 应用正确的初始状态
        self._set_iccid_editable(True)
        self._set_phone_editable(False)

        self.iccid_entry.focus_set()
        self.reset_btn.config(state=tk.NORMAL)
        self.confirm_btn.config(state=tk.DISABLED)

        self._hide_toast()

    def _set_iccid_editable(self, editable: bool):
        if editable:
            self.iccid_entry.config(
                state=tk.NORMAL,
                bg=COLOR_BG_NORMAL,
                fg=COLOR_TEXT_NORMAL,
                readonlybackground=COLOR_BG_NORMAL,
            )
        else:
            self.iccid_entry.config(
                state="readonly",
                readonlybackground=COLOR_BG_DISABLED,
                fg=COLOR_TEXT_VALID,
            )

    def _set_phone_editable(self, editable: bool):
        if editable:
            self.phone_entry.config(
                state=tk.NORMAL,
                bg=COLOR_BG_NORMAL,
                fg=COLOR_TEXT_NORMAL,
                readonlybackground=COLOR_BG_NORMAL,
            )
        else:
            self.phone_entry.config(
                state="readonly",
                readonlybackground=COLOR_BG_DISABLED,
                fg=COLOR_TEXT_VALID,
            )

    # ── 确认与写入 ──
    def _on_global_enter(self, event):
        """全局回车键：仅在确认按钮有效时触发"""
        if str(self.confirm_btn.cget("state")) == tk.NORMAL:
            self.on_confirm()

    def on_confirm(self):
        iccid = self.iccid_entry.get()
        phone = self.phone_entry.get()

        if not validate_iccid(iccid) or not validate_phone(phone):
            return

        write_to_file(iccid, phone)

        # 显示提示，0.5 秒后自动复位
        self.toast_label.config(text="号码已写入")
        self.root.after(TOAST_DURATION, self.reset_state)

    def _hide_toast(self):
        self.toast_label.config(text="")


if __name__ == "__main__":
    root = tk.Tk()
    app = BarcodeApp(root)
    root.mainloop()

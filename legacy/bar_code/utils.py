"""工具函数：验证与文件写入"""

from config import (
    OUTPUT_FILE, ICCID_PREFIX, ICCID_LENGTH,
    PHONE_PREFIX, PHONE_LENGTH, increment_scan_count,
)


def validate_iccid(text: str) -> bool:
    """验证 ICCID：必须以 8944 开头，且恰好 20 位纯数字"""
    if not text:
        return False
    return (
        text.startswith(ICCID_PREFIX)
        and text.isdigit()
        and len(text) == ICCID_LENGTH
    )


def validate_phone(text: str) -> bool:
    """验证电话号码：必须以 077 / 071 / 073 之一开头，且恰好 11 位纯数字"""
    if not text:
        return False
    return (
        any(text.startswith(p) for p in PHONE_PREFIX)
        and text.isdigit()
        and len(text) == PHONE_LENGTH
    )


def write_to_file(iccid: str, phone: str) -> None:
    """将 ICCID 和电话号码追加写入文件，格式：ICCID, 电话号码"""
    line = f"{iccid}, {phone}\n"
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(line)
    # 写入成功后增加扫码次数
    increment_scan_count()

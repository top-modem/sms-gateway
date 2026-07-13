"""核心逻辑测试脚本"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import validate_iccid, validate_phone, write_to_file
from config import OUTPUT_FILE, ICCID_LENGTH, PHONE_LENGTH


def test_validate_iccid():
    assert validate_iccid("89441000305104837680") is True
    assert validate_iccid("8944100030510483768") is False   # 19位
    assert validate_iccid("894410003051048376801") is False  # 21位
    assert validate_iccid("99441000305104837680") is False   # 前缀错误
    assert validate_iccid("8944abcd305104837680") is False   # 非数字
    assert validate_iccid("") is False
    print("[OK] ICCID 验证测试通过")


def test_validate_phone():
    assert validate_phone("07721447303") is True
    assert validate_phone("0772144730") is False   # 10位
    assert validate_phone("077214473030") is False  # 12位
    assert validate_phone("07821447303") is False  # 前缀错误
    assert validate_phone("0772144730a") is False  # 非数字
    assert validate_phone("") is False
    print("[OK] 电话号码验证测试通过")


def test_write_to_file():
    # 清理已有测试数据
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    write_to_file("89441000305104837680", "07721447303")
    write_to_file("89441000305104837120", "07721447305")

    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 2
    assert lines[0].strip() == "89441000305104837680, 07721447303"
    assert lines[1].strip() == "89441000305104837120, 07721447305"

    # 追加测试
    write_to_file("89441000305104837000", "07721447306")
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()
    assert len(lines) == 3

    print("[OK] 文件写入测试通过")
    print(f"   测试文件路径: {OUTPUT_FILE}")


if __name__ == "__main__":
    test_validate_iccid()
    test_validate_phone()
    test_write_to_file()
    print("\n所有核心逻辑测试全部通过！")

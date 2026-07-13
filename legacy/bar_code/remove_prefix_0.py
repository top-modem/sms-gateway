import csv

with open('号码.txt', 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        iccid, phone = line.split(', ')
        phone = phone.lstrip('0')
        print(f'{iccid}, {phone}')

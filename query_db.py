#!/usr/bin/env python3
import sqlite3
import os

db_path = 'sms_gateway.db'
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# 1) Tables list
print('='*80)
print('1) TABLES IN DATABASE')
print('='*80)
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
tables = cursor.fetchall()
for table in tables:
    print(f'  {table[0]}')

# 2) Rows where message contains '634485' or 'Try to usethis'
print('\n' + '='*80)
print('2) SMS ROWS WITH MESSAGE CONTAINING "634485" OR "Try to usethis"')
print('='*80)
cursor.execute("SELECT * FROM sms WHERE message LIKE '%634485%' OR message LIKE '%Try to usethis%';")
rows = cursor.fetchall()
if rows:
    print(f'Found {len(rows)} rows:')
    for i, row in enumerate(rows, 1):
        print(f'\nRow {i}:')
        row_dict = dict(row)
        for key, value in row_dict.items():
            print(f'  {key}: {value}')
else:
    print('  No rows found')

# 3) Rows where contact_id contains '15615431242'
print('\n' + '='*80)
print('3) SMS ROWS WITH CONTACT_ID CONTAINING "15615431242"')
print('='*80)
cursor.execute("SELECT * FROM sms WHERE contact_id LIKE '%15615431242%';")
rows = cursor.fetchall()
if rows:
    print(f'Found {len(rows)} rows:')
    for i, row in enumerate(rows, 1):
        print(f'\nRow {i}:')
        row_dict = dict(row)
        for key, value in row_dict.items():
            print(f'  {key}: {value}')
else:
    print('  No rows found')

# 4) Latest 15 rows with sim_id='8944110069325158593F'
print('\n' + '='*80)
print('4) LATEST 15 SMS ROWS WITH SIM_ID="8944110069325158593F"')
print('='*80)
cursor.execute("SELECT * FROM sms WHERE sim_id='8944110069325158593F' ORDER BY rowid DESC LIMIT 15;")
rows = cursor.fetchall()
if rows:
    print(f'Found {len(rows)} rows (latest first):')
    for i, row in enumerate(rows, 1):
        print(f'\nRow {i}:')
        row_dict = dict(row)
        for key, value in row_dict.items():
            print(f'  {key}: {value}')
else:
    print('  No rows found')

conn.close()
print('\n' + '='*80)
print('Query completed successfully')
print('='*80)

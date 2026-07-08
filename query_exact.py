#!/usr/bin/env python3
import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), 'sms_gateway.db')

if not os.path.exists(db_path):
    print(f'ERROR: Database file not found at {db_path}')
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('=' * 80)
print('SCHEMA: sms table')
print('=' * 80)
cursor.execute('PRAGMA table_info(sms)')
sms_schema = cursor.fetchall()
for row in sms_schema:
    print(row)

print('\n' + '=' * 80)
print('SCHEMA: contacts table')
print('=' * 80)
try:
    cursor.execute('PRAGMA table_info(contacts)')
    contacts_schema = cursor.fetchall()
    for row in contacts_schema:
        print(row)
except Exception as e:
    print(f'Error querying contacts table: {e}')

print('\n' + '=' * 80)
print('SCHEMA: sim_cards table')
print('=' * 80)
try:
    cursor.execute('PRAGMA table_info(sim_cards)')
    sim_cards_schema = cursor.fetchall()
    for row in sim_cards_schema:
        print(row)
except Exception as e:
    print(f'Error querying sim_cards table: {e}')

print('\n' + '=' * 80)
print('SPECIFIC ROW: id = b7bd848c-91a7-47db-9394-5dd41b895452')
print('=' * 80)
try:
    cursor.execute('SELECT * FROM sms WHERE id = ?', ('b7bd848c-91a7-47db-9394-5dd41b895452',))
    row = cursor.fetchone()
    if row:
        column_names = [description[0] for description in cursor.description]
        for name, value in zip(column_names, row):
            print(f'{name}: {value}')
    else:
        print('No row found with this id')
except Exception as e:
    print(f'Error: {e}')

print('\n' + '=' * 80)
print('QUERY: SELECT id, contact_id, timestamp, message, sim_id, send, status FROM sms WHERE message LIKE \'%634485%\'')
print('=' * 80)
try:
    cursor.execute('SELECT id, contact_id, timestamp, message, sim_id, send, status FROM sms WHERE message LIKE ?', ('%634485%',))
    rows = cursor.fetchall()
    if rows:
        print(f'Found {len(rows)} matching row(s):')
        for row in rows:
            print(f'  id: {row[0]}, contact_id: {row[1]}, timestamp: {row[2]}, message: {row[3]}, sim_id: {row[4]}, send: {row[5]}, status: {row[6]}')
    else:
        print('No rows found matching the pattern')
except Exception as e:
    print(f'Error: {e}')

print('\n' + '=' * 80)
print('SAMPLE DATA: sim_cards table (LIMIT 20)')
print('=' * 80)
try:
    cursor.execute('SELECT id, phone_number, country_code FROM sim_cards LIMIT 20')
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(f'  id: {row[0]}, phone_number: {row[1]}, country_code: {row[2]}')
    else:
        print('No data in sim_cards table')
except Exception as e:
    print(f'Error: {e}')

print('\n' + '=' * 80)
print('SAMPLE DATA: contacts table (LIMIT 20)')
print('=' * 80)
try:
    cursor.execute('SELECT * FROM contacts LIMIT 20')
    rows = cursor.fetchall()
    if rows:
        column_names = [description[0] for description in cursor.description]
        for row in rows:
            print('  ' + ', '.join([f'{name}: {value}' for name, value in zip(column_names, row)]))
    else:
        print('No data in contacts table')
except Exception as e:
    print(f'Error: {e}')

conn.close()
print('\n' + '=' * 80)
print('Query completed successfully')
print('=' * 80)

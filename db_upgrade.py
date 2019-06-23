# script to upgrade the db from original version (named 0 from now) to version 1

# imports
import sqlite3
from datetime import datetime

# connect to DB
conn = sqlite3.connect('./tx_cache.db')
c = conn.cursor()

# fetch data
c.execute("SELECT * FROM transactions")
db = c.fetchall()
print('We have found', len(db), 'records in the old table.')

# eliminate duplicates and sort by version
db = list(set(db))
db.sort(key=lambda x: x[0])

# add column based on strptime
str_to_datetime = lambda x: int(datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp())
new_db = [(*x, str_to_datetime(x[1])) for x in db]
print('highest version found is:', new_db[-1][0])

# rename table
c.execute("ALTER TABLE transactions RENAME TO TempOldTable;")

# create new table
c.execute('''CREATE TABLE transactions
             (version INTEGER NOT NULL PRIMARY KEY, expiration_date text, src text, dest text, 
             type text, amount real, gas_price real, max_gas real, sq_num INTEGER, pub_key text,
             expiration_unixtime INTEGER)''')

# insert data
c.executemany("INSERT INTO transactions VALUES(?,?,?,?,?,?,?,?,?,?,?);", new_db)
print('We have inserted', c.rowcount, 'records to the table.')

# delete old table
c.execute("DROP TABLE TempOldTable;")

# connection
conn.commit()
conn.close()

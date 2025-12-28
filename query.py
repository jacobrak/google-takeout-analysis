import sqlite3
import pandas as pd

conn = sqlite3.connect("mail.sqlite")

# 1) Most messaged day (count of emails)
df = pd.read_sql_query("""
SELECT date_day, COUNT(*) AS n
FROM emails
WHERE date_day != ''
GROUP BY date_day
ORDER BY n DESC
LIMIT 5
""", conn)
print(df)
print("\n")
# 2) Most common senders
df = pd.read_sql_query("""
SELECT from_addr, COUNT(*) AS n
FROM emails
GROUP BY from_addr
ORDER BY n DESC
LIMIT 5
""", conn)
print(df)
print("\n")     

# 3) most common subjects
df = pd.read_sql_query("""
SELECT subject, COUNT(*) AS n
FROM emails
GROUP BY subject
ORDER BY n DESC
LIMIT 5
""", conn)
print(df)
print("\n")

# 4) most common recipients
df = pd.read_sql_query("""
SELECT to_addr, COUNT(*) AS n
FROM emails
GROUP BY to_addr
ORDER BY n DESC
LIMIT 5
""", conn)
print(df)
print("\n")
conn.close()
import psycopg2
import pandas


## PostgreSQLに接続
conn = psycopg2.connect(
    host="localhost",
    database="company_db",
    user="postgres",
    password=""
)

#cur（カーソル）はその接続を使って実際にSQLを実行する窓口
cur = conn.cursor()

# SQLを実行
cur.execute("SELECT name, position, department_id FROM employees;")

#結果を全件取得
rows = cur.fetchall()

# cur.description : カラム名の情報を取得
columns = [desc[0] for desc in cur.description]

# 接続を閉じる
cur.close()
conn.close()

# クエリ結果をDataFrameに変換
df = pandas.DataFrame(rows, columns=columns)





import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

## PostgreSQLに接続
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
#cur（カーソル）はその接続を使って実際にSQLを実行する窓口
cur = conn.cursor()

# SQLを実行
cur.execute("""
            SELECT
                *
            FROM employees;
            """)

#結果を全件取得
rows = cur.fetchall()

# cur.description : カラム名の情報を取得
columns = [desc[0] for desc in cur.description]



def get_dataframe(cur,sql_message):
    # SQLを実行
    cur.execute(sql_message)

    #結果を全件取得
    rows = cur.fetchall()

    # cur.description : カラム名の情報を取得
    columns = [desc[0] for desc in cur.description]

    # クエリ結果をDataFrameに変換
    df = pd.DataFrame(rows, columns=columns)

    return df


df_employees = get_dataframe(cur,
                """
                SELECT
                    *
                FROM employees;
                """)

df_departments = get_dataframe(cur,
                """
                SELECT
                    id as department_id,
                    name as department_name,
                    location
                FROM departments;
                """)


result = pd.merge(df_employees, df_departments, on="department_id", how="left")
print(result)


# 接続を閉じる
cur.close()
conn.close()

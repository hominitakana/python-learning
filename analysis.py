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


df_emp_dept  = pd.merge(df_employees, df_departments, on="department_id", how="left")
department_count = df_emp_dept.groupby("department_name")["id"].count().reset_index()
department_count.columns = ["部署名", "人数"]
desc_department_count = department_count.sort_values("人数", ascending=False)

print(desc_department_count)


# 接続を閉じる
cur.close()
conn.close()

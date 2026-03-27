import psycopg2
import pandas
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
                e.id, e.name , e.position, d.id as department_id, d.name as department
            FROM employees e
            join departments d on e.department_id = d.id;
            """)

#結果を全件取得
rows = cur.fetchall()

# cur.description : カラム名の情報を取得
columns = [desc[0] for desc in cur.description]

# 接続を閉じる
cur.close()
conn.close()

# クエリ結果をDataFrameに変換
df = pandas.DataFrame(rows, columns=columns)

#部署ごとに社員数を取得
department_employee_count = df.groupby('department')['id'].count().reset_index()
department_employee_count.columns = ['department', '社員数']

# 社員数が最大のインデックスを取得
max_employee_department_idx = department_employee_count['社員数'].idxmax()
# 上記で取得したindexの行を取得
max_employee_department = department_employee_count.loc[max_employee_department_idx]

print(department_employee_count)
print(max_employee_department)






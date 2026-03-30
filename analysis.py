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

# SQLを取得、データフレームワークに変換するまでの関数
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



# employeesのdfを取得
df_employees = get_dataframe(cur,
                """
                SELECT
                    id as employee_id,
                    name as employee_name,
                    department_id
                    --position,
                    --hire_date
                FROM employees;
                """)

# departmentsのdfを取得
df_departments = get_dataframe(cur,
                """
                SELECT
                    id as department_id,
                    name as department_name,
                    location
                FROM departments;
                """)

# salariesのdfを取得
df_salaries  = get_dataframe(cur,
                """
                SELECT
                    id as salaries_id,
                    employee_id,
                    amount
                FROM salaries ;
                """)


# 部署ごとに人数を多い順に並べる
df_employees_departments  = pd.merge(df_employees, df_departments, on="department_id", how="left")
department_count = df_employees_departments.groupby("department_name")["department_id"].count().reset_index()
department_count.columns = ["部署名", "人数"]
desc_department_count = department_count.sort_values("人数", ascending=False)

# 部署ごとの平均給与が高い順に並べる
df_employees_salaries_departments  = pd.merge(df_employees_departments, df_salaries, on="employee_id", how="inner")
department_avg_amount = df_employees_salaries_departments.groupby("department_name")["amount"].mean().reset_index()

department_avg_amount.columns = ["部署名", "平均給与"]
desc_department_avg_amount = department_avg_amount.sort_values("平均給与", ascending=False)


#部署名 / 人数 / 平均給与 / 最高給与 / 最低給与 の5カラム
department_max_amount = df_employees_salaries_departments.groupby("department_name")["amount"].max().reset_index()
department_min_amount = df_employees_salaries_departments.groupby("department_name")["amount"].min().reset_index()

department_max_amount.columns = ["部署名", "最高給与"]
department_min_amount.columns = ["部署名", "最低給与"]

df_departments_amount_all = (department_count
                                .merge(department_avg_amount, on="部署名", how="inner")
                                .merge(department_max_amount,  on="部署名", how="inner")
                                .merge(department_min_amount,  on="部署名", how="inner")
                                )

df_departments_amount_all['平均給与'] = df_departments_amount_all['平均給与'].astype('int')
df_departments_amount_all['最高給与'] = df_departments_amount_all['最高給与'].astype('int')
df_departments_amount_all['最低給与'] = df_departments_amount_all['最低給与'].astype('int')

df_departments_amount_all = df_departments_amount_all.sort_values("平均給与", ascending=False)


# print(desc_department_count)
# print(desc_department_avg_amount)
print(df_departments_amount_all)


# 接続を閉じる
cur.close()
conn.close()

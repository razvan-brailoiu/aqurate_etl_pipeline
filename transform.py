# script for querying database for field ' period ' = 30
import sqlite3
import pandas as pd
from pathlib import Path
connection = sqlite3.connect('mY-db.db')
def select_from_table(conn, tableName):
    cur = conn.cursor()
    query = "SELECT * FROM " + tableName + " WHERE period=30"
    cur.execute(query)
    rows = cur.fetchall()
    # for row in rows:
    #     print(row)
    return rows


queried_from_first_table = select_from_table(conn=connection, tableName='vw_chart_5_4')
queried_from_second_table = select_from_table(conn=connection, tableName='vw_chart_5_3')


print(type(queried_from_first_table))


#merging with pandas
data_frame_one = pd.DataFrame(queried_from_first_table)
data_frame_two = pd.DataFrame(queried_from_second_table)

data_frame_one.columns = ['index', 'period', 'product_name', 'product_sku', 'net_sales', 'gross_margin']
data_frame_two.columns = ['index', 'period', 'product_name', 'product_sku', 'net_sales', 'gross_margin']


result = pd.merge(data_frame_one, data_frame_two, on='product_sku')
path_to_csv = Path('joiningByPandas.csv')
result.to_csv(path_to_csv)
print('Results successfully stored in ', path_to_csv)


#merging directly from query
def query_and_join_directly(conn, tableOne, tableTwo):
    cur = conn.cursor()
    query = 'SELECT * FROM vw_chart_5_4 INNER JOIN vw_chart_5_3 ON vw_chart_5_4.product_sku=vw_chart_5_3.product_sku WHERE vw_chart_5_4.period=30 AND vw_chart_5_3.period=30 ;'
    cur.execute(query)
    rows = cur.fetchall()
    # for row in rows:
    #     print(row)
    return rows


data_frame_joined = pd.DataFrame(query_and_join_directly(conn=connection, tableOne='vw_chart_5_4', tableTwo='vw_chart_5_3'))
# print(data_frame_joined)
my_path = Path('joiningByQuery.csv')
data_frame_joined.to_csv(my_path)
print('Result successfully stored in ', my_path)
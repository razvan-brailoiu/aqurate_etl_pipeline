# script for querying database for field ' period ' = 30
import sqlite3
import pandas as pd
from pathlib import Path

# function for querying the 2 tables separately
connection = sqlite3.connect('mY-db.db')
def select_from_table(conn, tableName):
    cur = conn.cursor()
    query = "SELECT * FROM " + tableName + " WHERE period=30"
    cur.execute(query)
    rows = cur.fetchall()
    return rows


#getting the records as lists
queried_from_first_table = select_from_table(conn=connection, tableName='vw_chart_5_4')
queried_from_second_table = select_from_table(conn=connection, tableName='vw_chart_5_3')

#transforming them into dataframes
data_frame_one = pd.DataFrame(queried_from_first_table)
data_frame_two = pd.DataFrame(queried_from_second_table)

string_for_columns_tbOne = ['index', 'period', 'product_name', 'product_sku', 'net_sales', 'gross_margin']
string_for_columns_tbTwo = ['index', 'period', 'product_name', 'product_sku', 'orders', 'page_views']

data_frame_one.columns = string_for_columns_tbOne
data_frame_two.columns = string_for_columns_tbTwo

# merging using pandas
result = pd.merge(data_frame_one, data_frame_two, on='product_sku')
path_to_csv = Path('joiningByPandas.csv')
result.to_csv(path_to_csv)
print('Results successfully stored in ', path_to_csv)


#merging directly from query
def query_and_join_directly(conn):
    cur = conn.cursor()
    query = 'SELECT * FROM vw_chart_5_4 INNER JOIN vw_chart_5_3 ON vw_chart_5_4.product_sku=vw_chart_5_3.product_sku WHERE vw_chart_5_4.period=30 AND vw_chart_5_3.period=30 ;'
    cur.execute(query)
    rows = cur.fetchall()
    return rows

# creating dataframe object
data_frame_joined = pd.DataFrame(query_and_join_directly(conn=connection))
data_frame_joined.columns = string_for_columns_tbOne + string_for_columns_tbTwo
my_path = Path('joiningByQuery.csv')
data_frame_joined.to_csv(my_path)
print('Result successfully stored in ', my_path)


# In my opinion, it is better to join directly from the sql query because if we were to have more than 
# 2 tables, creating a data frame object and then merging would be too much work, I would rather just increase 
# the length of the query string



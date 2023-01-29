import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from pandas import json_normalize
from pathlib import Path
url_post = 'https://staging-dot-merlin-api-dot-camelot-1.ey.r.appspot.com/v1/account/login'
myjson = { 'email' : 'ce2@aqurate.ai',
        'password' : 'g7vxtHur9GsG5HMG3LJ'}

response = requests.post(url_post, myjson)
response_as_json = response.json()
authentication_token = response_as_json['token']['X_Auth_Token']
print(response_as_json)
print(type(authentication_token))



url_get = 'https://staging-dot-merlin-api-dot-camelot-1.ey.r.appspot.com/v1/get_table'
header = { 'X-Auth-Token' : authentication_token}

def get_data (table_name):
    get_first_table_json = {
    'table_name' : table_name,
    'limit' : 10,
    'header' : authentication_token
    }
    response = requests.get(url_get,get_first_table_json, headers= header )
    return response.json()


data_from_table_one = get_data('vw_chart_5_4')
# data_from_table_two = get_data('vw_chart_5_3')


column_list = []
dictionary_data = {}
for data in data_from_table_one['columns']:
    column_list.append(data['column_name'])
    dictionary_data[data['column_name']] = data['data']

print(column_list)
print(dictionary_data)
# print(data_from_table_one)

first_data_frame = pd.DataFrame.from_dict(dictionary_data)
print(first_data_frame  )
# df = json_normalize(data_from_table_one['columns'])
# print(df)
#print(data_from_table_one['columns'][1])

# print(data_from_table_one)
# dfOne = pd.DataFrame(data_from_table_one)
# print(dfOne)
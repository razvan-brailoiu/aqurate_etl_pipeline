import requests
import pandas as pd
import sqlite3

# posting the data (email, pass) to the api, in order to get back the token
url_post = 'https://staging-dot-merlin-api-dot-camelot-1.ey.r.appspot.com/v1/account/login'
myjson = { 'email' : 'ce2@aqurate.ai',
        'password' : 'g7vxtHur9GsG5HMG3LJ'}

#getting the response token
response = requests.post(url_post, myjson)
response_as_json = response.json()
authentication_token = response_as_json['token']['X_Auth_Token']


#getting the data by using the token
url_get = 'https://staging-dot-merlin-api-dot-camelot-1.ey.r.appspot.com/v1/get_table'
header = { 'X-Auth-Token' : authentication_token}

#function to retrieve the data as json object
def get_data (table_name):
    get_first_table_json = {
    'table_name' : table_name,
    'limit' : 15000,
    'header' : authentication_token
    }
    response = requests.get(url_get,get_first_table_json, headers= header )
    return response.json()


data_from_table_one = get_data('vw_chart_5_4')
data_from_table_two = get_data('vw_chart_5_3')

#function to wrangle the data and obtain it into a clean, dataFrame object
def get_data_from_json(tableInJson):
    dictionary_data = { data['column_name'] : data['data'] for data in tableInJson['columns']}
    data_frame = pd.DataFrame.from_dict(dictionary_data)
    return data_frame


first_data_frame = get_data_from_json(data_from_table_one)
second_data_frame = get_data_from_json(data_from_table_two)


# putting it in the sqlite tables
connection = sqlite3.connect('mY-db.db')
first_data_frame.to_sql('vw_chart_5_4',connection,if_exists = 'replace', index=True)
second_data_frame.to_sql('vw_chart_5_3',connection,if_exists = 'replace', index=True)
connection.commit()
connection.close()
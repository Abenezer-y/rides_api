from typing import Union
from datetime import datetime
import requests
import json
import pandas as pd
from dataclasses import dataclass, asdict, replace, astuple
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


wwl_db = {'cluster': "WWL", 
'uri': 'https://data.mongodb-api.com/app/data-qvnrx/endpoint/data/v1' ,
'db': "wwl" ,
'key': "5d9sGO28viSiX1HnJlOLN6QMqPqxYz6NIKVUMvEU8wXvAS0CPHMMHs2jF0UHKSCF"}
@dataclass
class Rides_read:
    name: str
    key: str 
    status: list
    
def update_data(collection, condition, doc,  credential):
    headers = {'Content-Type': 'application/json', 'Access-Control-Request-Headers': '*','api-key': credential['key']}
    updateOne_url = f"{credential['uri']}/action/updateOne"
    Payload = json.dumps({"collection": collection, "database": credential['db'], "dataSource": credential['cluster'], "filter": condition, "update":{"$set": doc}})
    response = requests.request("POST", updateOne_url, headers=headers, data=Payload)
    return response

def get_data_json(collection, credential):
    headers = {'Content-Type': 'application/json', 'Access-Control-Request-Headers': '*','api-key': credential['key']}
    findAll_url =  f"{credential['uri']}/action/find"
    Payload = json.dumps({"collection": collection, "database":credential['db'], "dataSource": credential['cluster'], "filter": {}, "limit":5000})
    response = requests.request("POST", findAll_url, headers=headers, data=Payload)
    response_json = response.json()['documents']
    return response_json

def get_data_df(collection, credential):
    headers = {'Content-Type': 'application/json', 'Access-Control-Request-Headers': '*','api-key': credential['key']}
    findAll_url =  f"{credential['uri']}/action/find"
    Payload = json.dumps({"collection": collection, "database":credential['db'], "dataSource": credential['cluster'], "filter": {}, "limit":5000})
    response = requests.request("POST", findAll_url, headers=headers, data=Payload)
    response_json = response.json()['documents']
    df = pd.read_json(json.dumps(response_json))
    return df



app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], 
    expose_headers = ['*']
)


@app.get("/", tags=["root"])
def read_root():
    return {"Hello": "World"}


@app.get("/rides", tags=["root"])
def read_rides():
    df_db = get_data_df(collection='rides', credential=wwl_db).sort_values('name')
    row_count = df_db.shape[0]
    ride_read_list = []
    name_lt = df_db['name'].values.tolist()
    key_lt = df_db['_id'].values.tolist()
    Status_lt = df_db['status'].values.tolist()
    for i in range(row_count):
        record = Rides_read(status=[Status_lt[i], key_lt[i]], key=str(key_lt[i]), name=name_lt[i])
        ride_read_list.append(asdict(record))
    return ride_read_list

@app.post("/editStatus")
def edit_status(status: dict):
    df_db = get_data_df(collection='rides', credential=wwl_db).sort_values('name')
    data = df_db[df_db['_id']==status['_id']]
    if status['status'] == True:
        s = 'Open'
    else:
        s = 'Closed'
    cond = {'name': data['name'].values.tolist()[0], 'manufacturer': data['manufacturer'].values.tolist()[0]}
    doc = {'name': data['name'].values.tolist()[0], 'status': s, 'manufacturer': data['manufacturer'].values.tolist()[0]}
    update_data('rides', cond, doc, wwl_db)
    return 'Updated'

# @app.get("/items/{item_id}")
# def read_item(item_id: int, q: Union[str, None] = None):

#     return {"item_id": item_id, "q": q}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

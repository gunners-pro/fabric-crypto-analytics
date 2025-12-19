# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fe6ebbc7-584b-4dfa-8551-2e146c29865b",
# META       "default_lakehouse_name": "lh_crypto",
# META       "default_lakehouse_workspace_id": "cf249472-6085-4a46-9436-f2a2075428d1",
# META       "known_lakehouses": [
# META         {
# META           "id": "fe6ebbc7-584b-4dfa-8551-2e146c29865b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
from datetime import datetime
from requests import Response

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1
}

response: Response = requests.get(API_URL, params=PARAMS)
response.raise_for_status()

data = response.json()

now = datetime.utcnow()
path = f"Files/bronze/crypto/{now:%Y/%m/%d}/crypto_{now:%H%M%S}.json"

notebookutils.fs.put(path, json.dumps(data),True)

print(f"Bronze data written to {path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

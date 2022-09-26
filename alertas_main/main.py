import pandas as pd
from pymongo import MongoClient
import json
pd.options.mode.chained_assignment = None  # default='warn'

from utils import *

# find outliers using IQR
def find_outliers_IQR(df):
   q1 = df.quantile(0.25)
   q3 = df.quantile(0.75)
   IQR = q3 - q1
   # outliers = df[((df<(q1-1.5*IQR)) | (df>(q3+1.5*IQR)))]
   # outliers = ((df<(q1-1.5*IQR)) | (df>(q3+1.5*IQR)))
   outliers = df>(q3+1.5*IQR)
   return outliers, q3+1.5*IQR

client = MongoClient("mongodb://usr_ia:dut_tu6.uioA@sprdbdsmng12.syc.loc:27017,sprdbdsmng13.syc.loc:27017,scdabdsmng01.syc.loc:27017/?authSource=admin&replicaSet=replEbank&w=majority&readPreference=secondary&appname=MongoDB%20Compass&retryWrites=true&ssl=false")
ebk_logs = client.ebk_logs
collection = ebk_logs['Coomeva']
Document = list(collection.find({'Periodo':'202205'},projection={'_id':0}))
client.close() 

df = createDataFrame(Document)
variables = {'Alerta por reprecesos de estado':'Procesos radicado-estado',
            'Alerta por reprocesos de combinación de estados': 'Procesos radicado - Combinacion de estado',
            'Alerta por dias laborados por estado':'Dias laborados / Estado',
            'Alerta por radicados que duran más que el ANS':'Dias laborados / Radicado'}
alerts = find_alerts(df,variables)

with open('alertas.json', 'w') as json_file:
    json.dump(alerts, json_file)
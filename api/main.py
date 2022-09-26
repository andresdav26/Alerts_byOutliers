from http.client import HTTPException
import sys 
sys.path.append(r"/home/adguerrero/Documents/outliers")

from typing import Dict, List
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from pymongo import MongoClient

from alertas_main.utils import *

# get data mongo
client = MongoClient("mongodb://usr_ia:dut_tu6.uioA@sprdbdsmng12.syc.loc:27017,sprdbdsmng13.syc.loc:27017,scdabdsmng01.syc.loc:27017/?authSource=admin&replicaSet=replEbank&w=majority&readPreference=secondary&appname=MongoDB%20Compass&retryWrites=true&ssl=false")
ebk_logs = client.ebk_logs


async def mongo_connect(ebk_L: str, periodo: str): 
    
    collection = ebk_logs[ebk_L]
    document = list(collection.find({'Periodo':periodo},projection={'_id':0}))

    return document


async def alerts_detection(dataMongo: List[Dict]) -> dict:

    df = createDataFrame(dataMongo)
    variables = {'Alerta por reprecesos de estado':'Procesos radicado-estado',
                'Alerta por reprocesos de combinación de estados': 'Procesos radicado - Combinacion de estado',
                'Alerta por dias laborados por estado':'Dias laborados / Estado',
                'Alerta por radicados que duran más que el ANS':'Dias laborados / Radicado'}
    alerts = find_alerts(df,variables)

    return alerts


app = FastAPI(title='Detección de Alertas')

@app.get('/')
def root():
    html_content = """
    <html>
        <meta http-equiv=”Content-Type” content=”text/html; charset=UTF-8″ />
        <head>
            <title>Alertas API</title>
        </head>
        <body>
            <h1>Detección automática de alertas</h1>
            <h2>Servicio de Inteligencia Asistida.</h2>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)


@app.post('/Alertas/')
async def Alertas(ebk_L: str, periodo: str):

    # Mongo data
    data_mongo = await mongo_connect(ebk_L,periodo)
    if data_mongo:

        # alerts detection
        response = await alerts_detection(data_mongo)

        return response
    
    raise HTTPException(404, f"There is no ebk_logs with this name {ebk_L} or the {periodo} does not exist") 


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8080)

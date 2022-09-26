import pandas as pd
import numpy as np
import datetime

def createDataFrame(dataMongo):

    df = pd.DataFrame(dataMongo)
    df = pd.concat([df, pd.DataFrame(df['Metadata'].to_list())], axis=1)
    df.rename(columns = {'Detalles (Cantidad)':'Detalles', 'Folios (Cantidad)':'Folios','Caractares (Cantidad)':'Caracteres'}, inplace = True)
    df['Radicado'] = df['Radicado'].astype(str)
    df['Detalles'] = df['Detalles'].astype(int)
    df['Folios'] = df['Folios'].astype(int)
    df['Caracteres'] = df['Caracteres'].astype(int)
    df['Fecha Inicio / Hora'] = df['Fecha Inicio / Hora'] - datetime.timedelta(hours=5)
    df['Fecha Fin / Hora'] = df['Fecha Fin / Hora'] - datetime.timedelta(hours=5)
    
    # Días laborados por estado y por radicado
    df['Dias laborados / Estado'] = df.apply(lambda row : np.busday_count(row['Fecha Inicio / Hora'].date(),row['Fecha Fin / Hora'].date(),weekmask=[1,1,1,1,1,0,0]),axis=1)
    days_per_rad = df.groupby(by=['Radicado'])['Dias laborados / Estado'].sum() # dias por radicado
    df = pd.merge(left = df,right = pd.DataFrame(days_per_rad).reset_index().rename(columns={'Dias laborados / Estado':'Dias laborados / Radicado'}))
    df = df.drop(columns=(['Periodo', 'Metadata', 'Valor', 'Proyecto','Fecha Inicio / Hora', 'Fecha Fin / Hora']))

    # Cantidad de procesos 
    df_pro_rad_est = df.groupby(by=["Radicado","Estado"]).size().reset_index(name='Procesos radicado-estado')
    df = pd.merge(left=df,right=df_pro_rad_est)

    df['Combinacion estado'] = df['Estado']+'-'+df['Estado Destino']
    df_pro_rad_comb = df.groupby(by=["Radicado","Combinacion estado"]).size().reset_index(name='Procesos radicado - Combinacion de estado')
    df = pd.merge(left=df,right=df_pro_rad_comb)

    return df

# find outliers using IQR
def find_outliers_IQR(df):
   q1 = df.quantile(0.25)
   q3 = df.quantile(0.75)
   IQR = q3 - q1
   # outliers = df[((df<(q1-1.5*IQR)) | (df>(q3+1.5*IQR)))]
   # outliers = ((df<(q1-1.5*IQR)) | (df>(q3+1.5*IQR)))
   outliers = df>(q3+1.5*IQR)
   return outliers, q3+1.5*IQR


def find_alerts(df,variables):
    alertas = {}
    for a, k in enumerate(variables.keys()): # Alert , Alert type
        results = {}
        # 1st Alert
        if a == 0 or a == 2: 
            for type_estado in df["Estado"].unique():
                temp = df[df["Estado"] == type_estado][[variables[str(k)]]].reset_index()
                activacion, th = find_outliers_IQR(temp[variables[str(k)]]) # activación de la alerta (True o False)
                porcentaje = ((activacion == True).sum()/len(activacion))*100
                radicados = df['Radicado'][temp["index"][activacion[activacion == True].index].values].values.tolist()
                if porcentaje > 0:
                    results[type_estado] = {"Umbral": th, 
                                            "Proporcion" : porcentaje,
                                            "Radicados": radicados}
            alertas[str(k)] = results
    
        # 2nd Alert 
        elif a == 1: 
            for type_comb_estado in df["Combinacion estado"].unique():
                temp = df[df["Combinacion estado"] == type_comb_estado][[variables[str(k)]]].reset_index()
                activacion, th = find_outliers_IQR(temp[variables[str(k)]]) # activación de la alerta (True o False)
                porcentaje = ((activacion == True).sum()/len(activacion))*100
                radicados = df['Radicado'][temp["index"][activacion[activacion == True].index].values].values.tolist()
                if porcentaje > 0:
                    results[type_comb_estado] = {"Umbral": th, 
                                                "Proporcion" : porcentaje,
                                                "Radicados": radicados}
            alertas[str(k)] = results
    
        # 3rd Alert
        elif a == 3:
            th = 4
            radicados = df[df[variables[str(k)]] > th]["Radicado"].unique().tolist()
            porcentaje = (len(radicados) / len(df['Radicado'].unique())) *100
            if porcentaje > 0:
                results[type_comb_estado] = {"Umbral": th, 
                                            "Proporcion" : porcentaje,
                                            "Radicados": radicados}
            alertas[str(k)] = results

    return alertas



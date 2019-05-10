import pandas as pd
import numpy as np
import time
from influxdb import InfluxDBClient
import datetime
from random import randrange, uniform
import utm
import reverse_geocoder as rg
import calendar

def time_int_to_str(df):
    try:
        return str(df).zfill(2)
    except:
        print('Algo salio mal en el modulo time_int_to_str')  
        
def create_timestamp(year,month,day,hour):
    try:
        return '%s-%s-%sT%s'%(year,month,day,hour)
    except:
        print('Algo salio mal en el modulo create_timestamp')      
        
def create_date():
    Year = random.randint(2012,2018)
    Month = random.randint(1,12)
    if Month==2:
        Day = random.randint(1,28)
    else: Day = random.randint(1,30)    
    Hour = random.randint(0,23)
    return Year,Month,Day,Hour

def alerta(df):
    df.Alerta_Eficiencia=0
    df.Alerta_Oportunidad=0
    df.Alerta_Tiempo=0
    for i in range(len(df)):
        if df.loc[i,'Eficiencia']<79:
            df.loc[i,'Alerta_Eficiencia']=1
    if df.loc[i,'Oportunidad']<65:
        df.loc[i,'Alerta_Oportunidad']=1
    if df.loc[i,'Tiempo_Medio_Resolucion']>35:
        df.loc[i,'Alerta_Tiempo']=1

#Ingreso
si=pd.read_csv('/home/jovyan/work/Norma_tecnica/Solicitud_ingresada.csv',sep=',')
sr=pd.read_csv('/home/jovyan/work/Norma_tecnica/Solicitud_responidas.csv',sep=',')
#Reclamo
ri=pd.read_csv('/home/jovyan/work/Norma_tecnica/Reclamos_ingresados.csv',sep=',')
rr=pd.read_csv('/home/jovyan/work/Norma_tecnica/Reclamos_respondidos.csv',sep=',')
#Consulta
ci=pd.read_csv('/home/jovyan/work/Norma_tecnica/Consultas_ingresadas.csv',sep=',')
rc=pd.read_csv('/home/jovyan/work/Norma_tecnica/Consultas_responidas.csv',sep=',')

ind=pd.DataFrame(columns=['Empresa','Periodo','N_solicitudes_ingresadas','N_solicitudes_respondidas','N_reclamos_ingresados','N_reclamos_respondidos','N_consultas_ingresadas','N_consultas_respondidas','Total_ingresos','Total_respondidos','Eficiencia','Oportunidad','Tiempo_Medio_Resolucion','Oportunidad_solicitud','Oportunidad_reclamo','Oportunidad_consulta','Tiempo_medio_solicitud','Tiempo_medio_reclamo','Tiempo_medio_consulta','Alerta_Eficiencia','Alerta_Oportunidad','Alerta_Tiempo'])

######Eficiencia#######
#Solicitudes
si2 = si.set_index(pd.DatetimeIndex(si['FECHA_INICIO']))
#Cuenta mensual de Ingresos
SI=si2.FECHA_INICIO.resample('M').count()
sr = sr.set_index(pd.DatetimeIndex(sr['FECHA_RESPUESTA']))
#Cuenta mensual de Respuestas
SR=sr.FECHA_RESPUESTA.resample('M').count()

#Reclamos
ri = ri.set_index(pd.DatetimeIndex(ri['FECHA_INICIO']))
#Cuenta mensual de Ingresos
RI=ri.FECHA_INICIO.resample('M').count()
rr = rr.set_index(pd.DatetimeIndex(rr['FECHA_RESPUESTA']))
RR=rr.FECHA_RESPUESTA.resample('M').count()

#Consulta
ci = ci.set_index(pd.DatetimeIndex(ci['FECHA_INICIO']))
#Cuenta mensual de Ingresos
CI=ci.FECHA_INICIO.resample('M').count()
rc = rc.set_index(pd.DatetimeIndex(rc['FECHA_RESPUESTA']))
#Cuenta mensual de Respuestas
RC=rc.FECHA_RESPUESTA.resample('M').count()

######Oportunidad#######
#Solicitudes
OPsr=sr.RESPONDIDO_EN_PLAZO.resample('M').sum()

#Reclamos
OPrr=rr.RESPONDIDO_EN_PLAZO.resample('M').sum()

#Consulta
OPrc=rc.RESPONDIDO_EN_PLAZO.resample('M').sum()

######Tiempo Medio Respuesta######
#Solicitudes
#sr.DURACION_RESPUESTA=sr.DURACION_RESPUESTA/ pd.Timedelta(1, unit='d')
TMsr=sr.DURACION_RESPUESTA.resample('M').sum()
#Reclamos
#rr.DURACION_RESPUESTA=rr.DURACION_RESPUESTA/ pd.Timedelta(1, unit='d')
TMrr=rr.DURACION_RESPUESTA.resample('M').sum()
#Consulta
#rc.DURACION_RESPUESTA=rc.DURACION_RESPUESTA/ pd.Timedelta(1, unit='d')
TMrc=rc.DURACION_RESPUESTA.resample('M').sum()

mes = pd.date_range(start='2012-01-01 01:00:00' ,end='2017-01-01 01:00:00', freq='MS')
count=0
for i in range(len(mes)):
    ind.loc[i,'Periodo'] =mes[i]
    #Solicitud
    ind.loc[i,'N_solicitudes_ingresadas']=SI[0+count]+SI[1+count]+SI[2+count]+SI[3+count]+SI[4+count]+SI[5+count]+SI[6+count]+SI[7+count]+SI[8+count]+SI[9+count]+SI[10+count]+SI[11+count]+SI[12+count]
    #Respuestas entre 40% y 90%
    ind.loc[i,'N_solicitudes_respondidas']=SR[0+count]*uniform(0.4,0.9)+SR[1+count]*uniform(0.4,0.9)+SR[2+count]*uniform(0.4,0.9)+SR[3+count]+SR[4+count]+SR[5+count]*uniform(0.4,0.9)+SR[6+count]*uniform(0.4,0.9)+SR[7+count]*uniform(0.4,0.9)+SR[8+count]*uniform(0.4,0.9)+SR[9+count]*uniform(0.4,0.9)+SR[10+count]*uniform(0.4,0.9)+SR[11+count]*uniform(0.4,0.9)+SR[12+count]*uniform(0.4,0.9)
    ind.loc[i,'Oportunidad_solicitud']=OPsr[0+count]+OPsr[1+count]+OPsr[2+count]+OPsr[3+count]+OPsr[4+count]+OPsr[5+count]+OPsr[6+count]+OPsr[7+count]+OPsr[8+count]+OPsr[9+count]+OPsr[10+count]+OPsr[11+count]+OPsr[12+count]
    ind.loc[i,'Tiempo_medio_solicitud']=TMsr[0+count]+TMsr[1+count]+TMsr[2+count]+TMsr[3+count]+TMsr[4+count]+TMsr[5+count]+TMsr[6+count]+TMsr[7+count]+TMsr[8+count]+TMsr[9+count]+TMsr[10+count]+TMsr[11+count]+TMsr[12+count]
    
    #Reclamo
    ind.loc[i,'N_reclamos_ingresados']=RI[0+count]+RI[1+count]+RI[2+count]+RI[3+count]+RI[4+count]+RI[5+count]+RI[6+count]+RI[7+count]+RI[8+count]+RI[9+count]+RI[10+count]+RI[11+count]+RI[12+count]
    #Respuestas entre 95% y 100%
    ind.loc[i,'N_reclamos_respondidos']=RR[0+count]*uniform(0.95,1)+RR[1+count]*uniform(0.95,1)+RR[2+count]*uniform(0.95,1)+RR[3+count]+RR[4+count]+RR[5+count]*uniform(0.95,1)+RR[6+count]*uniform(0.95,1)+RR[7+count]*uniform(0.95,1)+RR[8+count]*uniform(0.95,1)+RR[9+count]*uniform(0.95,1)+RR[10+count]*uniform(0.95,1)+RR[11+count]*uniform(0.95,1)+RR[12+count]*uniform(0.95,1)
    ind.loc[i,'Oportunidad_reclamo']=OPrr[0+count]+OPrr[1+count]+OPrr[2+count]+OPrr[3+count]+OPrr[4+count]+OPrr[5+count]+OPrr[6+count]+OPrr[7+count]+OPrr[8+count]+OPrr[9+count]+OPrr[10+count]+OPrr[11+count]+OPrr[12+count]
    ind.loc[i,'Tiempo_medio_reclamo']=TMrr[0+count]+TMrr[1+count]+TMrr[2+count]+TMrr[3+count]+TMrr[4+count]+TMrr[5+count]+TMrr[6+count]+TMrr[7+count]+TMrr[8+count]+TMrr[9+count]+TMrr[10+count]+TMrr[11+count]+TMrr[12+count]
    
    #Consulta
    ind.loc[i,'N_consultas_ingresadas']=CI[0+count]+CI[1+count]+CI[2+count]+CI[3+count]+CI[4+count]+CI[5+count]+CI[6+count]+CI[7+count]+CI[8+count]+CI[9+count]+CI[10+count]+CI[11+count]+CI[12+count]
    #Respuestas entre 50% y 100%
    ind.loc[i,'N_consultas_respondidas']=CI[0+count]*uniform(0.5,1)+CI[1+count]*uniform(0.5,1)+CI[2+count]*uniform(0.5,1)+CI[3+count]+CI[4+count]+CI[5+count]*uniform(0.5,1)+CI[6+count]*uniform(0.5,1)+CI[7+count]*uniform(0.5,1)+CI[8+count]*uniform(0.5,1)+CI[9+count]*uniform(0.5,1)+CI[10+count]*uniform(0.5,1)+CI[11+count]*uniform(0.5,1)+CI[12+count]*uniform(0.5,1)
    ind.loc[i,'Oportunidad_consulta']=OPrc[0+count]+OPrc[1+count]+OPrc[2+count]+OPrc[3+count]+OPrc[4+count]+OPrc[5+count]+OPrc[6+count]+OPrc[7+count]+OPrc[8+count]+OPrc[9+count]+OPrc[10+count]+OPrc[11+count]+OPrc[12+count]
    ind.loc[i,'Tiempo_medio_consulta']=TMrc[0+count]+TMrc[1+count]+TMrc[2+count]+TMrc[3+count]+TMrc[4+count]+TMrc[5+count]+TMrc[6+count]+TMrc[7+count]+TMrc[8+count]+TMrc[9+count]+TMrc[10+count]+TMrc[11+count]+TMrc[12+count]
    
    count+=1
    
    
ind['Total_ingresos']=ind.N_solicitudes_ingresadas+ind.N_reclamos_ingresados+ind.N_consultas_ingresadas
ind['Total_respondidos']=ind.N_solicitudes_respondidas+ind.N_reclamos_respondidos+ind.N_consultas_respondidas
ind['Eficiencia']=ind.Total_respondidos/ind.Total_ingresos*100
ind['Oportunidad']=(ind.Oportunidad_solicitud+ind.Oportunidad_reclamo+ind.Oportunidad_consulta)/ind.Total_respondidos*100
ind['Tiempo_Medio_Resolucion']=(ind.Tiempo_medio_solicitud+ind.Tiempo_medio_reclamo+ind.Tiempo_medio_consulta)/ind.Total_respondidos


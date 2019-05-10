import pandas as pd
import numpy as np
import time
from influxdb import InfluxDBClient
from datetime import datetime
import random
import utm
import reverse_geocoder as rg
import calendar

def update_time2(Year,Month,Day,Hour):
    try:
        Hour = Hour + 1
        if(Hour > 23):
            Hour = 0
            Day = Day + 1
            if(Day > calendar.monthrange(Year,Month)[1]):
                Day = 1
                Month = Month + 1
                if(Month > 12):
                    Month = 1
                    Year = Year + 1 
        return (Year,Month,Day,Hour)
    except:
        return (Year,Month,Day,Hour)
    
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

def respondido_en_plazo(duracion,i):
    if duracion[i].days > 30:
        return 0
    else:
        return 1

pc1=pd.read_csv('/home/jovyan/work/Norma_tecnica/coordenadas.csv',sep=',')

############Tabla Clientes############

clientes=pd.DataFrame(columns=['CLIENTE_ID','COORDENADAS'])
Cliente_ID=[]
coord=[]
coord2=[]
coordenadas=pc1['coordenadas']
for i in range(0,2000):
    Cliente_ID.append(i+1)
    coord.append(random.choice(coordenadas))
    coord2.append(eval(coord[i]))
    
results = rg.search(coord2)
df1=pd.DataFrame.from_dict(results)    
clientes=pd.concat([df1, clientes], axis=1)
    
clientes.CLIENTE_ID= Cliente_ID
clientes.COORDENADAS= coord
clientes=clientes.loc[:,['name','admin2','CLIENTE_ID','COORDENADAS']]
clientes.rename(columns={'name': 'Ciudad','admin2':'Provincia'}, inplace=True)

############Solicitud Ingresada##############

clientes=pd.read_csv('/home/jovyan/work/Norma_tecnica/Clientes.csv',sep=',')
si=pd.DataFrame(columns=['SOLICITUD_ID','NOMBRE_EMPRESA', 'CLIENTE_ID','MEDIO_ATENCION','FECHA_INICIO','TIPO_SOLICITUD'])
med_atenc=['Oficina de atención a clientes','Fono cliente','Correo electronico','Carta escrita','Pagina Web','Pagina Web','Correo electronico','Pagina Web']
tipo_soli=['Modificación de datos comerciales','Modificación de datos comerciales','Modificación de empalme','Modificación de potencia','Problemas con Pagina WEB','Reposición o instalación de sello','Robo de equipos (empalme/medidor/transformador)','Servicio de telemedida','Solicitud de Pago de saldo a Favor','Solicitud de poda en via pública','Solicitud de poda en via pública','Solicitud de poda en via pública','Verificación de lectura','Verificación de lectura','Verificación de lectura','Verificación de lectura','Verificación de lectura']
i=0
Empresa='Empresa de Prueba'
Tipo_solicitud=[]
Medio_atencion=[]
Cliente_ID=[]
clientes_id=clientes['CLIENTE_ID'].tolist()
Solicitud_ID=[]
coord=[]
coordenadas=pc1['coordenadas']
#Fecha Base
fecha=[]
Year = 2012
Month = 1
Day = 1
Hour = 0

dYear=[]
dMonth=[]
dDay=[]
dHour=[]

while (i<100000):
    Tipo_solicitud.append(random.choice(tipo_soli))
    Medio_atencion.append(random.choice(med_atenc))
    Cliente_ID.append(random.choice(clientes_id))
    Solicitud_ID.append(i+1)
    
    (Year,Month,Day,Hour) = update_time2(Year,Month,Day,Hour)
    dYear.append(Year)
    dMonth.append(Month)
    dDay.append(Day)
    dHour.append(Hour)
    #coord.append(clientes.loc[clientes['CLIENTE_ID']==Cliente_ID[i]].COORDENADAS)
    fecha.append(create_timestamp(Year,time_int_to_str(Month),time_int_to_str(Day),time_int_to_str(Hour)))
    i+=1
    
    if Year>2018:
        Year=2012   
    
si.SOLICITUD_ID= Solicitud_ID
si.NOMBRE_EMPRESA=Empresa
si.CLIENTE_ID=Cliente_ID
si.MEDIO_ATENCION=Medio_atencion
si.FECHA_INICIO=fecha
si.TIPO_SOLICITUD=Tipo_solicitud
#si.COORDENADAS=coord    

si.FECHA_INICIO=pd.to_datetime(si.FECHA_INICIO , format='%Y-%m-%d')


############Solicitud Respuesta##############

sr=pd.DataFrame(columns=['SOLICITUD_ID','FECHA_RESPUESTA', 'DURACION_RESPUESTA', 'RESPONDIDO_EN_PLAZO','PLAZO_MAXIMO_RESPUESTA'])
Plazo_maximo=30
fecha=[]
duracion=[]
respondido_plazo=[]
i=0
Solicitud_ID=[]

while (i<len(si)):
    Solicitud_ID.append(si.SOLICITUD_ID[i])
    #Respuesta entre 20 y 40 días
    fecha.append(si.FECHA_INICIO[i]+pd.DateOffset(days=np.random.randint(20,40)))
    duracion.append(fecha[i]-si.FECHA_INICIO[i])
    respondido_plazo.append(respondido_en_plazo(duracion,i))
    i+=1
    
sr.SOLICITUD_ID=Solicitud_ID
sr.FECHA_RESPUESTA=fecha
sr.DURACION_RESPUESTA=duracion
sr.RESPONDIDO_EN_PLAZO=respondido_plazo
sr.PLAZO_MAXIMO_RESPUESTA=Plazo_maximo

sr.FECHA_RESPUESTA=pd.to_datetime(sr.FECHA_RESPUESTA , format='%Y-%m-%d')


############Reclamos Ingresados##############

clientes=pd.read_csv('/home/jovyan/work/Norma_tecnica/Clientes_calidad_comercial.csv',sep=',')
ri=pd.DataFrame(columns=['RECLAMO_ID','NOMBRE_EMPRESA', 'CLIENTE_ID','MEDIO_ATENCION','FECHA_INICIO','TIPO_RECLAMO'])
med_atenc=['Oficina de atención a clientes','Fono cliente','Correo electronico','Carta escrita','Pagina Web','Pagina Web','Correo electronico','Pagina Web']
tipo_recl=['Alumbrado público','Artefacto Dañado','Atención comercial Call Center','Atención comercial en terreno','Atención comercial Oficinas Comerciales','Atención comercial páginas web','Boleta/Factura no recepcionada','Boleta entregada fuera de plazo','Cambio de medidor','Cobro atención de emergencia en empalme domiciliario','Cobro atención de emergencia en la red','Cobro de corte y reposición no efectuado','Cobro Excesivo','Cobro proyectos y obras','Cobro verificación de medidor','Cobros por interés por mora','Cobros por reliquidación','Consumo No Registrado','Corte no efectuado en terreno','Corte programado excede tiempo informado','Cortes reiterados','Daños en Mercadería','Deuda No Radicada','Empalme/medidor en mal estado','Inconformidad de suministro','Inconsistencia en toma de lectura','Instalaciones, ubicación de equipos eléctricos','Lectura no tomada','Llega boleta de otro domicilio','No cumple plazo legal de reconexión','No informan corte programado','No se cumplen los plazos de entrega de presupuesto','Paga boleta antes del corte','Pago erróneo','Pago no registrado','Poste Mal Ubicado','Problemas con datos en boleta o impresión','Problemas con PAC-PAT','Variaciones Voltaje']
i=0
Empresa='Empresa de Prueba'
Tipo_reclamo=[]
Medio_atencion=[]
Cliente_ID=[]
clientes_id=clientes['CLIENTE_ID'].tolist()
Reclamo_ID=[]
coord=[]
coordenadas=pc1['coordenadas']
#Fecha Base
fecha=[]
Year = 2012
Month = 1
Day = 1
Hour = 0

dYear=[]
dMonth=[]
dDay=[]
dHour=[]

while (i<100000):
    Tipo_reclamo.append(random.choice(tipo_recl))
    Medio_atencion.append(random.choice(med_atenc))
    Cliente_ID.append(random.choice(clientes_id))
    Reclamo_ID.append(i+1)
    
    (Year,Month,Day,Hour) = update_time2(Year,Month,Day,Hour)
    dYear.append(Year)
    dMonth.append(Month)
    dDay.append(Day)
    dHour.append(Hour)
    #coord.append(clientes.loc[clientes['CLIENTE_ID']==Cliente_ID[i]].COORDENADAS)
    fecha.append(create_timestamp(Year,time_int_to_str(Month),time_int_to_str(Day),time_int_to_str(Hour)))
    i+=1
    
    if Year>2018:
        Year=2012   
    
ri.RECLAMO_ID= Reclamo_ID
ri.NOMBRE_EMPRESA=Empresa
ri.CLIENTE_ID=Cliente_ID
ri.MEDIO_ATENCION=Medio_atencion
ri.FECHA_INICIO=fecha
ri.TIPO_RECLAMO=Tipo_reclamo
#ri.COORDENADAS=coord    

ri.FECHA_INICIO=pd.to_datetime(ri.FECHA_INICIO , format='%Y-%m-%d')


############Respuesta Reclamos##############

rr=pd.DataFrame(columns=['RECLAMO_ID','FECHA_RESPUESTA', 'DURACION_RESPUESTA', 'RESPONDIDO_EN_PLAZO','PLAZO_MAXIMO_RESPUESTA'])
Plazo_maximo=30
fecha=[]
duracion=[]
respondido_plazo=[]
i=0
Reclamo_ID=[]

while (i<len(ri)):
    Reclamo_ID.append(ri.RECLAMO_ID[i])
    #Respuesta entre 20 y 40 días
    fecha.append(ri.FECHA_INICIO[i]+pd.DateOffset(days=np.random.randint(20,40)))
    duracion.append(fecha[i]-ri.FECHA_INICIO[i])
    respondido_plazo.append(respondido_en_plazo(duracion,i))
    i+=1
    
rr.RECLAMO_ID=Reclamo_ID
rr.FECHA_RESPUESTA=fecha
rr.DURACION_RESPUESTA=duracion
rr.RESPONDIDO_EN_PLAZO=respondido_plazo
rr.PLAZO_MAXIMO_RESPUESTA=Plazo_maximo

rr.FECHA_RESPUESTA=pd.to_datetime(rr.FECHA_RESPUESTA , format='%Y-%m-%d')


############Consultas Ingresadas##############

clientes=pd.read_csv('/home/jovyan/work/Norma_tecnica/Clientes_calidad_comercial.csv',sep=',')
ci=pd.DataFrame(columns=['CONSULTAS_ID','NOMBRE_EMPRESA', 'CLIENTE_ID','MEDIO_ATENCION','FECHA_INICIO','TIPO_RECLAMO'])
med_atenc=['Oficina de atención a clientes','Fono cliente','Correo electronico','Carta escrita','Pagina Web','Pagina Web','Correo electronico','Pagina Web']
tipo_cons=[]
i=0
Empresa='Empresa de Prueba'
Tipo_consulta=[]
Medio_atencion=[]
Cliente_ID=[]
clientes_id=clientes['CLIENTE_ID'].tolist()
Consulta_ID=[]
coord=[]
coordenadas=pc1['coordenadas']
#Fecha Base
fecha=[]
Year = 2012
Month = 1
Day = 1
Hour = 0

dYear=[]
dMonth=[]
dDay=[]
dHour=[]

while (i<100000):
    Tipo_consulta.append(random.choice(tipo_cons))
    Medio_atencion.append(random.choice(med_atenc))
    Cliente_ID.append(random.choice(clientes_id))
    Reclamo_ID.append(i+1)
    
    (Year,Month,Day,Hour) = update_time2(Year,Month,Day,Hour)
    dYear.append(Year)
    dMonth.append(Month)
    dDay.append(Day)
    dHour.append(Hour)
    #coord.append(clientes.loc[clientes['CLIENTE_ID']==Cliente_ID[i]].COORDENADAS)
    fecha.append(create_timestamp(Year,time_int_to_str(Month),time_int_to_str(Day),time_int_to_str(Hour)))
    i+=1
    
    if Year>2018:
        Year=2012   
    
ci.CONSULTAS_ID= Consulta_ID
ci.NOMBRE_EMPRESA=Empresa
ci.CLIENTE_ID=Cliente_ID
ci.MEDIO_ATENCION=Medio_atencion
ci.FECHA_INICIO=fecha
ci.TIPO_CONSULTA=Tipo_consulta
#ci.COORDENADAS=coord    

ci.FECHA_INICIO=pd.to_datetime(ci.FECHA_INICIO , format='%Y-%m-%d')


############Respuesta Consulta##############

rc=pd.DataFrame(columns=['CONSULTAS_ID','FECHA_RESPUESTA', 'DURACION_RESPUESTA', 'RESPONDIDO_EN_PLAZO','PLAZO_MAXIMO_RESPUESTA'])
Plazo_maximo=30
fecha=[]
duracion=[]
respondido_plazo=[]
i=0
Consulta_ID=[]

while (i<len(ri)):
    Reclamo_ID.append(ci.RECLAMO_ID[i])
    #Respuesta entre 20 y 40 días
    fecha.append(ci.FECHA_INICIO[i]+pd.DateOffset(days=np.random.randint(20,40)))
    duracion.append(fecha[i]-ci.FECHA_INICIO[i])
    respondido_plazo.append(respondido_en_plazo(duracion,i))
    i+=1
    
rc.RECLAMO_ID=Reclamo_ID
rc.FECHA_RESPUESTA=fecha
rc.DURACION_RESPUESTA=duracion
rc.RESPONDIDO_EN_PLAZO=respondido_plazo
rc.PLAZO_MAXIMO_RESPUESTA=Plazo_maximo

rc.FECHA_RESPUESTA=pd.to_datetime(rc.FECHA_RESPUESTA , format='%Y-%m-%d')


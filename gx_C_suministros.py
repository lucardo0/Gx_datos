import pandas as pd
import numpy as np
import time
from influxdb import InfluxDBClient
import datetime
from random import randrange, uniform
import random
import utm
import reverse_geocoder as rg
import calendar
from random import sample

def new_date(intervalo,fecha):
    try:
        nfecha = fecha + datetime.timedelta(days = intervalo)
        return nfecha
    except:
        print('No se pudo actualizar la fecha en el modulo new_date')                
        
def intervalo_fechas(fecha):
    try:
        nfecha = fecha + datetime.timedelta(minutes = np.random.randint(50,80))
        return nfecha
    except:
        print('No se pudo actualizar la fecha en el modulo new_date')         
        
def date_to_iso(fecha):
    try:
        fecha = fecha.isoformat()
        return fecha
    except:
        print('No se pudo dejar la fecha en formato ISO en el modulo date_to_iso')      
        
def guardar_lecturas(alimentador_id,fecha_inicio,interrupcion_id,fecha_termino,duracion,clientes_afectados):
    try:
        json_body = {
                    'measurement': 'clientes_interrupciones',
                    
                    'tags': 
                    {
                        'cliente_id': cliente_id,                    
                        'interrupcion_id': interrupcion_id,
                        'causa':causa,
                        'origen':origen,
                        'fecha_termino':fecha_termino,
                        'duracion':duracion
                    },
                    
                    'time':fecha_inicio,
                    
                    'fields': 
                    {
                        'latitud_alimentador':latitud_alimentador,
                        'longitud_alimentador':longitud_alimentador
       
                    }
                }    
        return json_body
    except:
        print('No se pudo escribir el diccionario correctamente en el modulo guardar_lecturas') 

clientes=pd.read_csv('/home/jovyan/work/Norma_tecnica/Clientes_calidad_comercial.csv', sep=',', encoding='utf-8')

#Separar las coordinadas
new = clientes["COORDENADAS"].str.split(",", n = 1, expand = True)

#Eliminar par�ntesis
new.iloc[:,0] = new.iloc[:,0].map(lambda x: x.lstrip('('))
new.iloc[:,1] = new.iloc[:,1].astype(str).str[:-1]

#Transfomar a float
new.iloc[:,0] = pd.to_numeric(new.iloc[:,0])
new.iloc[:,1] = pd.to_numeric(new.iloc[:,1])

#Reemplazar valores en df

clientes["Latitud"]= new[0] 
clientes["Longitud"]= new[1] 
clientes=clientes.drop(['COORDENADAS'], axis=1)

#Tabla Alimentadores con sus respectivos clientes
alimentador = pd.DataFrame(columns=['alimentador_id','clientes'] )
#1° alimentador, con 600 clientes
alimentador.loc[0,'clientes'] = clientes.iloc[:600,3].to_list()
alimentador.loc[0,'alimentador_id'] = 0
#2° alimentador, con 300 clientes
alimentador.loc[1,'clientes'] = clientes.iloc[601:900,3].to_list()
alimentador.loc[1,'alimentador_id'] = 1
#3° alimentador, con 400 clientes
alimentador.loc[2,'clientes'] = clientes.iloc[901:1300,3].to_list()
alimentador.loc[2,'alimentador_id'] = 2
#4° alimentador, con 150 clientes
alimentador.loc[3,'clientes'] = clientes.iloc[1301:1450,3].to_list()
alimentador.loc[3,'alimentador_id'] = 3
#5° alimentador, con 350 clientes
alimentador.loc[4,'clientes'] = clientes.iloc[1451:1800,3].to_list()
alimentador.loc[4,'alimentador_id'] = 4
#6° alimentador, con 200 clientes
alimentador.loc[5,'clientes'] = clientes.iloc[1801:2000,3].to_list()
alimentador.loc[5,'alimentador_id'] = 5

#Borrar los datos anteriores
client.drop_measurement("saifi_y_saidi")
client.drop_measurement("fic_y_tic")

detalle_cliente=pd.DataFrame(columns=['interrupcion_id','alimentador_id','CLIENTE_ID'])
df_int=pd.DataFrame(columns=['interrupcion_id','alimentador_id','fecha_inicio','fecha_termino','duracion','clientes_afectados'])
interrupcion_id=0
alimentador_id=0
tipo_origen=['Instalaciones =< 23 KV de propiedad de la concesionaria','Instalaciones > 23 KV de propiedad de la concesionaria','Instalaciones =< 23 KV de propiedad de clientes de la concesionaria','Instalaciones > 23 KV de propiedad de clientes de la concesionaria','Instalaciones =< 23 KV de propiedad de terceros (no clientes de la concesionaria)','Instalaciones > 23 KV de propiedad de terceros (no clientes de la concesionaria)']
causas=['Externas','Condiciones Atmosféricas','Eventos de la Naturaleza','Incendio no debido a fallas','Animales','Juegos personas','Accidentes','Actos vandálicos','Árboles','Por Vehículos','Instalaciones de clientes','Corte y reposición','Operación de la red','Sobrecarga','Mantenimiento','Construcción y equipos','Auto producidos','Otros no clasificados']

stream_json = {}
stream_json['Indicadores'] = []
stream_json['SAIFI_SAIDI'] = []
stream_json['interrupciones'] = []

Year = 2017
Month = 1
Day = 1
Hour = 3
Minute = 0
fecha = datetime.datetime(Year,Month,Day,Hour,Minute)
FI = datetime.datetime(Year,Month,Day,Hour,Minute)

while True:
    #Se escoge aleatoriamente un alimentador
    alimentador_id = alimentador['alimentador_id'].sample(n=1)
    #Generamos la nueva fecha
    tiempo = np.random.randint(0,8) #días
    fecha = new_date(tiempo,fecha) 
    #Pasamos la fecha lectura a formato ISO
    fecha_inicio = date_to_iso(fecha)
    fecha_inicio = pd.to_datetime(fecha_inicio , format='%Y-%m-%d') 
    FI = date_to_iso(FI)
    FI = pd.to_datetime(FI , format='%Y-%m-%d')
    fecha_termino = intervalo_fechas(fecha_inicio)
    duracion = fecha_termino - fecha_inicio
    causa = random.choice(causas)
    origen = random.choice(tipo_origen)    
    
    cliente_en_alimentador = alimentador.clientes[alimentador_id.iloc[0]]
    largo = len(alimentador.clientes[alimentador_id.iloc[0]])
    #Lista con una muestra de los clientes en el Alimentador actual
    cliente_id = sample(cliente_en_alimentador,np.random.randint(largo*0.4,largo))
    
    #Se recorre la lista de CLIENTES que hay en el ALIMENTADOR correspondiente
    for i in range(len(cliente_id)):                                                             #Todos los Clientes del Alimentador correpondiente
        df = pd.DataFrame({'interrupcion_id':[interrupcion_id],'alimentador_id':[alimentador_id.iloc[0]],'CLIENTE_ID':[cliente_id[i]]})
        detalle_cliente = detalle_cliente.append(df)
    
    #Se cuentan los clientes afectados por la interrupción actual
    clientes_afectados = detalle_cliente.loc[detalle_cliente.interrupcion_id==interrupcion_id].count()
    
    #Se rellena la tabla de Interrupciones
    df1 = pd.DataFrame({'interrupcion_id':[interrupcion_id],'alimentador_id':[alimentador_id.iloc[0]],'fecha_inicio':[fecha_inicio],'fecha_termino':[fecha_termino],'duracion':[duracion],'clientes_afectados':[clientes_afectados.iloc[0]],'causa':[causa],'origen':origen})
    df_int = df_int.append(df1)
    interrupcion_id+=1
    
    #Se rellena la tabla de Clientes Afectados con sus geolocalizaciones
    todo_clientes = pd.merge(detalle_cliente, clientes, on='CLIENTE_ID', how='left')
    todo_clientes = todo_clientes.drop(columns=['Unnamed: 0'])
    #Unión con las fechas de las interrupciones
    todo_clientes = pd.merge(todo_clientes, df_int, on='interrupcion_id', how='left')
    todo_clientes = todo_clientes.drop(columns=['alimentador_id_y'])
    todo_clientes = todo_clientes.rename(index=str, columns={"alimentador_id_x": "alimentador_id"})
    #Transformar la duración a número, en horas
    todo_clientes['duracion'] = todo_clientes.duracion.dt.seconds/(60*60)
    provincias = todo_clientes.Provincia.unique()
    
    print(df_int.fecha_inicio.iloc[-1] - FI)
    
    ###########INDICADORES############
    
    #Año móvil
    if (df_int.fecha_inicio.iloc[-1] - FI).days>365:
        print('FIC y TIC')
        
        ############ FIC y TIC ############
        
        #Se recorre la lista de clientes
        for i in range(0,len(todo_clientes.loc[(todo_clientes.fecha_inicio<df_int.fecha_inicio.iloc[-1])].groupby(['CLIENTE_ID']))):
            #Cuenta de interrupciones en un año, por cliente
            FICc = todo_clientes.loc[(todo_clientes.fecha_inicio<df_int.fecha_inicio.iloc[-1])].groupby(['CLIENTE_ID']).count().iloc[i,0]
            #Duración de las interrupciones en un año, por cliente
            TICc = todo_clientes.loc[(todo_clientes.fecha_inicio<df_int.fecha_inicio.iloc[-1])].groupby(['CLIENTE_ID']).sum().iloc[i,2]            
            
            id_cliente = i
        
        
            #Escribimos el json
            indc = guardar_indicadores(FICc,TICc,fecha_inicio,id_cliente,interrupcion_id)
            stream_json['Indicadores'].append(indc) 
        
        print('SAIFI y SAIDI')
        
        ############ SAIFI y SAIDI ############
        
        #Se recorren las provincias
        for i in (provincias):
            #Nominador SAIFI
            nsf=todo_clientes.loc[(todo_clientes.fecha_inicio<(todo_clientes.fecha_inicio + pd.Timedelta('365 days'))) & (todo_clientes.Provincia == i)].groupby(['Provincia']).count().iloc[0]
            #Denominador SAIFI y SAIDI
            d=clientes.loc[clientes.Provincia==i].groupby(['Provincia']).count().iloc[0]
            #Nominador SAIDI
            nsd=todo_clientes.loc[(todo_clientes.fecha_inicio<(todo_clientes.fecha_inicio + pd.Timedelta('365 days'))) & (todo_clientes.Provincia == i)].loc[:,'duracion'].sum()
            SAIFI = nsf/d
            SAIDI = nsd/d
            provincia = i
            
            #Escribimos el json
            sfd = guardar_indicadores2(SAIFI[0],SAIDI[0],fecha_inicio,provincia)
            stream_json['SAIFI_SAIDI'].append(sfd)             
        
        print('Escribiendo los datos a InfluxDB')
        todo_clientes.fecha_inicio += pd.Timedelta('30 days')
        FI += pd.Timedelta('30 days')
        
        #Escribimos los datos en InfluxDB
        client.write_points(stream_json['Indicadores']) 
        client.write_points(stream_json['SAIFI_SAIDI'])

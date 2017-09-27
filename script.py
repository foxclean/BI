#------------- Inician Imports. -------------#
import re
import sys
import time
import random
import datetime
from datetime import timedelta
import calendar
from calendar import monthrange
import os
import pymssql
import _mssql
from decimal import *
import statistics as stats
#------------- Finalizan Imports. -------------#

#------------- Inicia Declaración de Variables Globales. -------------#
#------------- Inicia Configuración de BD. -------------#
#--- Variables de conexión a la base de datos.
connection = pymssql.connect(server='66.232.22.196',
                            user='FOXCLEA_TAREAS',
                            password='JACINTO2014',
                            database='FOXCLEA_TAREAS'
                            #charset='utf8mb4',
                            #cursorclass=pymssql.cursors.DictCursor
                           )
#---
#------------- Finaliza Configuración de BD. -------------#

#------------- Inicio de las funciones Generales -------------#
#---

#---
#------------- Fin de las funciones Generales -------------#

#------------- Inicio de declaración de variables -------------#
#---
SETTING = []
PROPIEDADES = []
CONSULTA = []
COMPETENCIA_DIRECTA = []
today = (datetime.datetime.now()).date() #<--- Fecha de hoy.
#---
#------------- Fin de declaración de variables -------------#

#-- La funion que une anunciantes con consulta, tiene que recibir dun parametro, el cual sera una fecha, esa fecha tiene que ser igual o mayor a la fecha de entrada, y igual o menor a la fehca de salida (es decir puede estar entre medio)
#-- Funcion para listar los anuncios de la empresa:
#-- Funcion para listar la competencia directa:
#-- 
#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
try:
    #---
    with connection.cursor() as cursor:
        #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
        sql = "SELECT ID_PORTAL, NOMBRE, URL, DIAS_VERIFICACION, MAX_DAYS, BEGIN_DAY, PRICE_MIN, PRICE_MAX, NEAR_DAYS FROM SCR_PORTALES WHERE CALC_PRICE = 1"        
        cursor.execute(sql)
        SETTING = cursor.fetchall() #<--- Lista con los portales activos.
        #---
        #print(PORTAL)
        print('Correcto -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
print('---------')
print(SETTING)
print('---------')
#---
try:
    #---
    with connection.cursor() as cursor:
        #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
        sql = "SELECT p.ID_PROPIEDAD,pa.ID_ANUNCIO,a.ID_PORTAL,p.[ID_PROPIETARIO],p.[NOMBRE_PROPIEDAD],p.[GRUPO_ID],p.[IDHAB],h.[NOMBRE],h.[numpersonas],a.TITULO,p.[DORMITORIOS],p.[BAÑOS],p.[TELEFONO],p.[MOVIL],p.[DIRECCION],p.[codigo_postal],p.[poblacion],p.[PROVINCIA],p.[LATITUD],p.[LONGITUD],p.[PAIS] FROM [foxclea_tareas].[foxclea_tareas].[AV_PROPIEDADES] p join av_habitacion h on p.[IDHAB] = h.[IDHAB] JOIN SCR_PROPIEDADES_ANUNCIOS pa on p.ID_PROPIEDAD = pa.ID_PROPIEDAD JOIN SCR_ANUNCIOS a on pa.ID_ANUNCIO = a.ID_ANUNCIO;"        
        cursor.execute(sql)
        PROPIEDADES = cursor.fetchall() #<--- Lista con los portales activos.
        #---
        #print(PORTAL)
        print('Correcto -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#print(PROPIEDADES)
#---
def get_direct_comp(id_anuncio):
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
            sql = "SELECT ID_ANUNCIO, ID_COMPETENCIA, NIVEL_COMPETITIVO FROM SCR_COMPETENCIA_DIRECTA WHERE ID_ANUNCIO = %s;"        
            cursor.execute(sql, id_anuncio)
            return cursor.fetchall() #<--- Lista con los portales activos.
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos del "portal" a usar.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#print(COMPETENCIA_DIRECTA)
#---
try:
    #---
    with connection.cursor() as cursor:
        #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
        sql = "SELECT [ID_CONSULTA],[ID_ANUNCIO],[ID_PORTAL],[PAIS],[CIUDAD],[ZONA],[ADULTOS],[NIÑOS],[BEBES],[ESTADO] FROM [foxclea_tareas].[SCR_CONSULTAS]"
        cursor.execute(sql)
        CONSULTA = cursor.fetchall() #<--- Lista con los portales activos.
        #---
        #print(PORTAL)
        print('Correcto -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#print(CONSULTA)
#---
def get_extract_dates(day,portal,consulta,tipo,max_price, min_price):
#---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
            sql = "SELECT [ID_ANUNCIANTE],[ID_COMPETENCIA],[ID_ANUNCIO],[FECHAI],[FECHAF],[ORDEN],[ID_PORTAL],[PRECIO],[ID_CONSULTA],[TIPO],[N_CAMA],[RATIO],[FECHA_INGRESO] FROM [foxclea_tareas].[foxclea_tareas].[SCR_ANUNCIANTES] WHERE foxclea_tareas.solo_fecha(%s) between [FECHAI] and [FECHAF]-1 AND ID_PORTAL = %s AND ID_CONSULTA = %s  AND PRECIO < %s ORDER BY [FECHAI] ASC"
            cursor.execute(sql,(day,portal,consulta,max_price))
            return cursor.fetchall() #<--- Lista con los portales activos.
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos del "portal" a usar.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#print(CONSULTA)
#---
def get_reservation_dates(day):
    try:
    #---
        with connection.cursor() as cursor:
            sql = "SELECT R.ID_RESERVA, R.GRUPO_ID,R.ID_HAB,R.FECHA_RESERVA, R.FECHA_ENTRADA, R.FECHA_SALIDA, R.NO_PERSONAS, R.NO_NIÑOS, R.PRECIO, R.PROCEDENCIA,  R.ESTADO, R.PRECIOextra, R.COMISION, R.GASTOENERGIABAS, R.GASTOLIMREAL , S.NOMBRE,P.NOMBRE_PROPIEDAD,P.ID_PROPIEDAD,R.ID_PROPIEDAD FROM AV_RESERVAS R LEFT JOIN BMSUBCON S ON NUMERO = R.AGENCIA LEFT JOIN  AV_PROPIEDADES P on  P.ID_PROPIEDAD = R.ID_PROPIEDAD WHERE foxclea_tareas.solo_fecha('"+ day +"') between R.fecha_entrada and R.fecha_salida-1 and R.ESTADO<>'CANCELADA' ORDER BY R.FECHA_ENTRADA ASC;"
            print(sql)
            cursor.execute(sql)
            return cursor.fetchall() #<--- Lista con los portales activos.
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---

#---
def get_last_calc(id, dia, mes, año):
    try:
    #---
        with connection.cursor() as cursor:
            sql = "SELECT ID FROM SCR_CALENDARIO_ANALISIS WHERE ID_ANUNCIO = %s AND DIA = %s AND MES = %s AND AÑO = %s"            
            cursor.execute(sql,(id, dia, mes, año))
            temp = cursor.fetchone() #<--- Lista con los portales activos.
            if temp:
                return temp[0]
            else:
                return None
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
def get_calendar(id):
    try:
    #---
        with connection.cursor() as cursor:
            sql = "SELECT * FROM SCR_CALENDARIO_ANALISIS WHERE ID_ANUNCIO = %s ORDER BY [ID] DESC"            
            cursor.execute(sql,id)
            return cursor.fetchall() #<--- Lista con los portales activos.
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
def get_last_regis(id_port):
    #---
    try:
        #---
        last_result = [] #<--- Lista donde se almacena el ultimo resulta de SCR_PORTALES_DETALLES, del portal especificado.
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "SELECT TOP 1 * FROM SCR_PORTALES_DETALLE WHERE ID_PORTAL = %s ORDER BY [ID] DESC "
            cursor.execute(sql, id_port)
            last_result = cursor.fetchone()
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos del "portal" a usar.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---   
    return last_result

    #---
#--- Se genera una lista con los datos de un campo especifico.
def get_one_field_data(data, index):
    temp_resul = []
    for t_data in data:
        if (t_data[index] == None):
            temp_resul.append(0)
        else:
            temp_resul.append(t_data[index])
    #---
    return temp_resul #<--- Devuelve una lista con todos los datos de un campo en especifico.
#---
#--- Suma Meses a una Fecha
def add_days(sourcedate,days):
    #---
    modified_date = sourcedate + timedelta(days=days)
    #---
    return modified_date #<--- Devuelve una fecha.
#---
#--- Resta de meses
def monthdelta(d1, d2):
    delta = 0
    while True:
        mdays = monthrange(d1.year, d1.month)[1]
        d1 += timedelta(days=mdays)
        if d1 <= d2:
            delta += 1
        else:
            break
    return delta  #<--- Devuelve la la diferencia entre dos meses.
#---
def get_black_list(id_hab,dia,mes,año):
    #---
    try:
        #---
        last_result = [] #<--- Lista donde se almacena el ultimo resulta de SCR_PORTALES_DETALLES, del portal especificado.
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "SELECT PRECIO FROM SCR_CALENDAR_DEFAULT_LIST WHERE ID_ANUNCIO = %s AND DIA = %s AND MES = %s AND AÑO = %s"
            cursor.execute(sql, (id_hab, dia, mes, año))
            last_result = cursor.fetchone()
             #---
            if last_result:
                return last_result[0]
            else:
                return None
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos del "portal" a usar.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---
#---

print('today: ',today)

#reserv_day = get_reservation_dates('2017-09-15')
#print(reserv_day)
def insert_price(t_min,t_max,t_prom,propiedad,date,cd_min,cd_max,cd_prom,cd_total,cd_disp):
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "INSERT INTO SCR_CALENDARIO_ANALISIS (ID_ANUNCIO,DIA,MES,AÑO,CD_PRECIO_MEDIO,CD_PRECIO_MIN,CD_PRECIO_MAX,DISPONIBLES,TOTAL,CT_PRECIO_MEDIO,CT_PRECIO_MIN,CT_PRECIO_MAX) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, (propiedad, date.day, date.month, date.year, cd_prom, cd_min, cd_max, cd_disp, cd_total, t_prom, t_min, t_max))

            #---
            print('Correcto #5 -> Registro Correcto del Log.')
        connection.commit()
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error #5 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)

def update_price(reg_id,t_min,t_max,t_prom,propiedad,date,cd_min,cd_max,cd_prom,cd_total,cd_disp):
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "UPDATE SCR_CALENDARIO_ANALISIS SET CD_PRECIO_MEDIO = %s, CD_PRECIO_MIN = %s, CD_PRECIO_MAX = %s, DISPONIBLES = %s, TOTAL = %s, CT_PRECIO_MEDIO = %s, CT_PRECIO_MIN = %s, CT_PRECIO_MAX = %s WHERE ID = %s AND ID_ANUNCIO = %s"
            cursor.execute(sql, (cd_prom, cd_min, cd_max, cd_disp, cd_total, t_prom, t_min, t_max, reg_id, propiedad))

            #--- 
            print('Correcto #5 -> Registro Correcto del Log.')
        connection.commit()
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error #5 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---

for portal in SETTING:
    #--
    iteration = 0
    today = add_days(today,portal[5])
    print('Begin Date: ',today)
    #--
    while (iteration <= portal[4]):
        #---
        calc_date = add_days(today,iteration)
        print('iteration date: ',calc_date)
        print(iteration)
        #---        
        for prop in PROPIEDADES:
            #---
            last_id = get_last_calc(prop[1], int(calc_date.day),int(calc_date.month),int(calc_date.year))
            b_list = get_black_list(prop[1], int(calc_date.day),int(calc_date.month),int(calc_date.year))
            #---
            if (b_list == None):
                #---
                #last_id = None
                print('las id: ',last_id)
                #----
                id_consult = 0
                for consult in CONSULTA:
                    if (consult[6] == prop[8]) and (consult[2] == prop[2]):
                        id_consult = consult[0]
                print(id_consult)

                
                print('iteration date: ',calc_date)

                data_Extra = get_extract_dates(calc_date.strftime('%Y-%m-%d'),prop[2],id_consult,"",portal[7],portal[6])
                print("Competencia Total: ",len(data_Extra))

                #---
                COMPETENCIA_DIRECTA = get_direct_comp(prop[1])
                print('Competencia Directa Total: ', len(COMPETENCIA_DIRECTA))
                CD = []
                #---
                for anuncios in data_Extra:
                    #---
                    for temp_comp in COMPETENCIA_DIRECTA:
                        if temp_comp[1] == anuncios[1]:
                            CD.append(anuncios)
                #---
                print('Competencia Directa Disponible: ', len(CD))
                #print(CD)
                if (len(CD) > 0):
                    CD_data = get_one_field_data(CD, 7)
                    CD_precio = round(stats.mean(CD_data),2)            
                    CD_min_price = min(i for i in CD_data if i > int(portal[6]))
                    CD_max_price = max(CD_data)
                else:
                    CD_precio = 0            
                    CD_min_price = 0
                    CD_max_price = 0
                #---
                print("CD Precio :", CD_precio) 
                print("CD Min Price", CD_min_price)
                print("CD Max Price", CD_max_price)
                #---
                if (len(data_Extra) > 0):
                    data = get_one_field_data(data_Extra,7)
                    PRECIO_MEDIA = round(stats.mean(data),2)            
                    min_price = min(i for i in data if i > int(portal[6]))
                    max_price = max(data)
                else:
                    PRECIO_MEDIA = 0          
                    min_price = 0
                    max_price = 0
                #---
                print("CT Precio :", PRECIO_MEDIA)    
                print("CT Min Price", min_price)
                print("CT Max Price", max_price)
                #--
                if (last_id == None):
                    print("No hay datos registrados con anterioridad para esta propiedad")
                    insert_price(min_price,max_price,PRECIO_MEDIA,prop[1],calc_date,CD_min_price,CD_max_price,CD_precio,len(COMPETENCIA_DIRECTA),len(CD))
                #---def insert_price(cd_min,cd_max,cd_prom,cd_total,cd_disp):
                else:
                    print('Se actualizara el registro anterior')
                    update_price(last_id,min_price,max_price,PRECIO_MEDIA,prop[1],calc_date,CD_min_price,CD_max_price,CD_precio,len(COMPETENCIA_DIRECTA),len(CD))
                #---
            elif(b_list == 0):
                #---
                continue
                print('No se ha modificado ningun dato.')
                #---
            else:
                #--
                if (last_id == None):
                    print("No hay datos registrados con anterioridad para esta propiedad")
                    insert_price(b_list,b_list,b_list,prop[1],calc_date,b_list,b_list,b_list,0,0)
                #---def insert_price(cd_min,cd_max,cd_prom,cd_total,cd_disp):
                else:
                    print('Se actualizara el registro anterior')
                    update_price(last_id,b_list,b_list,b_list,prop[1],calc_date,b_list,b_list,b_list,0,0)


            
        #---
        iteration += 1







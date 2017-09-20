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
        sql = "SELECT ID_PORTAL, NOMBRE, URL, DIAS_VERIFICACION, MAX_DAYS, PRICE_PROM,PRICE_MIN,PRICE_MAX FROM SCR_PORTALES WHERE CALC_PRICE = 1"        
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
print(PROPIEDADES)
#---
try:
    #---
    with connection.cursor() as cursor:
        #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
        sql = "SELECT CD.[ID],CD.[ID_ANUNCIO],CD.[ID_COMPETENCIA],CD.[NIVEL_COMPETITIVO],C.[TITULO],C.[ID_PORTAL],C.[TIPO],C.[URL],C.[CAPACIDAD],C.[NCAMAS],C.[PAIS],C.[CIUDAD],C.[CALIDAD] FROM [SCR_COMPETENCIA_DIRECTA] CD JOIN [SCR_COMPETENCIA] C ON CD.[ID_COMPETENCIA] = C.[ID_COMPETENCIA];"        
        cursor.execute(sql)
        COMPETENCIA_DIRECTA = cursor.fetchall() #<--- Lista con los portales activos.
        #---
        #print(PORTAL)
        print('Correcto -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
print(COMPETENCIA_DIRECTA)
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
print(CONSULTA)
#---
def get_extract_dates(day,portal,consulta,tipo):
#---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
            sql = "SELECT [ID_ANUNCIANTE],[ID_COMPETENCIA],[ID_ANUNCIO],[FECHAI],[FECHAF],[ORDEN],[ID_PORTAL],[PRECIO],[ID_CONSULTA],[TIPO],[N_CAMA],[RATIO],[FECHA_INGRESO] FROM [foxclea_tareas].[foxclea_tareas].[SCR_ANUNCIANTES] WHERE foxclea_tareas.solo_fecha(%s) between [FECHAI] and [FECHAF]-1 AND ID_PORTAL = %s AND ID_CONSULTA = %s ORDER BY [FECHAI] ASC"
            cursor.execute(sql,(day,portal,consulta))
            return cursor.fetchall() #<--- Lista con los portales activos.
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos del "portal" a usar.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
print(CONSULTA)
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
def get_last_calc(id):
    try:
    #---
        with connection.cursor() as cursor:
            sql = "SELECT TOP 1 * FROM SCR_CALENDARIO_ANALISIS WHERE ID_ANUNCIO = %s ORDER BY [ID] DESC"            
            cursor.execute(sql,id)
            return cursor.fetchall() #<--- Lista con los portales activos.
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
print('today: ',today)
last_date = add_days(today,60)
print('last_date: ',last_date)
print('month: ', last_date.month)
print('day: ', last_date.day)
#reserv_day = get_reservation_dates('2017-09-15')
#print(reserv_day)
def insert_price(t_min,t_max,t_prom,propiedad,date):
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "INSERT INTO SCR_CALENDARIO_ANALISIS (ID_ANUNCIO,DIA,MES,AÑO,CT_PRECIO_MEDIO,CT_PRECIO_MIN,CT_PRECIO_MAX) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, (propiedad, date.day, date.month, date.year, t_prom, t_min, t_max))

            #---
            print('Correcto #5 -> Registro Correcto del Log.')
        connection.commit()
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error #5 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)

def update_price(reg_id,t_min,t_max,t_prom,propiedad,date):
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "UPDATE SCR_CALENDARIO_ANALISIS SET CT_PRECIO_MEDIO = %s, CT_PRECIO_MIN = %s, CT_PRECIO_MAX = %s WHERE ID = %s AND ID_ANUNCIO = %s"
            cursor.execute(sql, (t_prom, t_min, t_max, reg_id, propiedad))

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
    #--
    while (iteration <= portal[4]):
        #---
        calc_date = add_days(today,iteration)
        print('iteration date: ',calc_date)
        print(iteration)
        #---        
        for prop in PROPIEDADES:
            last_calc = get_last_calc(prop[1])
            last_id = None
            if (last_calc != None):
                for regis in last_calc:
                    if (regis[2] == int(calc_date.day) and regis[3] == int(calc_date.month)):
                        last_id = regis[0]
                        break
                        #---
                #---
            #----
            id_consult = 0
            for consult in CONSULTA:
                if (consult[6] == prop[8]) and (consult[2] == prop[2]):
                    id_consult = consult[0]
            print(id_consult)

            PORT_DETAIL = []
            PORT_DETAIL = get_last_regis(prop[2])
            begin_date =  today + datetime.timedelta(days=int(PORT_DETAIL[13]))
            print(begin_date)

            data_Extra = get_extract_dates(calc_date.strftime('%Y-%m-%d'),prop[2],id_consult,"")
            print("data: ",len(data_Extra))

            data = get_one_field_data(data_Extra,7)
            PRECIO_MEDIA = round(stats.mean(data),2)
            print("Precio :",PRECIO_MEDIA)    
            min_price = min(i for i in data if i > 39)
            max_price = max(data)
            print("Min Price",min_price)
            print("Max Price",max_price)
            #--
            if (last_id == None):
                print("No hay datos registrados con anterioridad para esta propiedad")
                insert_price(min_price,max_price,PRECIO_MEDIA,prop[1],calc_date)
            #---
            else:
                print('Se actualizara el registro anterior')
                update_price(last_id,min_price,max_price,PRECIO_MEDIA,prop[1],calc_date)
            #---

            
        #---
        iteration += 1







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
import requests
import xml.etree.ElementTree as ET
#------------- Finalizan Imports. -------------#

#------------- Inicia Declaración de Variables Globales. -------------#
SETTING = []
PROPIEDADES = []
KEY = '9fa8f6d8d290a249137f131f8f0fb4c5'
today = (datetime.datetime.now()).date() #<--- Fecha de hoy.
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
#--- Función para modificar el estado de la consulta.


def change_State(state):
    #---
    td = (datetime.datetime.now())  # <--- Fecha del dia de hoy.
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #---
            sql = "UPDATE SCR_ESTADO SET ESTADO = %s, FECHA = %s WHERE ID_PORTAL = %s"
            cursor.execute(sql, (state, today, 11))
        connection.commit()
        print("------ Se ha realizado el cambio de estado de la consulta.")
    #---
    except _mssql.MssqlDatabaseException as e:
        #---
        print('Error -> Número de error: ', e.number,
              ' - ', 'Severidad: ', e.severity)
    #---
    if (state == True):
        return "El Script se ejecuto correctamente. Se Ha modificado el estado de ejecución del script."
    else:
        return "El Script no se termino de ejecutar. Se Ha modificado el estado de ejecución del script."


#---
print(change_State(False))  # --- Se cambia el estado de la pagina a Falso.
#---
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
#---
try:
    #---
    with connection.cursor() as cursor:
        #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
        sql = "SELECT p.ID_PROPIEDAD,pa.ID_ANUNCIO,a.ID_PORTAL,p.[ID_PROPIETARIO],p.[NOMBRE_PROPIEDAD],p.[GRUPO_ID],p.[IDHAB],h.[NOMBRE],h.[numpersonas],pa.[OCTORATE_PROPERTY_ID],pa.[OCTORATE_ID],pa.[ALLOW_MOD],a.TITULO,p.[DORMITORIOS],p.[BAÑOS],p.[TELEFONO],p.[MOVIL],p.[DIRECCION],p.[codigo_postal],p.[poblacion],p.[PROVINCIA],p.[LATITUD],p.[LONGITUD],p.[PAIS] FROM [foxclea_tareas].[foxclea_tareas].[AV_PROPIEDADES] p join av_habitacion h on p.[IDHAB] = h.[IDHAB] JOIN SCR_PROPIEDADES_ANUNCIOS pa on p.ID_PROPIEDAD = pa.ID_PROPIEDAD JOIN SCR_ANUNCIOS a on pa.ID_ANUNCIO = a.ID_ANUNCIO;"        
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
#--- Suma Meses a una Fecha
def add_days(sourcedate,days):
    #---
    modified_date = sourcedate + timedelta(days=days)
    #---
    return modified_date #<--- Devuelve una fecha.
#---
def get_date_price(id_hab, dates):
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
            sql = "SELECT PRECIO FROM SCR_CALENDARIO_ANALISIS WHERE ID_ANUNCIO = %s AND DIA = %s AND MES = %s AND AÑO = %s"        
            cursor.execute(sql, (id_hab, dates.day, dates.month, dates.year))
            temp_data = cursor.fetchall() #<--- Lista con los portales activos.
            if (temp_data):
                return temp_data[0]
            else:
                return 0
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos del "portal" a usar.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
def update_oct_price(prop_id,date,room_id,price):
    day = str(date.year) + '-' + str(date.month) + '-' + str(date.day) 
    xml = """xml=<?xml version="1.0" encoding="UTF-8"?>
      <SetAllocation>
       <Auth>
          <ApiKey>%s</ApiKey>
          <PropertyId>%s</PropertyId>
       </Auth>
       <UpdateMethod>availbb</UpdateMethod>
       <DateRange>
          <StartDate>%s</StartDate>
          <EndDate>%s</EndDate>
       </DateRange>
       <Allocations>
          <Allocation>
             <RoomTypeId>%s</RoomTypeId>
             <Price>%s</Price>
        </Allocation>
       </Allocations>
     </SetAllocation>"""%(KEY,prop_id,day,day,room_id,price)
    print(xml)
    headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'} # set what your server accepts
    response = requests.post('https://www.bbliverate.com/api/live/callApi.php?method=UpdateAvailbb', data=xml, headers=headers).text
    if ('Success' in response):
        print('Se asigno el precio en la fecha: ', day)
        return True
    elif('error' in response or 'false' in response):
        print('Ocurrio un error al asignar el precio en la fecha: ', day)
        return False
    #---    
#----
def get_total_oct_day_avia(prop_id,date):
    day = str(date.year) + '-' + str(date.month) + '-' + str(date.day) 
    xml = """xml=<?xml version="1.0" encoding="UTF-8"?>
  <GetAvailability>
  <Auth>
      <ApiKey>%s</ApiKey>
      <PropertyId>%s</PropertyId>
  </Auth>
    <From>%s</From>
    <To>%s</To>
 </GetAvailability>"""%(KEY,prop_id,day,day)
    print(xml)
    #----
    headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'} # set what your server accepts
    response = requests.post('https://www.bbliverate.com/api/live/callApi.php?method=GetAvailability', data=xml, headers=headers)
    #print(response.text)
    return response.content
    #---   
#---
def get_room_avia(xml_data,room_id):
    # create element tree object
    tree = ET.fromstring(xml_data) 
    print('-------')    
    # get root element
    #root = tree.getroot()
    room = tree.find('.//RoomsAvailability/RoomAvailability[@RoomId="'+ str(room_id) +'"]')
    print('Disponibilidad de Habitación: ', room)

    avia = room.find(".//Alot")    
    print('Disponibilidad: ', avia.text)
    if(avia.text == "1"):
        return True
    else:
        return False

#---


#---
for portal in SETTING:
    #--
    iteration = 0
    today = add_days(today, portal[5])
    short_date =  add_days(today, int(portal[8]))
    #---
    print('Begin Date: ', today)    
    print('Short date: ', short_date)
    #---
    while (iteration <= portal[4]):
        #---
        calc_date = add_days(today,iteration)        
        print('iteration date: ',calc_date)
        print(iteration)

        #---        
        for prop in PROPIEDADES:
            #---
            if (prop[11] == True):
                print('Anuncio ID: ', prop[1])
                rooms_avia = get_total_oct_day_avia(prop[9],calc_date)
                avia = get_room_avia(rooms_avia,prop[10])
                #---
                if (avia == True):
                    #---
                    price = get_date_price(prop[1], calc_date)
                    update_oct = update_oct_price(prop[9],calc_date,prop[10],price)
                    #---
                #---
        #---
        iteration += 1
        #---
        #if(iteration == 2):
        #    break
print(change_State(True))

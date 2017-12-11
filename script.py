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
SETTING = [] #<--- Arreglo dondo se almacenan las configuraciones por portal.
PROPIEDADES = [] #<--- Variable donde se alamacenaran las propiedades.
CONSULTA = [] #<--- Arreglo donde se almacenaran todas las consultas por portal.
COMPETENCIA_DIRECTA = [] #<--- Arreglo donde se guardaran la competencia directa.
today = (datetime.datetime.now()).date() #<--- Variable donde se almacena la fecha de hoy.
#---
#------------- Fin de declaración de variables -------------#

#-- La funion que une anunciantes con consulta, tiene que recibir dun parametro, el cual sera una fecha, esa fecha tiene que ser igual o mayor a la fecha de entrada, y igual o menor a la fehca de salida (es decir puede estar entre medio)
#-- Funcion para listar los anuncios de la empresa:
#-- Funcion para listar la competencia directa:
#-- 
#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
#---

try:
    #---
    with connection.cursor() as cursor:
        #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
        sql = "SELECT ID_PORTAL, NOMBRE, URL, DIAS_VERIFICACION, MAX_DAYS, BEGIN_DAY, PRICE_MIN, PRICE_MAX, NEAR_DAYS, INCREMENTO_1, RESERVAS_MAX_1, INCREMENTO_2, RESERVAS_MAX_2, CAPACITY_SEARCH FROM SCR_PORTALES WHERE CALC_PRICE = 1"
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
#--- Función para modificar el estado de la consulta.
def change_State(state):
    #---
    td = (datetime.datetime.now()) #<--- Fecha del dia de hoy.
    #---
    try:
    #---
        with connection.cursor() as cursor:
        #--- CONSULTA PARA CAMBIAR EL ESTADO DEL SCRIPT
            sql = "UPDATE SCR_ESTADO SET ESTADO = %s, FECHA = %s WHERE ID_PORTAL = %s"
            cursor.execute(sql, (state,td,7))
        connection.commit()
        print("------ Se ha realizado el cambio de estado de la consulta.")
    #---
    except _mssql.MssqlDatabaseException as e:
    #---
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---
    if (state == True):
        return "El Script se ejecuto correctamente. Se Ha modificado el estado de ejecución del script."
    else:
        return "El Script no se termino de ejecutar. Se Ha modificado el estado de ejecución del script."
#---
print(change_State(False)) #--- Se cambia el estado de la pagina a Falso.
#---
#--- FUNCIÓN PARA OBTENER LAS PROPIEDADES POR PORTAL QUE SE ANALIZARAN.
def get_properties(id_portal):
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de las PROPIEDADES_ANUNCIOS
            sql = "SELECT p.ID_PROPIEDAD,pa.ID_ANUNCIO,a.ID_PORTAL,p.[ID_PROPIETARIO],p.[NOMBRE_PROPIEDAD],p.[GRUPO_ID],p.[IDHAB],h.[NOMBRE],h.[numpersonas],pa.[OCTORATE_PROPERTY_ID],pa.[OCTORATE_ID],pa.[ALLOW_MOD],a.TITULO,p.[DORMITORIOS],p.[BAÑOS],p.[TELEFONO],p.[MOVIL],p.[DIRECCION],p.[codigo_postal],p.[poblacion],p.[PROVINCIA],p.[LATITUD],p.[LONGITUD],p.[PAIS] FROM [foxclea_tareas].[foxclea_tareas].[AV_PROPIEDADES] p JOIN av_habitacion h ON p.[IDHAB] = h.[IDHAB] JOIN SCR_PROPIEDADES_ANUNCIOS pa ON p.ID_PROPIEDAD = pa.ID_PROPIEDAD JOIN SCR_ANUNCIOS a ON pa.ID_ANUNCIO = a.ID_ANUNCIO WHERE a.ID_PORTAL = %s;"
            cursor.execute(sql, (id_portal))            
            #---
            print('Correcto -> Extracción de las PROPIEDADES_ANUNCIOS')
            return cursor.fetchall()  # <--- Lista con las propiedades.
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#--- FUNCIÓN PARA CONTAR LA CANTIDAD DE PROPIEDADES POR GRUPO (GROUP_ID)
def count_property(id_portal, group_id, PROPERS):
    #---
	total = 0 #<--- Se inicializa en 0 para que siempre devuelva algo.
	for propiedad in PROPERS:
		if (propiedad[2] == id_portal and propiedad[5] == group_id):
			total += 1
	#---		
	return total
#---
#--- FUNCION PARA OBTENER LA COMPETENCIA DIRECTA DE UN ANUNCIO.
def get_direct_comp(id_anuncio):
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de la COMPETENCIA_DIRECTA.
            sql = "SELECT ID_ANUNCIO, ID_COMPETENCIA, NIVEL_COMPETITIVO FROM SCR_COMPETENCIA_DIRECTA WHERE ID_ANUNCIO = %s;"        
            cursor.execute(sql, id_anuncio)            
            #---
            print('Correcto -> Extracción de los datos de la COMPETENCIA DIRECTA.')
            return cursor.fetchall()  # <--- Lista de la competencia DIRECTA.
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#--- EXTRACCIÓN DE LAS CONSULTAS
def get_consultas(id_portal):
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de las CONSULTAS POR PORTAL
            sql = "SELECT [ID_CONSULTA],[ID_ANUNCIO],[ID_PORTAL],[PAIS],[CIUDAD],[ZONA],[ADULTOS],[NIÑOS],[BEBES],[ESTADO] FROM [foxclea_tareas].[SCR_CONSULTAS] WHERE ID_PORTAL = %s;"
            cursor.execute(sql, id_portal)
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos de las CONSULTAS por portal.')
            return cursor.fetchall()  # <--- Lista de las CONSULTAS por portal.
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#--- FUNCIÓN PARA DEVOLVER LOS ANUNCIOS EXTRAIDOS EN DETERMINADAS FECHAS.
def get_extract_dates(day,portal,consulta,tipo,max_price, min_price):
#---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de los ANUNCIOS extraidos en ANUNCIANTES.
            sql = "SELECT  max([ID_ANUNCIANTE]) as ID_ANUNCIANTE, max([ID_COMPETENCIA]) as ID_COMPETENCIA, max([ID_ANUNCIO]) as ID_ANUNCIO, max([FECHAI]) as FECHAI, max([FECHAF]) as FECHAF, max([ORDEN]) as ORDEN, max([ID_PORTAL]) as ID_PORTAL, max([PRECIO]) as PRECIO, max([ID_CONSULTA]) as ID_CONSULTA, max([TIPO]) as TIPO, max([N_CAMA]) as N_CAMA, max([RATIO]) as RATIO, max([FECHA_INGRESO]) as FECHA_INGRESO FROM [foxclea_tareas].[foxclea_tareas].[SCR_ANUNCIANTES] WHERE foxclea_tareas.solo_fecha(%s) between [FECHAI] and [FECHAF]-1 AND ID_PORTAL = %s AND ID_CONSULTA = %s  AND PRECIO < %s GROUP BY ID_COMPETENCIA ORDER BY max(FECHA_INGRESO) DESC;"
            cursor.execute(sql,(day,portal,consulta,max_price))
            #---
            print('Correcto -> Extracción de los datos de ANUNCIOS extraidos')
            return cursor.fetchall()  # <--- Lista con los anuncios extraidos
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#--- FUNCIÓN PARA DEVOLVER LOS ANUNCIOS EXTRAIDOS EN DETERMINADAS FECHAS.
def get_extract_dates_specific(day,portal,capacity,tipo,max_price, min_price):
#---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de los ANUNCIOS extraidos en ANUNCIANTES.
            sql = """
                 SELECT TOP 100 PERCENT MAX(foxclea_tareas.SCR_ANUNCIANTES.ID_ANUNCIANTE) AS ID_ANUNCIANTE, MAX(foxclea_tareas.SCR_ANUNCIANTES.ID_COMPETENCIA) AS ID_COMPETENCIA, 
                         MAX(foxclea_tareas.SCR_ANUNCIANTES.ID_ANUNCIO) AS ID_ANUNCIO, MAX(foxclea_tareas.SCR_ANUNCIANTES.FECHAI) AS FECHAI, MAX(foxclea_tareas.SCR_ANUNCIANTES.FECHAF) AS FECHAF, 
                         MAX(foxclea_tareas.SCR_ANUNCIANTES.ORDEN) AS ORDEN, MAX(foxclea_tareas.SCR_ANUNCIANTES.ID_PORTAL) AS ID_PORTAL, MAX(foxclea_tareas.SCR_ANUNCIANTES.PRECIO) AS PRECIO, 
                         MAX(foxclea_tareas.SCR_ANUNCIANTES.ID_CONSULTA) AS ID_CONSULTA, MAX(foxclea_tareas.SCR_ANUNCIANTES.TIPO) AS TIPO, MAX(foxclea_tareas.SCR_ANUNCIANTES.N_CAMA) AS N_CAMA, 
                         MAX(foxclea_tareas.SCR_ANUNCIANTES.RATIO) AS RATIO, MAX(foxclea_tareas.SCR_ANUNCIANTES.FECHA_INGRESO) AS FECHA_INGRESO, MAX( foxclea_tareas.SCR_COMPETENCIA.CAPACIDAD) AS CAPACIDAD
				FROM foxclea_tareas.SCR_ANUNCIANTES LEFT OUTER JOIN
				foxclea_tareas.SCR_COMPETENCIA ON foxclea_tareas.SCR_ANUNCIANTES.ID_COMPETENCIA = foxclea_tareas.SCR_COMPETENCIA.ID_COMPETENCIA 
				WHERE foxclea_tareas.solo_fecha(%s) between SCR_ANUNCIANTES.[FECHAI] and SCR_ANUNCIANTES.[FECHAF]-1 AND SCR_COMPETENCIA.CAPACIDAD = %s AND SCR_ANUNCIANTES.ID_PORTAL = %s  AND SCR_ANUNCIANTES.PRECIO < %s
				GROUP BY foxclea_tareas.SCR_ANUNCIANTES.ID_COMPETENCIA
				ORDER BY MAX(foxclea_tareas.SCR_ANUNCIANTES.FECHA_INGRESO) DESC;
            """
            cursor.execute(sql,(day,capacity,portal,max_price))
            #---
            print('Correcto -> Extracción de los datos de ANUNCIOS extraidos')
            return cursor.fetchall()  # <--- Lista con los anuncios extraidos
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#print(CONSULTA)
#--- FUNCIÓN PARA OBTENER LAS RESERVAS SOLICITADAS EN UN DÍA DETERMINADO.
def get_reservation_dates(day,group_id):
    try:
    #---
        with connection.cursor() as cursor:
            #--- Extraccion de los datos de las RESERVAS.
            sql = "SELECT R.ID_RESERVA, R.GRUPO_ID, R.ID_HAB, R.FECHA_RESERVA, R.FECHA_ENTRADA, R.FECHA_SALIDA, R.NO_PERSONAS, R.NO_NIÑOS, R.PRECIO, R.PROCEDENCIA,  R.ESTADO, R.PRECIOextra, R.COMISION, R.GASTOENERGIABAS, R.GASTOLIMREAL, S.NOMBRE, P.NOMBRE_PROPIEDAD, P.ID_PROPIEDAD, R.ID_PROPIEDAD FROM AV_RESERVAS R LEFT JOIN BMSUBCON S ON NUMERO = R.AGENCIA LEFT JOIN  AV_PROPIEDADES P on  P.ID_PROPIEDAD = R.ID_PROPIEDAD WHERE(foxclea_tareas.solo_fecha(%s) between R.fecha_entrada and R.fecha_salida) and R.GRUPO_ID = %s AND R.ESTADO <> 'CANCELADA' AND R.ESTADO <> 'PENDIENTE' AND R.ESTADO <> 'NOSHOW'  ORDER BY R.FECHA_ENTRADA ASC;"
            cursor.execute(sql,(day,group_id))
            #---
            print('Correcto -> Extracción de los datos de las RESERVAS')
            return cursor.fetchall() #<--- Lista con las RESERVAS.
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#--- FUNCIÓN PARA OBTENER EL "ID" DEL ULTIMO REGISTRO EN CALENDARIO_ANALISIS
def get_last_calc(id, dia, mes, año):
    try:
    #---
        with connection.cursor() as cursor:
            sql = "SELECT ID FROM SCR_CALENDARIO_ANALISIS WHERE ID_ANUNCIO = %s AND DIA = %s AND MES = %s AND AÑO = %s"            
            cursor.execute(sql,(id, dia, mes, año))
            temp = cursor.fetchone() #<--- Lista con los portales activos.
            #---
            print('Correcto -> Extracción del ultimo registro de CALENDARIO ANALISIS')
            #---
            if temp:
                return temp[0]
            else:
                return None
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#--- FUNCION PARA OBTENER UN REGISTRO EN CALENDARIO ANALISIS POR EL "ID" --- NO SE USA.
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
#--- OBTIENE EL ULTIMO REGISTRO POR PORTAL DE LOS DATOS ANALISADOS DE LAS RESERVAS -- NO SE USA.
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
            print('Correcto -> Extracción de los datos de "PORTALES_DETALLES".')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---   
    return last_result
#---

#--- Se genera una lista con los datos de un campo especifico.
def get_one_field_data(data, index): #<--DATA es todo el arreglo ex: [[1,2,3,4],[1,2,3,4],[1,2,3,4]] y index es el indice que interesa de esos arreglos.
    #--- si index fuera 1 (utilizando el ejemplo comentariado arriba), temp_result seria = [2,2]
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
#--- FUNCION PARA EXTRAER VALORES PREDEFINIDOS POR EL USUARIO EN LA TABLA "SCR_CALENDAR_DEFAULT_LIST"
def get_black_list(id_hab,dia,mes,año):
    #---
    try:
        #---        
        last_result = []
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
#--- FUNCION PARA INSERTAR UN REGISTRO DE PRECIOS EN "SCR_CALENDARIO_ANALISIS"
def insert_price(t_min,t_max,t_prom,propiedad,date,cd_min,cd_max,cd_prom,cd_total,cd_disp,price,cant_reservas):
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "INSERT INTO SCR_CALENDARIO_ANALISIS (ID_ANUNCIO,DIA,MES,AÑO,CD_PRECIO_MEDIO,CD_PRECIO_MIN,CD_PRECIO_MAX,DISPONIBLES,TOTAL,CT_PRECIO_MEDIO,CT_PRECIO_MIN,CT_PRECIO_MAX,PRECIO,RESERVAS) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, (propiedad, date.day, date.month, date.year, cd_prom, cd_min, cd_max, cd_disp, cd_total, t_prom, t_min, t_max, price,cant_reservas))

            #---
            print('Correcto #5 -> Registro Correcto del Log.')
        connection.commit()
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error #5 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#--- FUNCION PARA ACTUALIZAR UN REGISTRO DE PRECIOS EN "SCR_CALENDARIO_ANALISIS"
def update_price(reg_id,t_min,t_max,t_prom,propiedad,date,cd_min,cd_max,cd_prom,cd_total,cd_disp,price,cant_reservas):
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "UPDATE SCR_CALENDARIO_ANALISIS SET CD_PRECIO_MEDIO = %s, CD_PRECIO_MIN = %s, CD_PRECIO_MAX = %s, DISPONIBLES = %s, TOTAL = %s, CT_PRECIO_MEDIO = %s, CT_PRECIO_MIN = %s, CT_PRECIO_MAX = %s, PRECIO = %s, RESERVAS = %s WHERE ID = %s AND ID_ANUNCIO = %s"
            cursor.execute(sql, (cd_prom, cd_min, cd_max, cd_disp, cd_total, t_prom, t_min, t_max, price, cant_reservas, reg_id, propiedad))

            #--- 
            print('Correcto #5 -> Registro Correcto del Log.')
        connection.commit()
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error #5 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
#--- INICIA EL RECORRIDO POR LOS DATOS DE PORTALES.
for portal in SETTING:
    #--
    iteration = 0 #<--- conteo de días.
    today = add_days(today,portal[5]) #<--- Se obtiene el día con que inicia el PORTAL, PORTAL[5] = BEGIN_DAY
    PROPIEDADES = get_properties(portal[0]) #<--- Se obtienen las PROPIEDADES que tiene el PORTAL.
    CONSULTA = get_consultas(portal[0]) #<--- Se obtinene las CONSULTAS por PORTAL
    print('Begin Date: ',today)
    short_date =  add_days(today,int(portal[8])) #<--- Se calcula los Días Proximos, portal[8] = NEAR_DAYS
    print('Short date: ', short_date)
    #---
    while (iteration <= portal[4]): #<-- ITERACION = 0 Y PORTAL[4] = MAX_DAYS
        #---
        calc_date = add_days(today,iteration) #<-- DÍA EN EL QUE INICIA EL CALCULO.       
        print('iteration date: ',calc_date)
        print(iteration)

        #--- SE RECORREN LAS PORPIEDADES DEL PORTAL.
        for prop in PROPIEDADES:
            #---
            last_id = get_last_calc(prop[1], int(calc_date.day),int(calc_date.month),int(calc_date.year))
            b_list = get_black_list(prop[1], int(calc_date.day),int(calc_date.month),int(calc_date.year))
            reservas = get_reservation_dates(calc_date.strftime('%Y-%m-%d'),prop[5])
            cant_reservas = 0
            cant_reservas = len(reservas)            
            total_reserv = count_property(prop[2], prop[5], PROPIEDADES)
            perc_reservas = 0
            #---
            if(cant_reservas != 0):
                perc_reservas = (cant_reservas/total_reserv)
            #---
            print("ID Propiedad: ", prop[1])
            print('Total reservas: ', total_reserv, ' --- grupo_id: ', prop[5])
            print('Cantidad reservas: ', cant_reservas)
            print('Porcentage reservas: ', perc_reservas)
            #--- LISTA NEGRA.
            if (b_list == None):
                #---
                #last_id = None
                print('Last ID: ', last_id)
                #----
                id_consult = 0
                consulta_cap = 0 #<-- CONSULTA CAPACIDAD
                #--- CONSULTAS
                for consult in CONSULTA:
                    if (consult[6] == prop[8]) and (consult[2] == prop[2]):
                        id_consult = consult[0]
                        consulta_cap = consult[6]
                print("ID Consulta: ", id_consult)
                print('Iteration Date: ',calc_date)
                #---
                if(portal[13] == True):
                    data_Extra = get_extract_dates_specific(calc_date.strftime('%Y-%m-%d'),prop[2],prop[8],"",portal[7],portal[6])
                else:
                    data_Extra = get_extract_dates(calc_date.strftime('%Y-%m-%d'),prop[2],id_consult,"",portal[7],portal[6])
                #---
                print("Competencia Total: ",len(data_Extra))
                #---
                COMPETENCIA_DIRECTA = get_direct_comp(prop[1])
                print('Competencia Directa Total: ', len(COMPETENCIA_DIRECTA))
                CD = [] #<--- COMPETENCIA DIRECTA
                #---
                for anuncios in data_Extra:
                    #---
                    for temp_comp in COMPETENCIA_DIRECTA:
                        if temp_comp[1] == anuncios[1]:
                            CD.append(anuncios)
                #---
                print('Competencia Directa Disponible: ', len(CD))
                percentage = 0 #<--- PORCENTAGE
                allow = False
                if (len(CD) > 0 and len(COMPETENCIA_DIRECTA) > 0):
                    percentage = (len(CD) / len(COMPETENCIA_DIRECTA)) * 100
                    allow = True
                #----
                print('Porcentage disponible: ',percentage) 
                #print(CD)
                if (len(CD) > 0):
                    #----COMPENTENCIA DIRECTA CALCULOS
                    CD_data = get_one_field_data(CD, 7)
                    CD_precio = round(stats.mean(CD_data),2)
                    if (len(CD) > 1):
                        CD_min_price = min(i for i in CD_data if i > int(portal[6]))
                    else:
                        CD_min_price = CD_data[0]

                    CD_max_price = max(CD_data)
                else:
                    CD_precio = 0            
                    CD_min_price = 0
                    CD_max_price = 0
                #---
                print("CD Precio: ", CD_precio)
                print("CD Min Price: ", CD_min_price)
                print("CD Max Price: ", CD_max_price)
                #---
                if (len(data_Extra) > 0):
                    #----COMPENTENCIA TOTAL CALCULOS
                    data = get_one_field_data(data_Extra,7)
                    PRECIO_MEDIA = round(stats.mean(data),2)    
                    if (len(data_Extra) > 1):
                        min_price = min(i for i in data if i > int(portal[6]))
                    else:
                        min_price = data[0]
                    max_price = max(data)
                else:
                    PRECIO_MEDIA = 0          
                    min_price = 0
                    max_price = 0
                #----
                price = 0                
                if (percentage >= 60 and len(CD) > 0):
                    price = CD_precio
                elif (allow == True, percentage <= 35 and len(CD) > 0 and len(data_Extra) > 0 and consulta_cap > 3):
                    price = ((PRECIO_MEDIA + max_price) / 2)
                else:
                    price = PRECIO_MEDIA

                if (perc_reservas >= portal[10]):
                	price = Decimal(price) + portal[9]
                if (perc_reservas >= portal[12]):
                	price = Decimal(price) + portal[11]

                #-----
                if (calc_date <= short_date):
                    price = PRECIO_MEDIA
                    print('La Fecha esta proxima por lo que se usara el precio total: ', price)
                #---
                print("CT Precio: ", PRECIO_MEDIA)
                print("CT Min Price: ", min_price)
                print("CT Max Price: ", max_price)
                #--
                if (last_id == None):
                    print("No hay datos registrados con anterioridad para esta propiedad")
                    insert_price(min_price,max_price,PRECIO_MEDIA,prop[1],calc_date,CD_min_price,CD_max_price,CD_precio,len(COMPETENCIA_DIRECTA),len(CD), price,cant_reservas)
                #---def insert_price(cd_min,cd_max,cd_prom,cd_total,cd_disp):
                else:
                    print('Se actualizara el registro anterior')
                    update_price(last_id,min_price,max_price,PRECIO_MEDIA,prop[1],calc_date,CD_min_price,CD_max_price,CD_precio,len(COMPETENCIA_DIRECTA),len(CD), price,cant_reservas)
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
                    insert_price(b_list,b_list,b_list,prop[1],calc_date,b_list,b_list,b_list,0,0,b_list,cant_reservas)
                #---def insert_price(cd_min,cd_max,cd_prom,cd_total,cd_disp):
                else:
                    print('Se actualizara el registro anterior')
                    update_price(last_id,b_list,b_list,b_list,prop[1],calc_date,b_list,b_list,b_list,0,0,b_list,cant_reservas)

            print('---------------------------------')            
        #---
        iteration += 1
        print(change_State(True))

print(change_State(True))






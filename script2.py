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

#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
try:
    #---
    with connection.cursor() as cursor:
        #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
        sql = "SELECT ID_PORTAL, NOMBRE, URL, DIAS_VERIFICACION FROM SCR_PORTALES WHERE ESTADIST_CALC = 1"
        cursor.execute(sql)
        SETTING = cursor.fetchall() #<--- Lista con los portales activos.
        #---
        #print(PORTAL)
        print('Correcto -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---

#------------- Inicio de las funciones Generales -------------#
#---

#---
#------------- Fin de las funciones Generales -------------#

#------------- Inicio de declaración de variables -------------#
#---

#---
#------------- Fin de declaración de variables -------------#
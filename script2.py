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
xml = """xml=<?xml version="1.0" encoding="UTF-8"?>
<GetProperties>
    <ApiKey>9fa8f6d8d290a249137f131f8f0fb4c5</ApiKey>
</GetProperties>"""#%(AddressVerified,AmountPaid)

print(xml)

headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'} # set what your server accepts
print(requests.post('https://www.bbliverate.com/api/live/callApi.php?method=GetProperties', data=xml, headers=headers).text)
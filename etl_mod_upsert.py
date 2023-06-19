from etl_mod_connection import sf
from etl_mod_clean import *

#&& Funciones Principales
#~~ Funciones Secundarias (que se llaman dentro de las Funciones Principales)

#&& Obtiene los ids y la fecha maxima del campo "ultima modificacion" en PSQL &&#
def psql_obtener_ids_y_fecha_maxima_modificacion(conexion, objeto_standard):
    cursor = conexion.cursor() # Crear cursor
    # Query a postgreSQL
    cursor.execute(f"select id, lastmodifieddate::text from etl.{objeto_standard};") # Obtengo la fecha maxima de lastmodifieddate
    id_all_psql = []
    date_mod_max_psql = []
    resultados = cursor.fetchall() # se obtienen los datos de psql

    for f in resultados:
        id_all_psql.append(
            f[0] # por la query de arriba, el id siempre sera el 0
        )
        date_mod_max_psql.append(
            f[1] # por la query de arriba, el lastmodifieddate siempre sera el 1
        )

    date_mod_max_psql = max(date_mod_max_psql) # obtiene la fecha maxima de modificacion
    conexion.close() # Cierra conexion

    return id_all_psql, date_mod_max_psql
#&& FIN Obtiene los ids y la fecha maxima del campo "ultima modificacion" en PSQL &&#

#&& Obtiene los datos de SF cuando la fecha sea mayor a la "ultima modificacion" de PSQL &&#
def buscar_actualizaciones_sf(data_mod_max_psql, objeto_standard, objeto):
    
    data_max_mod_psql_to_date_sf = date_standard_sf(data_mod_max_psql)

    # Query a SOQL - Salesforce
    result = sf().query(f"Select {', '.join(x for x in sel_list_objeto(objeto_standard))} from {objeto} where LastModifiedDate > {data_max_mod_psql_to_date_sf}") # Va a buscar todos los registros donde la fecha de modificacion sea mayor
    records = result['records']

    data_sf_upd_new = []

    for rec in records:
        data_sf_upd_new.append(
        tuple(rec[sel_list_objeto(objeto_standard)[x]] for x in range(0,len(sel_list_objeto(objeto_standard))))
        )
    
    return data_sf_upd_new, data_max_mod_psql_to_date_sf
#&& FIN Obtiene los datos de SF cuando la fecha sea mayor a la "ultima modificacion" de PSQL &&#

#&& Inserta y Actualiza los datos en PSQL &&#
def realizar_upsert_psql(conexion, data_sf_upd_new, objeto, objeto_standard,data_max_mod_psql_to_date_sf, id_all_psql):
    #cursor = conexion.cursor() # Crear cursor
    id_update_psql, id_insert_psql = [], []

    #! ALERT: Para que esta funcione tome, el 'Id' (sf[0]) siempre debe ser el primero en data_selected
    for sf in data_sf_upd_new: # recorre todo el for para solo obtener el id
        if sf[0] in id_all_psql: # compara si el id de SF, se encuentra en PSQL 
            id_update_psql.append(
                sf[0]
            )
        else:
            id_insert_psql.append(
                sf[0]
            )
    
#&& FIN Inserta y Actualiza los datos en PSQL &&#

# todo Creacion de 2 funciones

#~~ Creacion de cuentas nuevas ~~#

#~~ FIN Creacion de cuentas nuevas ~~#

#~~ Actualizacion de cuentas ~~#

#~~ FIN Actualizacion de cuentas ~~#
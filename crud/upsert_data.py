from con.connection import sf
from mod.clean import date_standard_sf, sel_list_objeto, cambiar_id_al_final
from crud.insert_data import sf_obtener_datos

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

    return id_all_psql, date_mod_max_psql
#&& FIN Obtiene los ids y la fecha maxima del campo "ultima modificacion" en PSQL &&#

#&& Obtiene los datos de SF cuando la fecha sea mayor a la "ultima modificacion" de PSQL &&#
def buscar_actualizaciones_sf(data_mod_max_psql, objeto_standard, objeto):
    
    date_max_mod_psql_to_date_sf = date_standard_sf(data_mod_max_psql)

    # Query a SOQL - Salesforce
    result = sf().query(f"Select {', '.join(x for x in sel_list_objeto(objeto_standard))} from {objeto} where LastModifiedDate > {date_max_mod_psql_to_date_sf}") # Va a buscar todos los registros donde la fecha de modificacion sea mayor
    records = result['records']

    data_sf_upd_new = []

    for rec in records:
        data_sf_upd_new.append(
        tuple(rec[sel_list_objeto(objeto_standard)[x]] for x in range(0,len(sel_list_objeto(objeto_standard))))
        )

    return data_sf_upd_new, date_max_mod_psql_to_date_sf
#&& FIN Obtiene los datos de SF cuando la fecha sea mayor a la "ultima modificacion" de PSQL &&#

#&& Inserta y Actualiza los datos en PSQL &&#
def realizar_upsert_psql(conexion, data_sf_upd_new, objeto, objeto_standard,date_max_mod_psql_to_date_sf, id_all_psql, opc_elec):
    #cursor = conexion.cursor() # Crear cursor
    data_update_psql, data_insert_psql = [], []

    #! ALERT: Para que esta funcione tome, el 'Id' (sf[0]) siempre debe ser el primero en data_selected
    for sf in data_sf_upd_new: # recorre todo el for para solo obtener el id
        if sf[0] in id_all_psql: # compara si el id de SF, se encuentra en PSQL 
            sf = cambiar_id_al_final(sf)
            data_update_psql.append(
                sf
            )
        else:
            data_insert_psql.append(
                sf # buscar a sf
            )

    cursor = conexion.cursor()
    if len(data_update_psql) != 0:
        update_query(cursor, conexion, data_update_psql, objeto, objeto_standard, opc_elec)
    else:
        print("No existen datos para actualizar")
    
    if len(data_insert_psql) != 0:
        insert_query(cursor, conexion, data_insert_psql, objeto, objeto_standard, opc_elec)
    else:
        print("No existen datos nuevos")
#&& FIN Inserta y Actualiza los datos en PSQL &&#

#~~ Actualizacion de cuentas ~~#
def update_query(cursor, conexion, data_update_psql, objeto, objeto_standard, opc_elec):
    campos_objeto, s = sf_obtener_datos(objeto, objeto_standard, opc_elec, status_query = 'update')
    update_fields = '\n'.join([f"{x} = {y}" for x,y in zip(campos_objeto, s)])

    query_update = f"""
    update etl.{objeto_standard}
    set {update_fields}
    where Id = %s
    """
    cursor.executemany(query_update, data_update_psql)
    
    cant_registros = int(cursor.rowcount)
    print(f"Se actualizaron {cant_registros} filas de postgreSQL, en la tabla {objeto_standard}")
    conexion.commit()
#~~ FIN Actualizacion de cuentas ~~#

#~~ Creacion de cuentas nuevas ~~#
def insert_query(cursor, conexion, data_insert_psql, objeto, objeto_standard, opc_elec):
    
    campos_objeto, s = sf_obtener_datos(objeto, objeto_standard, opc_elec, status_query = 'insert')

    query_insert = f"""
    INSERT INTO etl.{str(objeto_standard).lower()} ({str(campos_objeto).lower()}) VALUES ({s});
    """

    cursor.executemany(query_insert, data_insert_psql)
    cant_registros = int(cursor.rowcount)
    print(f"Se insertaron {cant_registros} filas de Salesforce a postgreSQL, en la tabla {objeto_standard}")
    conexion.commit()    
#~~ FIN Creacion de cuentas nuevas ~~#

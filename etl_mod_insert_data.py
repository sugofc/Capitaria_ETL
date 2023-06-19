from etl_mod_connection import sf
from etl_mod_clean import *

#&& Funciones Principales
#~~ Funciones Secundarias (que se llaman dentro de las Funciones Principales)
#?? retornos

#&& ---------------- OBTIENE FILAS DE SALESFORCE ---------------- &&#
#^ Funcion para Obtener campos de Salesforce
def sf_obtener_datos(objeto, objeto_standard):

    data_sf_campos, campos_objeto, s  = obtener_datos_objeto(objeto, objeto_standard)

    return data_sf_campos, campos_objeto, s #?? etl
#&& ---------------- FIN OBTIENE FILAS DE SALESFORCE ---------------- &&#

#&& ---------------- INSERTAR FILAS DE SALESFORCE A POSTGRESQL ---------------- &&#
def insertar_registros_psql(conexion, sf_registros_pg, objeto_standard, campos_objeto, s):
    cursor = conexion.cursor() # Crear cursor
    
    query_truncate = f"TRUNCATE TABLE etl.{str(objeto_standard).lower()} CONTINUE IDENTITY RESTRICT;"
    cursor.execute(query_truncate)
    conexion.commit()

    query = f"""
    INSERT INTO etl.{str(objeto_standard).lower()} ({str(campos_objeto).lower()}) VALUES ({s});
    """

    cursor.executemany(query, sf_registros_pg)
    cant_registros = int(cursor.rowcount)
    print(f"Se insertaron {cant_registros} filas de Salesforce a postgreSQL, en la tabla {objeto_standard}")
    conexion.commit()
    conexion.close() # Cierra conexion
#&& ---------------- FIN INSERTAR FILAS DE SALESFORCE A POSTGRESQL ---------------- &&#

#~ ---------------- FUCION QUE OBTIENE LOS DATOS PARA LA INSERCION ---------------- ~#
def obtener_datos_objeto(objeto, objeto_standard):

    # Query a SOQL - Salesforce
    result = sf().query(f"Select {', '.join(x for x in sel_list_objeto(objeto_standard))} from {objeto}") 
    records = result['records']
    
    data_sf_campos = []

    for rec in records:
        data_sf_campos.append(
        tuple(rec[sel_list_objeto(objeto_standard)[x]] for x in range(0,len(sel_list_objeto(objeto_standard))))
        )

    campos_objeto = ', '.join(x for x in sel_list_objeto(objeto_standard))


    s = len(sel_list_objeto(objeto_standard)) * "%s, "
    s = s[:len(s)-2] # sirve para la BD
    
    return data_sf_campos, campos_objeto, s #? sf_obtener_datos


#~ ---------------- FIN FUCION QUE OBTIENE LOS DATOS PARA LA INSERCION ---------------- ~#


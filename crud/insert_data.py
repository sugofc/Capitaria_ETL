from con.connection import sf
from mod.clean import sel_list_objeto, quitar_id_s_dejarlo_en_lista

#todo revisar funcion 'obtener_datos_objeto' que es la misma que 'sf_obtener_datos'

#&& Funciones Principales
#~~ Funciones Secundarias (que se llaman dentro de las Funciones Principales)
#?? retornos

#&& ---------------- OBTIENE FILAS DE SALESFORCE ---------------- &&#
#^ Funcion para Obtener campos de Salesforce
def sf_obtener_datos(objeto, objeto_standard, opc_elec, status_query):

    campos_objeto = ','.join(x for x in sel_list_objeto(objeto_standard))

    s = len(sel_list_objeto(objeto_standard)) * "%s, "
    s = s[:len(s)-2] # le quito la el ultimo ', '

    if opc_elec == 3 or opc_elec == 4:
        # Query a SOQL - Salesforce
        if opc_elec == 3: 
            result = sf().query(f"Select {', '.join(x for x in sel_list_objeto(objeto_standard))} from {objeto}") 
        elif opc_elec == 4:
            id_rest_psql_for_sf = status_query
            result = sf().query(f"Select {', '.join(x for x in sel_list_objeto(objeto_standard))} from {objeto} where Id in ({id_rest_psql_for_sf})")

        records = result['records']
        
        data_sf_campos = []

        for rec in records:
            data_sf_campos.append(
            tuple(rec[sel_list_objeto(objeto_standard)[x]] for x in range(0,len(sel_list_objeto(objeto_standard))))
            )

        return data_sf_campos, campos_objeto, s #?? devuelve a: etl

    elif opc_elec == 2 and status_query == 'update':
        campos_objeto, s = quitar_id_s_dejarlo_en_lista(campos_objeto, s)
        return campos_objeto, s #?? devuelve a: funcion en upsert
    elif opc_elec == 2 and status_query == 'insert':
        return campos_objeto, s #?? devuelve a: funcion en upsert
    
#&& ---------------- FIN OBTIENE FILAS DE SALESFORCE ---------------- &&#

#&& ---------------- INSERTAR FILAS DE SALESFORCE A POSTGRESQL ---------------- &&#
def insertar_registros_psql(conexion, sf_registros_pg, objeto_standard, campos_objeto, s):
    cursor = conexion.cursor() # Crear cursor
    
    query_truncate = f"TRUNCATE TABLE etl.{str(objeto_standard).lower()} CONTINUE IDENTITY RESTRICT;"
    cursor.execute(query_truncate)
    conexion.commit()

    query_insert = f"""
    INSERT INTO etl.{str(objeto_standard).lower()} ({str(campos_objeto).lower()}) VALUES ({s});
    """

    cursor.executemany(query_insert, sf_registros_pg)
    cant_registros = int(cursor.rowcount)
    print(f"Se insertaron {cant_registros} filas de Salesforce a postgreSQL, en la tabla {objeto_standard}")
    conexion.commit()
#&& ---------------- FIN INSERTAR FILAS DE SALESFORCE A POSTGRESQL ---------------- &&#

#~ ---------------- FUNCION QUE OBTIENE LOS DATOS PARA LA INSERCION ---------------- ~#
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
#~ ---------------- FIN FUNCION QUE OBTIENE LOS DATOS PARA LA INSERCION ---------------- ~#


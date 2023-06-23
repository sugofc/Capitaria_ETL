from con.connection import sf
from mod.explain import explain_diff_ids
from mod.clean import comparar_ids_psql_sf, sel_list_objeto
from crud.insert_data import sf_obtener_datos, insertar_registros_psql

# el campo IsDeleted no funciona, solo funciona con la API de ETL 

#&& Funcion que permite verificar (en numero) los ID's &&#
def corrige_data_psql(conexion, objeto, objeto_standard):

    # obtiene todos los ID's de PSQL
    cursor = conexion.cursor()
    query_select_psql = f"select id from etl.{objeto_standard}"
    cursor.execute(query_select_psql)
    ids_ps = cursor.fetchall()
    all_ids_psql = [id[0] for id in ids_ps]

    # Obtiene todos los ID's de SF
    query_select_sf = sf().query(f"Select Id from {objeto}") 
    ids_sf = query_select_sf['records']
    all_ids_sf = [id['Id'] for id in ids_sf]
    
    # Compara los IDS
    id_del_psql, id_rest_psql, ids_repetidos, id_del_psql_view, id_rest_psql_view, id_rest_psql_for_sf = comparar_ids_psql_sf(all_ids_psql, all_ids_sf)

    # Elimina los ids de PSQL que se hayan eliminado en SF
    if len(id_del_psql) != 0:
        elimina_registros_psql(conexion, objeto_standard, id_del_psql)
    
    # Restaura los ids en PSQL y que si esitan en SF
    if len(id_rest_psql_for_sf) != 0:
        restaura_registros_psql(conexion, objeto, objeto_standard, id_rest_psql_for_sf)
    
    # Explica en numeros los ID's
    if len(id_del_psql) != 0 or len(id_rest_psql_for_sf) != 0:
        print(explain_diff_ids(id_del_psql, id_rest_psql, ids_repetidos, all_ids_psql, all_ids_sf, objeto, objeto_standard, id_del_psql_view, id_rest_psql_view))
    else:
        print(f"El Objeto {objeto} de SF, se encuentra sincornizado con la tabla {objeto_standard} de PSQL.")
    
#&& FIN Funcion que permite verificar (en numero) los ID's &&#
    
#&& Funcion que permite realizar el ELIMINAR registros de PSQL que no esten en SF &&#
def elimina_registros_psql(conexion, objeto_standard, id_del_psql):
    
    #^ Elimina de PSQL los registros que estan eliminados de SF
    cursor = conexion.cursor()

    s = len(id_del_psql)*" %s,"
    s = s[:len(s)-1]

    query_delete_psql = f"delete from etl.{objeto_standard} where id = %s;"
    
    cursor.executemany(query_delete_psql,id_del_psql)
    conexion.commit()
    
#&& FIN Funcion que permite realizar el ELIMINAR registros de PSQL que NO esten en SF &&#

#&& Funcion que permite realizar RESTAURACION de registros de PSQL que SI esten en SF &&#
def restaura_registros_psql(conexion, objeto, objeto_standard, id_rest_psql_for_sf):

    # Obtenemos los campos, datos y las 's' de solo los ids que queremos restaurar    
    data_sf_campos, campos_objeto, s = sf_obtener_datos(objeto, objeto_standard, opc_elec = 4, status_query = id_rest_psql_for_sf)

    # En base a los datos que obtenemos arriba, ahora los insertaremos
    insertar_registros_psql(conexion, data_sf_campos, objeto_standard, campos_objeto, s, opc_elec = 4)

#&& FIN Funcion que permite realizar RESTAURACION de registros de PSQL que SI esten en SF &&#
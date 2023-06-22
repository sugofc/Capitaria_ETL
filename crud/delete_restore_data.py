from con.connection import sf

#~ el campo IsDeleted no funciona, solo funciona con la API de ETL
def psql_verificar_data_eliminada_sf(conexion, objeto, objeto_standard):

    cursor = conexion.cursor()
    query_select_psql = f"select id from etl.{objeto_standard}"
    cursor.execute(query_select_psql)
    ids_ps = cursor.fetchall()
    all_ids_psql = [id[0] for id in ids_ps] # Obtenemos todos los ID's de PSQL

    query_select_sf = sf().query(f"Select Id from {objeto}") 
    ids_sf = query_select_sf['records']
    all_ids_sf = [id['Id'] for id in ids_sf] # Obtenemos todos los ID's de SF
    
    id_del_psql = [id for id in all_ids_psql if id not in all_ids_sf] # Obtenemos los ID's de PSQL que no esten en SF, para saber cual esta eliminado de SF

    id_rest_psql = [id for id in all_ids_sf if id not in all_ids_psql] # Obtenemos los ID's de SF que no esten en PSQL, para asi poder restaurarlo a PSQL

    ids_repetidos = list(set(all_ids_psql) & set(all_ids_sf)) # tambien se puede hacer con un list comprehension

    print(f'\nALL_ID_PSQL\n******\n{all_ids_psql}\n******\nPRINT\n')
    print(f'\nALL_ID_SF\n******\n{all_ids_sf}\n******\nPRINT\n')
    print(f'\nELIMINAR\n******\n{id_del_psql}\n******\nPRINT\n')
    print(f'\nRESTAURAR\n******\n{id_rest_psql}\n******\nPRINT\n')
    
    print(f"""Existen {len(all_ids_psql)} ID's en PSQL
Existen {len(all_ids_sf)} ID's en SF
Hay que eliminar {len(id_del_psql)} de PSQL
Hay que restaurar {len(id_rest_psql)} de PSQL
Hay {len(ids_repetidos)} ID's que se repiten en SF y PSQL"""), breakpoint() #^ TestPrint

# todo HACER NUEVAS FUNCIONES PARA DELETE Y RESTORE
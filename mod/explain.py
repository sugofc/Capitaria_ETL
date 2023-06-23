
#&& Explica las opciones del Menu &&#
def explain_menu(objeto, objeto_standard):
    explain = f"""
Descripción de la opcion:
1) CREAR: Crea la tabla de {objeto} en BD con el nombre {objeto_standard}, los campos creados son seleccionados en un archivo anteriormente.

2) UPSERT: Realiza un Update y in Insert a la tabla {objeto_standard}, en base a la ultima modificacion.

3) TRUNCATE: Se Trunca la tabla y se insertan los datos nuevamente en {objeto_standard}, para eliminar cualquier consistencia de datos.

4) CORREGIR: Elimina regitros de {objeto_standard} que se hayan eliminado de Salesforce y Restaurar registros que se hayan eliminado de {objeto_standard} pero que existen en Salesforce.
    """
    return explain
#&& FIN Explica las opciones del Menu &&#

#&& Explica la diferencia entre los ID's de PSQL y SF &&#
def explain_diff_ids(id_del_psql, id_rest_psql, ids_repetidos, all_ids_psql, all_ids_sf, objeto, objeto_standard, id_del_psql_view, id_rest_psql_view):
    
    
    explain_diff = f"""
"""
    if len(id_del_psql) != 0:
        explain_diff = explain_diff + f"""
Se elimino {len(id_del_psql)} ID's de {objeto_standard} que ¡NO! existe en {objeto}.
Registros Eliminados: {id_del_psql_view}
"""
    if len(id_rest_psql) != 0:
        explain_diff = explain_diff + f"""
Se restauro {len(id_rest_psql)} ID's de {objeto_standard} que ¡SI! existen en {objeto}.
Registros Restaurados: {id_rest_psql_view}
"""
    if len(all_ids_psql) != 0:
        explain_diff = explain_diff + f"""
Existen un total de {len(all_ids_psql)} registros en la tabla {objeto_standard}.
"""
    else:
        f"ALERTA!, La tabla {objeto_standard} se encuentra vacia."
    
    explain_diff = explain_diff + f"""
Existen un total de {len(all_ids_sf)} registros en el objeto {objeto}.
"""

    explain_diff = explain_diff + f"""
Hay un total de {len(ids_repetidos)} ID's que existen tanto en {objeto} como en {objeto_standard}.
"""

    return explain_diff

#&& FINExplica la diferencia entre los ID's de PSQL y SF &&#
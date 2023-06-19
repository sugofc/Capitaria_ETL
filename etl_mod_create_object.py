from etl_mod_connection import sf
from etl_mod_clean import *


#&& Funciones Principales
#~~ Funciones Secundarias (que se llaman dentro de las Funciones Principales)

#&& Obtencion de campos de Salesforce &&##
#^ Funcion para Obtener campos de Salesforce
def sf_generacion_objeto(objeto,objeto_standard):
    campos_objeto = sf().__getattr__(f"{objeto}").describe()['fields']
    
    filas_sf = ",\n".join([f"{str(campo['name']).lower()} {datatype_correct(campo['type'])} {length_correct(campo['length'])}" for campo in campos_objeto if campo['name'] in sel_list_objeto(objeto_standard)])

    cant_campos = filas_sf.count(',')+1

    return filas_sf, cant_campos
#&& FIN Obtencion de campos de Salesforce &&#

#&& Creacion de Objeto de Salesforce en Postgres &&#
def crea_objeto_psql(conexion, sf_filas_pg, cant_campos, objeto_standard):
    cursor = conexion.cursor() # Crear cursor
    
    # Query a postgreSQL
    query = f"""
    DROP TABLE IF exists etl.{objeto_standard};
    CREATE TABLE IF NOT EXISTS etl.{str(objeto_standard).lower()} (
        {sf_filas_pg}
    );
    """

    print(f"Se creo la tabla {objeto_standard} en el esquema etl.\nSe traspasaron {cant_campos} campos de Salesforce a postgreSQL")

    cursor.execute(query) # Ejecuta la query
    conexion.commit() # Cofnirma cambios #! Commentar para no crear
    conexion.close() # Cierra conexion
#&& FIN Creacion de Objeto de Salesforce en Postgres &&#


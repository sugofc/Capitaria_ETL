from con.connection import psql
from mod.clean import sf_objeto_opciones
from crud.create_object import sf_generacion_objeto, crea_objeto_psql
from crud.upsert_data import psql_obtener_ids_y_fecha_maxima_modificacion, buscar_actualizaciones_sf, realizar_upsert_psql
from crud.insert_data import sf_obtener_datos, insertar_registros_psql
from crud.delete_restore_data import psql_verificar_data_eliminada_sf

# print("""
#                         SELECCION DE OBJETO

# 1) Account      2) Subcuenta__c     3) Opportunity__c       4) Transacci_n__c
# 5) Task         6) Event            7) AccountHistory       8) User
# 9) Campaign__c  10) CampaignMember__c
# """)
# objeto, objeto_standard = sf_objeto_opciones(int(input(f"¿Que Objeto deseas crear? (numero): ")))

# print(f"""
# ¿Que desea hacer con el objeto {objeto}?
# 1) Crear tabla {objeto_standard}
# 2) Realizar Upsert (Update+Insert) en {objeto_standard}
# 3) Limpiar e Insertar datos (Truncate + Insert) del {objeto_standard}
# 4) Eliminar datos (Delete) de {objeto_standard} que se hayan eliminado de Salesforce 
# 5) Restaurar datos que se hayan eliminado de {objeto_standard} pero que existen en Salesforce
# """)
# opc_elec = int(input("Que desea hacer? (numero): "))

objeto, objeto_standard, opc_elec = 'Account', 'sf_account', 4
#objeto, objeto_standard, opc_elec = 'Subcuenta__c', 'sf_subcuenta', 3
#objeto, objeto_standard, opc_elec = 'AccountHistory', 'sf_accounthistory', 2

try:
    conexion = psql()

    if conexion: # Verificar si la conexión está abierta
        print("Conexión exitosa a la base de datos PostgreSQL")

        if opc_elec == 1: #* Creacion de Objeto
            # A: Obtiene los campos de SF / B: Crea el Objeto en la BD
            sf_filas_pg, cant_campos = sf_generacion_objeto(objeto,objeto_standard) #~ A
            crea_objeto_psql(conexion, sf_filas_pg, cant_campos, objeto_standard) #~ B
        elif opc_elec == 2: #* Upsert de Data
            # E: Obtiene la fema maxima de la ultima modificacion / F: Obtiene los datos de SF cuando son mayor a la fecha maxima de actualizacion de PSQL
            id_all_psql, date_mod_max_psql = psql_obtener_ids_y_fecha_maxima_modificacion(conexion, objeto_standard) #~ E
            data_sf_upd_new, date_max_mod_psql_to_date_sf = buscar_actualizaciones_sf(date_mod_max_psql, objeto_standard, objeto) #~ F
            realizar_upsert_psql(conexion, data_sf_upd_new, objeto, objeto_standard, date_max_mod_psql_to_date_sf, id_all_psql, opc_elec) #~ G
        elif opc_elec == 3: #* Truncar Tabla e Insrtar toda la Data
            # C: Obtiene los registros de SF / D: Inserta los registros en la BD
            sf_registros_pg, campos_objeto, s = sf_obtener_datos(objeto, objeto_standard, opc_elec) #~ C
            insertar_registros_psql(conexion, sf_registros_pg,objeto_standard, campos_objeto, s) #~ D
        elif opc_elec == 4: #* Eliminar datos de PSQL que no esten en Salesforce
            psql_verificar_data_eliminada_sf(conexion, objeto, objeto_standard)
        conexion.close() # Cierra conexion
    else:
        conexion.rollback() # Regresa todo atras en caso de error
except psql().Error as e:
    print("Error al conectar a la base de datos PostgreSQL: {}".format(e))
finally:
    conexion.close() # Pa que quede bien cerra la wea xD
    print("Conexion cerrada.")
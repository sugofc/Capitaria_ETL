from con.connection import psql
from mod.clean import sf_objeto_opciones
from mod.explain import explain_menu
from crud.create_object import sf_generacion_objeto, crea_objeto_psql
from crud.upsert_data import psql_obtener_ids_y_fecha_maxima_modificacion, buscar_actualizaciones_sf, realizar_upsert_psql
from crud.insert_data import sf_obtener_datos, insertar_registros_psql
from crud.correct_data import corrige_data_psql

# print("""
#                         SELECCION DE OBJETO

# (1) Account      (2) Subcuenta__c     (3) Opportunity__c       (4) Transacci_n__c
# (5) Task         (6) Event            (7) AccountHistory       (8) User
# (9) Campaign__c  (10) CampaignMember__c
# """)
# objeto, objeto_standard = sf_objeto_opciones(int(input(f"¿Que Objeto deseas crear? (numero): ")))

# print(f"""
# ¿Que desea hacer con el objeto {objeto}?
# (0) Explicacion
# (1) Crear tabla {objeto_standard}
# (2) Realizar Upsert (Update+Insert) en {objeto_standard}
# (3) Limpiar e Insertar datos (Truncate + Insert) del {objeto_standard}
# (4) Corregir datos de {objeto_standard}
# """)
# opc_elec = int(input("Que desea hacer? (numero): "))

#? Acceso rapido, comentar todo para arriba excepto los import
objeto, objeto_standard, opc_elec = 'Account', 'sf_account', 4
#objeto, objeto_standard, opc_elec = 'Subcuenta__c', 'sf_subcuenta', 1
#objeto, objeto_standard, opc_elec = 'Opportunity__c', 'sf_opportunity', 1
#objeto, objeto_standard, opc_elec = 'Transacci_n__c', 'sf_transacci_n', 1
#objeto, objeto_standard, opc_elec = 'Task', 'sf_task', 1
#objeto, objeto_standard, opc_elec = 'Event', 'sf_event', 1
#objeto, objeto_standard, opc_elec = 'User', 'sf_user', 1
#objeto, objeto_standard, opc_elec = 'Campaign__c', 'sf_camaign', 1
#objeto, objeto_standard, opc_elec = 'CampaignMember__c', 'sf_campaignmember', 1

#! ALERT: recuerda iniciar la BD en ubuntu XD

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
            data_sf_upd_new = buscar_actualizaciones_sf(date_mod_max_psql, objeto_standard, objeto) #~ F
            realizar_upsert_psql(conexion, data_sf_upd_new, objeto, objeto_standard, id_all_psql, opc_elec) #~ G
        elif opc_elec == 3: #* Truncar Tabla e Insrtar toda la Data
            # C: Obtiene los registros de SF / D: Inserta los registros en la BD
            sf_registros_pg, campos_objeto, s = sf_obtener_datos(objeto, objeto_standard, opc_elec, status_query = None) #~ C
            insertar_registros_psql(conexion, sf_registros_pg,objeto_standard, campos_objeto, s, opc_elec) #~ D
        elif opc_elec == 4: #* Eliminar datos de PSQL que no esten en Salesforce
            corrige_data_psql(conexion, objeto, objeto_standard)
        elif opc_elec == 0: #* Explica las opciones
            print(explain_menu(objeto, objeto_standard))
    else:
        conexion.rollback() # Regresa todo atras en caso de error
except psql().Error as e:
    print("Error al conectar a la base de datos PostgreSQL: {}".format(e))
finally:
    conexion.close() # Pa que quede bien cerra la wea xD
    print("Conexion cerrada.")
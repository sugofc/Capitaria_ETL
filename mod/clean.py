from data.data_field_type import VARCHAR, INT, FLOAT, BOOL, TIMESTAMP, TIME, DATE
from data.data_selected import *

#^ Funciones limpieza salesforce
def datatype_correct(tipo): # Modifica el tipo de dato acorde a PSQL
    return 'varchar' if ['varchar' for x in VARCHAR if x in tipo] == ['varchar'] else 'int' if ['int' for x in INT if x in tipo] == ['int'] else 'float' if ['float' for x in FLOAT if x in tipo] == ['float'] else 'timestamp' if ['timestamp' for x in TIMESTAMP if x in tipo] == ['timestamp'] else 'bool' if ['bool' for x in BOOL if x in tipo] == ['bool'] else 'date' if ['date' for x in DATE if x in tipo] == ['date'] else 'time' if ['time' for x in TIME if x in tipo] == ['time'] else tipo

#^ Funcion limpieza largo
def length_correct(largo): # Modifica el Largo acorde a PSQL
    return '' if largo == 0 else "(255)" if largo > 255 else f"({largo})"

#^ funcion de seleccion de opcion
def sf_objeto_opciones(objeto_opc):
    if objeto_opc == 1: # Account
        opciones = 'Account','sf_account'
    elif objeto_opc == 2: # Subcuenta__c
        opciones = 'Subcuenta__c','sf_subcuenta'
    elif objeto_opc == 3: # Opportunity__c
        opciones = 'Opportunity__c','sf_opportunity'
    elif objeto_opc == 4: # Transacci_n__c
        opciones = 'Transacci_n__c','sf_transacci_n'
    elif objeto_opc == 5: # Task
        opciones = 'Task','sf_task'
    elif objeto_opc == 6: # Event
        opciones = 'Event','sf_event'
    elif objeto_opc == 7: # AccountHistory
        opciones = 'AccountHistory','sf_accounthistory'
    elif objeto_opc == 8: # User
        opciones = 'User','sf_user'
    elif objeto_opc == 9: # Campaign__c
        opciones = 'Campaign__c','sf_campaign'
    elif objeto_opc == 10: # CampaignMember__c
        opciones = 'CampaignMember__c','sf_campaignmember'
    else:
        opciones = objeto_opc,'Numero Invalido'
    
    return opciones
    

#^ funcion de seleccionar campos de la lista data selected
def sel_list_objeto(objeto_standard):    
    return sf_account if objeto_standard == 'sf_account' else sf_subcuenta if objeto_standard == 'sf_subcuenta' else sf_opportunity if objeto_standard == 'sf_opportunity' else sf_transacci_n if objeto_standard == 'sf_transacci_n' else sf_task if objeto_standard == 'sf_task' else sf_event if objeto_standard == 'sf_event' else sf_accounthistory if objeto_standard == 'sf_accounthistory' else sf_campaign if objeto_standard == 'sf_campaign' else sf_campaignmember if objeto_standard == 'sf_campaignmember' else sf_user if objeto_standard == 'sf_user' else 'error'

#^ Estandarizar fecha SF a fecha PSQL - formato YYYY-MM-DD HH:MM:SS
def standard_fix_date_sf(date_sf):
    
    return date_sf[:str(date_sf).rfind('T')]+" "+date_sf[str(date_sf).rfind('T')+1:-9]
    

#^ Estandarizar fecha Maxima de PSQL a SF - formato YYYY-MM-DDTHH:MM:SS.000Z
def date_standard_sf(data_max_psql):
    return data_max_psql[:str(data_max_psql).rfind(' ')]+'T'+data_max_psql[str(data_max_psql).rfind(' ')+1:]+'.000Z'

#^ Se pasa el ID al final en una tupla, para poder actualizar
def cambiar_id_al_final(data_sf):
    data_sf = list(data_sf) # transformo la tupla sf, a lista
    id = data_sf.pop(0) # le quito el id y almaceno el indice 0 que es el 'Id' en variable id
    data_sf.append(id) # le agrego el id al final de la lista
    data_sf = tuple(data_sf) # transformo la lista a tupla

    return data_sf

#^ Quito el ID de los campos y un '%s', ademas lo dejo en lista, para poder actualizar
def quitar_id_s_dejarlo_en_lista(campos_objeto, s):
    campos_objeto = campos_objeto.split(',') # lo transformo a lista
    campos_objeto.pop(0) # le quito el indice
    s = s[:len(s)-4] # le quito la ultimo '%s,
    s = s.split(' ')

    return campos_objeto, s

#^ Obtiene las comparaciones de ID's de SF y PSQL
def comparar_ids_psql_sf(all_ids_psql, all_ids_sf):
    
    #* id_del_psql y id_rest_psql, se transforman en tuplas de 1 valor, dentro de listas para poder ocuparlas en ejecucion de PSQL
    # Obtenemos los ID's de PSQL que no esten en SF, para saber cual esta eliminado de SF
    id_del_psql = ids_listatupla([id for id in all_ids_psql if id not in all_ids_sf])
    # Obtenemos los ID's de SF que no esten en PSQL, para asi poder restaurarlo a PSQL
    id_rest_psql = ids_listatupla([id for id in all_ids_sf if id not in all_ids_psql])
    # Los 'ids_repetidos' tambien se puede hacer con un list comprehension
    ids_repetidos = list(set(all_ids_psql) & set(all_ids_sf))

    #* Listas para mostrar
    id_del_psql_view = ", ".join([id for id in all_ids_psql if id not in all_ids_sf])
    id_rest_psql_view = ", ".join([id for id in all_ids_sf if id not in all_ids_psql])
    
    #* Obtiene los ids para consultar en SF
    id_rest_psql_for_sf = ", ".join([f"'{id}'" for id in all_ids_sf if id not in all_ids_psql])

    return id_del_psql, id_rest_psql, ids_repetidos, id_del_psql_view, id_rest_psql_view, id_rest_psql_for_sf

#^ list -> tupla -> lista(tupla()) = Constructor de cadenas para PSQL
def ids_listatupla(lista_a_tupla):
    lista = []
    for n, x in enumerate(lista_a_tupla):
        lista.append([])
        lista[n] = (x,)

    return lista

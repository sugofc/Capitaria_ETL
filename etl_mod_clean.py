from etl_mod_data_type import VARCHAR, INT, FLOAT, BOOL, TIMESTAMP, TIME, DATE
from etl_mod_data_selected import *

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
    elif objeto_opc == 8: # Campaign__c
        opciones = 'Campaign__c','sf_campaign'
    elif objeto_opc == 9: # CampaignMember__c
        opciones = 'CampaignMember__c','sf_campaignmember'
    else:
        opciones = objeto_opc,'Numero Invalido'

    return opciones

#^ funcion de seleccionar campos de la lista data selected
def sel_list_objeto(objeto_standard):    
    return sf_account if objeto_standard == 'sf_account' else sf_subcuenta if objeto_standard == 'sf_subcuenta' else sf_opportunity if objeto_standard == 'sf_opportunity' else sf_transacci_n if objeto_standard == 'sf_transacci_n' else sf_task if objeto_standard == 'sf_task' else sf_event if objeto_standard == 'sf_event' else sf_accounthistory if objeto_standard == 'sf_accounthistory' else sf_campaign if objeto_standard == 'sf_campaign' else sf_campaignmember if objeto_standard == 'sf_campaignmember' else 'error'

#^ Estandarizar fecha SF a fecha PSQL - formato YYYY-MM-DD HH:MM:SS
def standard_fix_date_sf(date_sf):
    
    return date_sf[:str(date_sf).rfind('T')]+" "+date_sf[str(date_sf).rfind('T')+1:-9]
    

#^ Estandarizar fecha Maxima de PSQL a SF - formato YYYY-MM-DDTHH:MM:SS.000Z
def date_standard_sf(data_max_psql):
    return data_max_psql[:str(data_max_psql).rfind(' ')]+'T'+data_max_psql[str(data_max_psql).rfind(' ')+1:]+'.000Z'
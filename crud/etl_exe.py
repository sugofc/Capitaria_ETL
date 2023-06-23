# Ejecuta etl.py que esta en la raiz, se debe llamar
#! from etl_exe import exe_main_etl as etl
#! etl()
# Con lo comentado en rojo, se pega en cualquier archivo de la carpeta CRUD al inicio
# Teniendo las 2 lineas rojas en cualquier archivo de CRUD, no permitira ir a ejecutar el archivo etl.py ya que cae en un loop


def exe_main_etl():
    import os
    import sys
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
    sys.path.append(parent_dir)
    
    import etl


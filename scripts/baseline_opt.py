"""
Código para ejecutar la optimización del baseline
"""
from output import *
import time
from limpieza_masters import limpieza_data


start_time = time.time()
sheet_names = ['master_producto', 'master_ubicaciones', 'master_demanda',
               'master_tarifario', 'master_red_infraestructura']
data = limpieza_data('input/datamaster_base_opt.xlsx', sheet_names)

output = [i for i in ejecucion(data)]
output_names = ['base_opt_decision_consolidado.csv']
output_path = 'output/'

# Guardar output
guardar_outputs([output[0]], [output_names[0]], output_path)
print(f"Tiempo total de ejecucion: {time.time() - start_time} segundos\n",
      f"Costo total operacion {output[2]} COP")
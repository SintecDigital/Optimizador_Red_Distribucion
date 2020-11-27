from output import *
import time

start_time = time.time()
data = carga_datos()
output_list = [i for i in ejecucion(data)]
output_names = ['decision_consolidado.csv', 'restriccion_consolidado.csv', 'costo_mensual.csv']
output_path = 'output/'


guardar_outputs([output_list[0]], [output_names[0]], output_path)
print(f"Tiempo total de ejecucion: {time.time() - start_time} segundos\n",
      f"Costo total operacion {output_list[2]} COP")

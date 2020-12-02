"""
Código para crear las variables de decisión del baseline
"""
import pandas as pd
import time
from baseline_ajustes import variables_decision_nacional, variables_decision_exp
from limpieza_masters import limpieza_data, ajustar_tarifario
from output import guardar_outputs

# Carga de datos. Retorna diccionario de DFs
start_time = time.time()
sheet_names = ['master_demanda', 'master_tarifario', 'master_homologacion']
data = limpieza_data('input/datamaster_baseline.xlsx', sheet_names, is_baseline=True)


# Ejecutar funciones de Baseline. Tener en cuenta que Nal y Exp vienen en la misma hoja de calculo
## Nacional
demanda_nal = data['master_demanda'].loc[data['master_demanda']['id_ciudad_origen'] != 'CGNA_PORT']
demanda_exp = data['master_demanda'].loc[data['master_demanda']['id_ciudad_origen'] == 'CGNA_PORT']

# Validamos que el tarifario no tenga duplicados
data['mater_tarifario'] = ajustar_tarifario(data['master_tarifario'])
decision_nal, demanda_nal_omitida = variables_decision_nacional(demanda_nal, data['master_tarifario'],
                                                                data['master_homologacion'])
decision_exp, demanda_exp_omitida = variables_decision_exp(demanda_exp,data['master_tarifario'], factor_eficiencia=0.78)

# Concatenar archivos de decision y guardar decision en output/
pd.concat([decision_nal, decision_exp]).to_csv('output/baseline_decision_consolidado.csv')

# Guardar demanda omitida en rfi/
omitidos = [demanda_nal_omitida, demanda_exp_omitida]
omitidos_nombres = ['baseline_nacional_omitido.csv', 'baseline_exportacion_omitido.csv']
guardar_outputs(omitidos, omitidos_nombres, output_path='input/')
print("Archivo de decisión generado para Baseline")

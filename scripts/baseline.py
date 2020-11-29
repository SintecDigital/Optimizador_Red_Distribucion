import pandas as pd
from baseline_ajustes import *
from output import guardar_outputs

# Carga de datos. Retorna diccionario de DFs
start_time = time.time()
sheet_names = ['master_demanda', 'master_tarifario', 'master_homologacion']
data = limpieza_data('input/datamaster_baseline.xlsx', sheet_names)


# Ejecutar funciones de Baseline. Tener en cuenta que Nal y Exp vienen en la misma hoja de calculo
## Nacional
demanda_nal = data['master_demanda'].loc[data['master_demanda']['id_ciudad_origen'] != 'CGNA_PORT']
demanda_exp = data['master_demanda'].loc[data['master_demanda']['id_ciudad_origen'] == 'CGNA_PORT']
decision_nal, demanda_nal_omitida = variables_decision_nacional(demanda_nal, data['master_tarifario'],
                                                                data['master_homologacion'])
decision_exp, demanda_exp_omitida = variables_decision_exp(demanda_exp, data['master_tarifario'], factor_eficiencia=0.85)

# Concatenar archivos de decision y guardar decision en output/
pd.concat([decision_nal, decision_exp]).to_csv('output/baseline_decision_consolidado.csv')

# Guardar demanda omitida en rfi/
omitidos = [demanda_nal_omitida, demanda_exp_omitida]
omitidos_nombres = ['nacional_omitido.csv', 'exportacion_omitido.csv']
guardar_outputs(omitidos, omitidos_nombres, output_path='rfi/')
print("Archivo de decisión generado para Baseline")

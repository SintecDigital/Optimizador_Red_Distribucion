import pandas as pd
from baseline_ajustes import *
from output import guardar_outputs

# Abrir datos de RFI para observar comportamiento de Esenttia y diccionario de mapeo
t1_rfi = pd.read_csv('../rfi/t1_rfi.csv')
t2_rfi = pd.read_csv('../rfi/t2_rfi.csv')
apoyo_t2 = pd.read_csv('../rfi/apoyo_t2_rfi.csv')
exp_rfi = pd.read_csv('../rfi/exp_rfi.csv')
sku_dict = pd.read_excel('../rfi/diccionario_sku_familia.xlsx')

# Abrir masters
tarifario = pd.read_csv('../input/master_tarifario.csv')

# Ejecutar funciones de baseline
t1_df, t1_omitida = t_decision(data=t1_rfi, fam_dict=sku_dict, tarifario=tarifario, tipo='t1', year=2019)
t2_df, t2_omitida = t_decision(data=t2_rfi, fam_dict=sku_dict, tarifario=tarifario, apoyo_t2=apoyo_t2, tipo='t2', year=2019)
exp_df, exp_omitida = exp_decision(exp=exp_rfi, tarifario=tarifario, factor_eficiencia=0.85, year=2019)

# Concatenar archivos de decision y guardar decision en output/
pd.concat([t1_df, t2_df, exp_df]).to_csv('../output/baseline_decision_consolidado.csv')

# Guardar demanda omitida en rfi/
omitidos = [t1_omitida, t2_omitida, exp_omitida]
omitidos_nombres = ['t1_omitido.csv', 't2_omitido.csv', 'exp_omitido.csv']
guardar_outputs(omitidos, omitidos_nombres, output_path='../rfi/')

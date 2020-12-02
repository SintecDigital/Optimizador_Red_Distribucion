"""
Este script crea el master a partir del RFI. Estará almacenado en una carpeta oculta con el fin de dejar transparencia en
cómo se limpiaron los archivos. Este archivo toma los datos almacenados en el RFI y guarda un archivo nuevo en el input.
Este script no se usa por el cliente, es para tener trazabilidad de cómo se construyó el baseline. Los paths de este
script son diferentes a los que estamos usando dado que este  código no correrá desde Powershell ni desde la interfaz.
Este código se corre internamente.
"""
import pandas as pd
from limpieza_masters import remover_tildes_espacios


def limpieza_nacional(data, fam_dict, tipo: str):
    """
    Limpia datos de T1 y T2 de RFI, dejándolos en un formato similar para luego concatenar.

    :param data: datos de demanda limpios, con estructura de master demanda
    :param fam_dict: Diccionario de familias y SKUs
    :param apoyo_t2: Tabla de homologación de destinos de T2
    :param tarifario: Tarifario de transporte
    :param tipo: indicar si es 't1' o 't2'
    """

    # Eliminar filas con NaN en origen, destino, sku
    data = data.dropna(subset=['id_ciudad_origen', 'id_ciudad_destino', 'sku'])

    # Configurar columna fecha a formato fecha, crear col fecha con año-mes. Filtrar solo 2019
    data = data.astype({'fecha': 'datetime64'})
    data['fecha'] = data['fecha'].dt.strftime('%Y-%m')
    data = data.loc[data['fecha'].str[:4] == '2019']

    # Filtrar nacional en t2, cambiar origen CGNA a CGNA_CEDI para t2 y CGNA_PLANT para t1 y poner cantidades en
    # toneladas positivas
    if tipo == 't2':
        data['nacional_exportacion'] = remover_tildes_espacios(data['nacional_exportacion'])
        data = data.loc[data['nacional_exportacion'] == 'NACIONAL']
        data['cantidad'] /= 1000
        data['id_ciudad_origen'] = data['id_ciudad_origen'].str.replace('CGNA', 'CGNA_CEDI')
    elif tipo == 't1':
        data['cantidad'] /= -1000
        data['id_ciudad_origen'] = data['id_ciudad_origen'].str.replace('CGNA', 'CGNA_PLANT')
    else:
        pass

    # Conteo inicial de demanda para saber cuánto se descartará
    demanda_inicial = data['cantidad'].sum()

    # Ajustar destino CGNA a CGNA_CEDI
    data['id_ciudad_destino'] = data['id_ciudad_destino'].str.replace('CGNA', 'CGNA_CEDI')

    # Seleccionar solo columnas relevantes
    data = data.loc[:, ['fecha', 'id_ciudad_origen', 'id_ciudad_destino', 'sku', 'cantidad']]

    # Unir familias a data de demanda
    data = data.merge(fam_dict, how='left', on='sku')
    data_omitida = data.loc[data['familia'].isna()]
    data = data.loc[~data['familia'].isna()]

    # Agrupar datos
    data = data.groupby(['fecha', 'familia', 'id_ciudad_origen', 'id_ciudad_destino'])['cantidad'].sum().reset_index()

    # Limpiar textos
    for col in ['id_ciudad_origen', 'id_ciudad_destino', 'familia']:
        data[col] = remover_tildes_espacios(data[col])

    return data, data_omitida


def variables_decision_nacional(data, tarifario, apoyo_t2):
    """
    A partir de un histórico estandarizado se construye el baseline. En esta función se toman en cuenta unos valores de
    eficiencia que hacen que la operación suba o baje de precio. Dichos pesos están en el dict `pesos_camion_dict`
    :param data:
    :param tarifario:
    :param apoyo_t2:
    :return:
    """
    # Conteo inicial de demanda para saber cuánto se descartará
    demanda_inicial = data['cantidad'].sum()

    ## CALCULO variables decision ##
    # Lograr poner en tarifario diccionario de porcentajes de uso. La idea es que en el tarifario podamos poner estos
    # pesos. Para ello, primero hay que filtrar las tarifas que son nacionales y omitir aquellas que tienen un 999.
    # Luego, haremos una variación en el diccionario de pesos usado en el tarifario. Habían cinco casos, donde dos de
    # ellos (que haya solo dos tarifas aunque de diferentes pesos) son iguales en la cantidad de tarifas disponibles.
    # El cambio es el siguiente. Si hay solo 1 transporte, va 100%; si hay dos, va el 95% en el más alto y 5%; si hay
    # tres, va 87%, 5%, 8%; y si hay cuatro, van 92%, 3%, 1%, 4%.

    tarifario = tarifario.loc[(tarifario['id_ciudad_origen'].isin(['MB_PLANT', 'CGNA_PLANT', 'CGNA_CEDI', 'ABOD','BBOD',
                                                            'CBOD', 'BUEN_PORT'])) & (tarifario['capacidad'] != 999)]
    pesos_camion_dict = {1: [1.05], 2: [0.9, 0.1], 3: [0.80, 0.05, 0.15], 4: [0.8, 0.03, 0.01, 0.16]}

    # Hacer un groupby provisional para saber cuántos transportes hay por rutas. Luego lo unimos con tarifario organizado por alfabeto
    tarifario_group = tarifario.groupby(['id_ciudad_origen', 'id_ciudad_destino'])['capacidad'].count().reset_index()
    tarifario_group = tarifario_group.rename(columns={'capacidad': 'conteo'})
    tarifario = tarifario.sort_values(['id_ciudad_origen', 'id_ciudad_destino', 'capacidad'],
                                      ascending=[True, True, False])

    # Unir conteo con tarifario. Teniendo conteo, agregar valores de pesos de camiones en tasa_util
    tarifario = tarifario.merge(tarifario_group, how='left', on=['id_ciudad_origen', 'id_ciudad_destino'])
    del (tarifario_group)
    tarifario = tarifario.loc[:, ['id_ciudad_origen', 'id_ciudad_destino', 'capacidad', 'costo', 'conteo']]
    tarifario['tasa_util'] = 0
    for i in tarifario['conteo'].unique():
        tarifario.loc[tarifario['conteo'] == i, 'tasa_util'] = pesos_camion_dict[i] * int(tarifario.loc[tarifario['conteo'] == i, 'conteo'].shape[0] / i)

    # Calcular costo de almacenamiento
    almacenamiento = data.loc[data['id_ciudad_destino'].isin(['ABOD', 'BBOD', 'CBOD', 'CGNA_CEDI'])]
    almacenamiento.loc[:, 'id_ciudad_destino'] = almacenamiento['id_ciudad_destino'] + '_ALMACENAMIENTO'
    almacenamiento.loc[:, 'capacidad'] = 1
    almacenamiento.loc[:, 'costo'] = 34401 * 1.105
    almacenamiento['valor_decision'] = almacenamiento.loc[:, 'cantidad']

    # Cruzar demanda con tarifario, cruzar homologacion, guardar omitidos
    data = data.merge(tarifario, how='left', on=['id_ciudad_origen', 'id_ciudad_destino'])

    # Usar tabla de Homologación para T2
    # data_t2 = data.loc[data['tipo'] == 't2']
    data_homologar = data.loc[data['capacidad'].isna()].drop(columns=['capacidad', 'costo', 'conteo', 'tasa_util'])
    data_homologar = data_homologar.merge(apoyo_t2, on='id_ciudad_destino', how='left')
    data_homologar = data_homologar.drop(columns=['id_ciudad_destino'])
    data_homologar = data_homologar.rename(columns={'id_ciudad_destino_homologado': 'id_ciudad_destino'})
    data_homologar = data_homologar.merge(tarifario, on=['id_ciudad_origen', 'id_ciudad_destino'], how='left')
    data_omitida = data_homologar.loc[data_homologar['capacidad'].isna()]
    data = pd.concat([data, data_homologar.loc[:, data.columns]], ignore_index=True)
    # else:
    # data_omitida = data.loc[data['capacidad'].isna()]
    data = data.loc[~data['capacidad'].isna()]
    # Calcular variable de decisión, que equivale a (tasa_util * cantidad) / capacidad
    data['valor_decision'] = (data['tasa_util'] * data['cantidad']) / data['capacidad']
    demanda_final = (data['cantidad'] * data['tasa_util']).sum()

    # Preparar output para que tenga misma estructura de procedimiento optimizado, mostrar demanda preliminar y final
    data = pd.concat([data, almacenamiento], ignore_index=True)
    data = data.rename(columns={'fecha': 'tiempo', 'familia': 'producto', 'capacidad': 'transporte',
                                'id_ciudad_origen': 'origen', 'id_ciudad_destino': 'destino'})
    data = data.loc[:, ['tiempo', 'producto', 'transporte', 'origen', 'destino', 'costo', 'valor_decision']]
    print(f"Habían {demanda_inicial} toneladas y ahora hay {demanda_final} en Nacional")

    return data, data_omitida


def limpieza_exp(exp):

    # Crear id_ciudad_origen para poder cruzar con tarifario
    exp['id_ciudad_origen'] = 'CGNA_PORT'

    # Borrar filas con NaN. Filtrar año
    exp = exp.loc[exp['año'] == 2019]
    exp = exp.dropna(subset=['familia', 'id_ciudad_destino', 'cantidad'])

    #  Limpiar campos de texto de EXP
    exp['id_ciudad_destino'] = remover_tildes_espacios(exp['id_ciudad_destino'])
    exp['familia'] = remover_tildes_espacios(exp['familia'])

    # Crear columna de fecha para que sea similar a los datos nacionales
    exp['fecha'] = exp['año'].astype(str) + '-' + exp['mes'].astype(str).str.pad(2, fillchar='0')

    # Agrupar por columnas relevantes
    exp = exp.groupby(['fecha', 'familia', 'id_ciudad_origen', 'id_ciudad_destino'])['cantidad'].sum().reset_index()

    return exp


def variables_decision_exp(exp, tarifario, factor_eficiencia):
    """
    Cálculo de variables de decisión de demanda exportación desde archivo de exportacion

    :param exp: Datos de exportacion
    :param tarifario:
    :param factor_eficiencia: En Baseline los contenedores no siempre iban llenos, así que aplicaremos un factor de
    eficiencia para replicar situación de exportacion real.
    :return:
    """
    # Crear conteo de demanda de exportación inicial
    demanda_inicial = exp['cantidad'].sum()

    # Unir con tarifario. Tener en cuenta que aquí no se hace optimización, así que no pueden haber varios tarifarios
    # para el mismo destino. Tomamos la capacidad más grande por destino
    tarifario = tarifario.sort_values('capacidad').drop_duplicates(['id_ciudad_origen',
                                                                    'id_ciudad_destino'], keep='last')

    exp = exp.merge(tarifario, how='left', on=['id_ciudad_origen', 'id_ciudad_destino'])
    exp_omitida = exp.loc[exp['capacidad'].isna()]
    exp = exp.loc[~exp['capacidad'].isna()]

    # Calcular variable de decisión, que equivale a (cantidad / (capacidad * factor_eficiencia))
    exp['valor_decision'] = exp['cantidad'] / (exp['capacidad'] * factor_eficiencia)

    # Mostrar demanda final
    print(f"Habían {demanda_inicial} toneladas y ahora hay {exp['cantidad'].sum()} en Exportación")

    # Preparar output para que tenga misma estructura de procedimiento optimizado
    exp = exp.rename(columns={'fecha': 'tiempo', 'familia': 'producto', 'capacidad': 'transporte',
                              'id_ciudad_origen': 'origen', 'id_ciudad_destino': 'destino'})
    exp = exp.loc[:, ['tiempo', 'producto', 'transporte', 'origen', 'destino', 'costo', 'valor_decision']]

    return exp, exp_omitida


if __name__ == '__main__':
    t1 = pd.read_csv('../rfi/t1_rfi.csv')
    t2 = pd.read_csv('../rfi/t2_rfi.csv')
    exp = pd.read_csv('../rfi/exp_rfi.csv')
    dict_sku_fam = pd.read_excel('../rfi/diccionario_sku_familia.xlsx')
    tarifario = pd.read_csv('../rfi/master_tarifario.csv')
    apoyo_t2 = pd.read_csv('../rfi/apoyo_t2_rfi.csv')

    t1_limpio, t1_omitido = limpieza_nacional(t1, dict_sku_fam, tipo='t1')  # Omitido es familias en blanco
    t2_limpio, t2_omitido = limpieza_nacional(t2, dict_sku_fam, tipo='t2')
    exp_limpio = limpieza_exp(exp)

    demanda_concat = pd.concat([t1_limpio, t2_limpio, exp_limpio], ignore_index=True)

    # El baseline se compone de los datos de demanda, de tarifario, y de apoyo_t2. Pondremos esos tres datos en el mismo
    # archivo de Excel
    with pd.ExcelWriter('../input/datamaster_baseline.xlsx') as writer1:
        demanda_concat.to_excel(writer1, sheet_name='master_demanda', index=False)
        tarifario.to_excel(writer1, sheet_name='master_tarifario', index=False)
        apoyo_t2.to_excel(writer1, sheet_name='master_homologacion', index=False)

    # Baseline omitido preliminarmente. Lo guardaremos en la carpeta de RFI
    pd.concat([t1_omitido, t2_omitido], ignore_index=True).to_csv('../rfi/baseline_demanda_omitida.csv')


import pandas as pd
from limpieza_masters import remover_tildes_espacios


def exp_decision(exp, tarifario, factor_eficiencia=0.85, year=2019):
    """
    Limpia y ajusta los datos de demanda exportación RFI para Baseline

    :param exp: Datos de exportacion
    :param tarifario:
    :param factor_eficiencia: En Baseline los contenedores no siempre iban llenos, así que aplicaremos un factor de eficiencia para replicar
    situación de exportacion real.
    :param year: año a analizar
    :return:
    """

    # Crear conteo de demanda de exportación inicial
    demanda_inicial = exp['cantidad'].sum()
    # Crear id_ciudad_origen para poder cruzar con tarifario
    exp['id_ciudad_origen'] = 'CGNA_PORT'

    # Borrar filas con NaN. Filtrar año
    exp = exp.loc[exp['año'] == year]
    exp = exp.dropna(subset=['familia', 'id_ciudad_destino', 'cantidad'])

    #  Limpiar campos de texto de EXP
    exp['id_ciudad_destino'] = remover_tildes_espacios(exp['id_ciudad_destino'])
    exp['familia'] = remover_tildes_espacios(exp['familia'])

    # Agrupar por columnas relevantes
    exp = exp.groupby(['mes', 'familia', 'id_ciudad_origen', 'id_ciudad_destino'])['cantidad'].sum().reset_index()

    # Unir con tarifario. Usaremos solo capacidad 27 para este escenario
    tarifario = tarifario.loc[(tarifario['id_ciudad_origen'] == 'CGNA_PORT') & (tarifario['capacidad'] == 40)]
    exp = exp.merge(tarifario, how='left', on=['id_ciudad_origen', 'id_ciudad_destino'])
    exp_omitida = exp.loc[exp['capacidad'].isna()]
    exp = exp.loc[~exp['capacidad'].isna()]

    # Calcular variable de decisión, que equivale a (cantidad / (capacidad * factor_eficiencia))
    exp['valor_decision'] = exp['cantidad'] / (exp['capacidad'] * factor_eficiencia)

    # Mostrar demanda final
    print(f"Habían {demanda_inicial} toneladas y ahora hay {exp['cantidad'].sum()}")

    # Preparar output para que tenga misma estructura de procedimiento optimizado
    exp = exp.rename(columns={'mes': 'tiempo', 'familia': 'producto', 'capacidad': 'transporte',
                              'id_ciudad_origen': 'origen', 'id_ciudad_destino': 'destino'})
    exp = exp.loc[:, ['tiempo', 'producto', 'transporte', 'origen', 'destino', 'costo', 'valor_decision']]

    return exp, exp_omitida


def t_decision(data,tarifario, fam_dict, tipo: str, apoyo_t2=None, year=2019):
    """
    Limpia datos de T1 y T2 de RFI. Tambien calcula las variables de decisión

    :param data: datos de demanda limpios, con estructura de master demanda
    :param fam_dict: Diccionario de familias y SKUs
    :param apoyo_t2: Tabla de homologación de destinos de T2
    :param tarifario: Tarifario de transporte
    :param tipo: indicar si es 't1' o 't2'  # TODO: vale la pena unir t1 y t2 y poner una columna que indique cuál es.
    """

    # Eliminar filas con NaN en origen, destino, sku
    data = data.dropna(subset=['id_ciudad_origen', 'id_ciudad_destino', 'sku'])

    # Configurar columna fecha a formato fecha, crear col de año y mes. Filtrar año en cuestión
    data = data.astype({'fecha': 'datetime64'})
    data['año'] = data['fecha'].dt.year
    data['mes'] = data['fecha'].dt.month
    data = data.loc[data['año'] == year]

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

    # Conteo inicial de demanda para saber cuánto se descartó
    demanda_inicial = data['cantidad'].sum()

    # Ajustar destino CGNA a CGNA_CEDI
    data['id_ciudad_destino'] = data['id_ciudad_destino'].str.replace('CGNA', 'CGNA_CEDI')

    # Seleccionar solo columnas relevantes
    data = data.loc[:, ['mes', 'id_ciudad_origen', 'id_ciudad_destino', 'sku', 'cantidad']]

    # Unir familias a data de demanda
    data = data.merge(fam_dict, how='left', on='sku')
    data_omitida = data.loc[data['familia'].isna()]
    data = data.loc[~data['familia'].isna()]

    # Agrupar datos
    data = data.groupby(['mes', 'familia', 'id_ciudad_origen', 'id_ciudad_destino'])['cantidad'].sum().reset_index()

    # Limpiar textos
    for col in ['id_ciudad_origen', 'id_ciudad_destino', 'familia']:
        data[col] = remover_tildes_espacios(data[col])

    ## CALCULO variables decision ##
    # Lograr poner en tarifario diccionario de porcentajes de uso. La idea es que en el tarifario podamos poner estos pesos. Para ello,
    # primero hay que filtrar las tarifas que son nacionales y omitir aquellas que tienen un 999. Para ello haremos una variación en
    # el diccionario de pesos usado en el tarifario. Habían cinco casos, donde dos de ellos (que haya solo dos tarifas aunque de diferentes
    # pesos), son iguales en la cantidad de tarifas disponibles. El cambio es el siguiente. Si hay solo 1 transporte, va 100%; si hay dos,
    # va el 95% en el más alto y 5%; si hay tres, va 90%, 5%, 5%; y si hay cuatro, van 95%, 3%, 1%, 1%
    tarifario = tarifario.loc[(tarifario['id_ciudad_origen'].isin(['MB_PLANT', 'CGNA_PLANT', 'CGNA_CEDI', 'ABOD', 'BBOD',
                                                                   'CBOD', 'BUEN_PORT'])) & (tarifario['capacidad'] != 999)]
    pesos_camion_dict = {1: [1], 2: [0.95, 0.05], 3: [0.9, 0.05, 0.05], 4: [0.95, 0.03, 0.01, 0.01]}

    # Hacer un groupby provisional para saber cuántos transportes hay por rutas. Luego lo unimos con tarifario organizado por alfabeto
    tarifario_group = tarifario.groupby(['id_ciudad_origen', 'id_ciudad_destino'])['capacidad'].count().reset_index()
    tarifario_group = tarifario_group.rename(columns={'capacidad': 'conteo'})
    tarifario = tarifario.sort_values(['id_ciudad_origen', 'id_ciudad_destino', 'capacidad'], ascending=[True, True, False])

    # Unir conteo con tarifario. Teniendo conteo, agregar valores de pesos de camiones en tasa_util
    tarifario = tarifario.merge(tarifario_group, how='left', on=['id_ciudad_origen', 'id_ciudad_destino'])
    del(tarifario_group)
    tarifario = tarifario.loc[:, ['id_ciudad_origen', 'id_ciudad_destino', 'capacidad', 'costo', 'conteo']]
    tarifario['tasa_util'] = 0
    for i in tarifario['conteo'].unique():
        tarifario.loc[tarifario['conteo'] == i, 'tasa_util'] = pesos_camion_dict[i] *\
                                                               int(tarifario.loc[tarifario['conteo'] == i, 'conteo'].shape[0] / i)

    # Cruzar demanda con tarifario, cruzar homologacion, guardar omitidos
    data = data.merge(tarifario, how='left', on=['id_ciudad_origen', 'id_ciudad_destino'])

    # Cruzar con tarifario. Usar tabla de Homologación para T2
    if tipo == 't2':
        data_homologar = data.loc[data['capacidad'].isna()].drop(columns=['capacidad', 'costo', 'conteo', 'tasa_util'])
        data_homologar = data_homologar.merge(apoyo_t2, on='id_ciudad_destino', how='left')
        data_homologar = data_homologar.drop(columns=['id_ciudad_destino'])
        data_homologar = data_homologar.rename(columns={'id_ciudad_destino_homologado': 'id_ciudad_destino'})
        data_homologar = data_homologar.merge(tarifario, on=['id_ciudad_origen', 'id_ciudad_destino'], how='left')
        data_omitida = pd.concat([data_omitida, data_homologar[data_homologar['capacidad'].isna()]], ignore_index=True)
        data = pd.concat([data, data_homologar.loc[:, data.columns]], ignore_index=True)
    else:
        data_omitida = pd.concat([data_omitida, data.loc[data['capacidad'].isna()]], ignore_index=True)
    data = data.loc[~data['capacidad'].isna()]

    # Calcular variable de decisión, que equivale a (tasa_util * cantidad) / capacidad
    data['valor_decision'] = (data['tasa_util'] * data['cantidad']) / data['capacidad']
    demanda_final = (data['cantidad'] * data['tasa_util']).sum()

    # Preparar output para que tenga misma estructura de procedimiento optimizado, mostrar demanda preliminar y final
    data = data.rename(columns={'mes': 'tiempo', 'familia': 'producto', 'capacidad': 'transporte',
                              'id_ciudad_origen': 'origen', 'id_ciudad_destino': 'destino'})
    data = data.loc[:, ['tiempo', 'producto', 'transporte', 'origen', 'destino', 'costo', 'valor_decision']]
    print(f"Habían {demanda_inicial} toneladas en {tipo} para {year} y ahora hay {demanda_final}")

    return data, data_omitida

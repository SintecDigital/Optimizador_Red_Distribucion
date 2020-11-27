import pandas as pd
from output import guardar_outputs


def remover_tildes_espacios(series):
    """
    Pone mayúsculas, remueve tildes y espacios al inicio y al final de los strings en una pd.Series
    :param series: pd.Series
    :return:
    """
    series = series.str.upper()
    series = series.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    series = series.str.strip()
    return series


def ajustar_producto(df_producto):
    """
    Ajusta el master de productos
    :param df_producto:
    :return: df_producto: Master productos limpio
    """
    # Reemplazar los NaN en columna ubicacion producto por CGNA_PLANT}
    df_producto['ubicacion_producto'] = df_producto.ubicacion_producto.fillna('CGNA_PLANT')

    # Guardar cuánto hay inicialmente
    filas_df_prod = df_producto.shape[0]

    # Limpiar columnas de texto
    df_producto['familia'] = remover_tildes_espacios(df_producto['familia'])
    df_producto['ubicacion_producto'] = remover_tildes_espacios(df_producto['ubicacion_producto'])

    # Aquello sin ubicacion_producto se le pone CGNA_PLANT. Aquello que diga PLANTA_CARTAGENA, se le pondrá CGNA_PLANT
    df_producto['ubicacion_producto'] = df_producto.ubicacion_producto.str.replace('PLANTA CARTAGENA', 'CGNA_PLANT')
    df_producto['ubicacion_producto'] = df_producto.ubicacion_producto.str.replace('#N/A', 'CGNA_PLANT')

    # Agrupar y decir cuánto salió
    df_producto = df_producto.groupby(['familia', 'ubicacion_producto'])['produccion_max'].mean().reset_index()
    print('Habían %d filas en master_producto y ahora hay %d' % (filas_df_prod, df_producto.shape[0]))


    return df_producto


def ajustar_tarifario(df_tarifario):
    """
    Ajusta el master tarifario. En últimas no es una buena opción dado que lo que hacemos aquí es eliminar duplicados
    a través de un promedio. Se corre el riesgo de afectar una tarifa por alguna entrada errónea
    :param df_tarifario:
    :return:
    """
    # Agrupamos para verificar que tenemos valores únicos, usamos media en caso que hayan valores repetidos
    df_tarifario = df_tarifario.groupby(['id_ciudad_origen', 'id_ciudad_destino', 'capacidad'])[
        'costo'].mean().reset_index()
    df_tarifario['id_ciudad_origen'] = remover_tildes_espacios(df_tarifario['id_ciudad_origen'])
    df_tarifario['id_ciudad_destino'] = remover_tildes_espacios(df_tarifario['id_ciudad_destino'])

    return df_tarifario


# No hay que ajustar master_ubicaciones

def ajustar_demanda(df_demanda, df_producto, df_tarifario):

    # Llevar conteo de demanda inicial para saber cuánto se descartó
    demanda = df_demanda['cantidad'].sum()

    # Quitar tildes y otros elementos de `familia` y `id_ciudad`
    df_demanda['familia'] = remover_tildes_espacios(df_demanda['familia'])
    df_demanda['id_ciudad'] = remover_tildes_espacios(df_demanda['id_ciudad'])

    # Revisar que los destinos y los productos existan en tarifario y master producto
    df_demanda_filtered = df_demanda.loc[df_demanda['familia'].isin(df_producto['familia'].unique())]
    df_demanda_filtered = df_demanda_filtered.loc[df_demanda['id_ciudad'].isin(df_tarifario['id_ciudad_destino'].unique())]

    # Demanda omitida
    df_demanda_omitida = df_demanda.loc[set(df_demanda.index) - set(df_demanda_filtered.index)]
    df_demanda_omitida = df_demanda_omitida.groupby(['año', 'fecha', 'familia', 'id_ciudad'])['cantidad'].sum().reset_index()

    # Agrupar demanda para asegurarnos de tener filar únicas dados los ids
    df_demanda_filtered = df_demanda_filtered.groupby(['año', 'fecha', 'familia', 'id_ciudad'])['cantidad'].sum().reset_index()

    print(f"Cantidad inicial {demanda}, cantidad final {df_demanda_filtered['cantidad'].sum()}")
    return df_demanda_filtered, df_demanda_omitida


def limpieza_data(data_path, sheet_names, is_baseline=False):

    """
    :param data_path: dirección relativa de archivo .xlsx o .xls que contiene la información a limpiar
    :param sheet_names: lista con los nombres de las hojas relevantes
    :param is_baseline: Boolean para determinar si el input es el baseline (que tiene un tratamiento especial)

    :return: datasets: diccionario que contiene todos los masters de datos y demanda omitida para cuando aplique
    """

    # Los guardaremos en un dicccionario con los nombres de cada hoja
    datasets = [pd.read_excel(data_path, sheet_name=i) for i in sheet_names]
    datasets = dict(zip(sheet_names, datasets))

    # Condición para limpiar baseline por separado
    if is_baseline:
        # Dado que solo tenemos dos hojas, deberíamos ponerlas aquí y limpiarlas.
        pass
    else:
        # Limpieza de master_producto, master_tarifario, y master_demanda
        datasets['master_producto'] = ajustar_producto(datasets['master_producto'])
        datasets['master_tarifario'] = ajustar_tarifario(datasets['master_tarifario'])
        datasets['master_demanda'], datasets['demanda_omitida'] = ajustar_demanda(datasets['master_demanda'],
                                                                      datasets['master_producto'],
                                                                      datasets['master_tarifario'])
    return datasets


"""
if __name__ == '__main__':


    # La idea es limpiar los 3 archivos, aunque el baseline se hace de forma separada, ya que tiene hojas diferentes
    DATA_PATH = ['datamaster_baseline.xlsx']

    # Tal vez toque hacer funciones especiales para limpiar el baseline


    DATA_PATH = ['datamaster_base_opt.xlsx', 'datamaster_escenarios.xlsx']
    #DATA_PATH = 'datamaster.xlsx'

    for datamaster in DATA_PATH:
        # Tomamos las hojas relevantes del .xlsx
        DATASET_NAMES = ['master_producto', 'master_ubicaciones', 'master_demanda',
                         'master_tarifario', 'master_red_infraestructura']
        DATASETS = [pd.read_excel(DATA_PATH, sheet_name=i) for i in DATASET_NAMES]

        # Los asignamos a objetos independientes sin razon alguna
        master_producto, master_ubicaciones, master_demanda, master_tarifario, master_red = DATASETS

        # Limpieza de master_producto, master_tarifario, y master_demanda
        producto = ajustar_producto(master_producto)
        tarifario = ajustar_tarifario(master_tarifario)
        demanda, demanda_omitida = ajustar_demanda(master_demanda, producto, tarifario)

        producto.to_csv('input/master_producto.csv', index=False)
        demanda.to_csv('input/master_demanda.csv', index=False)
        demanda_omitida.to_csv('input/demanda_omitida.csv', index=False)
        tarifario.to_csv('input/master_tarifario.csv', index=False)
        master_ubicaciones.to_csv('input/master_ubicaciones.csv', index=False)
        master_red.to_csv('input/master_red_infraestructura.csv', index=False)"""
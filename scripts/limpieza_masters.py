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


def ajustar_producto(df_producto, file_path):
    """
    Ajusta el master de productos
    :param file_path:
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
    print(f'{file_path}\nHabían {filas_df_prod} filas en master_producto y ahora hay {df_producto.shape[0]}')


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
    df_demanda_omitida = df_demanda_omitida.groupby(['fecha', 'familia', 'id_ciudad'])['cantidad'].sum().reset_index()

    # Agrupar demanda para asegurarnos de tener filar únicas dados los ids
    df_demanda_filtered = df_demanda_filtered.groupby(['fecha', 'familia', 'id_ciudad'])['cantidad'].sum().reset_index()

    print(f"Cantidad inicial {demanda}, cantidad final {df_demanda_filtered['cantidad'].sum()}\n")
    return df_demanda_filtered, df_demanda_omitida


def limpieza_data(data_path, sheet_names, is_baseline=False):

    """
    Llama las funciones especializadas de arriba para limpiar los masters.
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
        # Dado que solo tenemos dos hojas, deberíamos ponerlas aquí y limpiarlas. Se itera dos veces. Una por archivo,
        # otra por columnas.
        for df in datasets:
            for col in datasets[df]:
                datasets[df][col] = remover_tildes_espacios(datasets[df][col])
                print('Mayúsculas, espacios extraños, y tildes y caracteres extraños correctamente eliminados')
    else:
        # Limpieza de master_producto, master_tarifario, y master_demanda
        datasets['master_producto'] = ajustar_producto(datasets['master_producto'], data_path)
        datasets['master_tarifario'] = ajustar_tarifario(datasets['master_tarifario'])
        datasets['master_demanda'], datasets['demanda_omitida'] = ajustar_demanda(datasets['master_demanda'],
                                                                      datasets['master_producto'],
                                                                      datasets['master_tarifario'])
    return datasets


if __name__ == '__main__':

    """
    Aquí está el código que se usa al ejecutarse directamente esta función para limpiar las 3 bases de datos a la vez.
    """

    # Al existir la posibilidad de que alguno de los 3 archivos posibles no esté, atrapamos el error
    DATAPATH = ['datamaster_baseline.xlsx', 'datamaster_base_opt.xlsx', 'datamaster_escenario.xlsx']
    sheet_names = ['master_producto', 'master_ubicaciones', 'master_demanda',
                   'master_tarifario', 'master_red_infraestructura']
    baseline_sheet_names = []
    datasets = {}

    for path in DATAPATH:
        try:
            if path == 'datamaster_baseline.xlsx':
                datasets[path] = limpieza_data('input/' + path, baseline_sheet_names, is_baseline=True)
            else:
                datasets[path] = limpieza_data('input/' + path, sheet_names)
                datasets[path]['demanda_omitida'].to_excel('input/' + path[:-5] + '_demanda_omitida.xlsx')
        except FileNotFoundError:
            print(f'El archivo {path} no fue encontrado\n')
            pass

    # No hay necesidad de guardar los archivos para limpieza, de hecho. Con solo ejecutarlos ya es suficiente.

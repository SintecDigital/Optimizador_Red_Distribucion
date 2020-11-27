import pandas as pd
import numpy as np


def build_items(master_red: pd.DataFrame, master_ubicaciones: pd.DataFrame, master_demanda, master_producto):
    """
    Crea un df de items con 5 columnas donde se especifica tiempo, producto, nodo, tipo, y valor. Estamos
    ignorando material importado, ya que toca hacer cambios a la tabla de ubicación para agregar a CGNA_PLANT como
    CGNA_PLANT_DISTR

    :param master_producto:
    :param master_demanda:
    :param master_ubicaciones:
    :param master_red:
    :return:
    """

    # De hecho, se debe crear primero la sección de restricciones estáticas y dinámicas, ya que no dependen de producto.
    # Delimitar cantidad de tiempo
    MONTHS = sorted(master_demanda['fecha'].unique())

    # Nodos totales y únicos de la red
    nodos = pd.concat([master_red.loc[:, 'id_locacion_origen'], master_red.loc[:, 'id_locacion_destino']],
                      ignore_index=True).unique()

    # Creamos DF final que tiene estructura definida en documentacion: `tiempo`, `producto`, `nodo`, `tipo`, `valor`
    item_df = pd.DataFrame(columns=['tiempo', 'producto', 'nodo', 'tipo', 'valor'])
    for t in MONTHS:

        # RESTR DINAMICA Y ESTATICA: Extraemos restricciones dinámicas y estáticas y lo ponemos en formato de `item_df`
        nodos_restr = master_ubicaciones.loc[:, ['id_locacion', 'capacidad_din', 'capacidad_est']]
        nodos_restr = pd.melt(nodos_restr, id_vars=['id_locacion'], value_vars=['capacidad_din', 'capacidad_est'])
        nodos_restr.columns = item_df.columns[-3:]

        # Borramos las filas que tengan `nodos_restr[valor].isna()`
        nodos_restr = nodos_restr.dropna(subset=['valor'])

        # Añadimos tiempo `t` y producto `NaN` a esas restricciones para que se pueda concatenar a `item_df`
        nodos_restr['tiempo'] = t
        nodos_restr['producto'] = np.nan

        # PRODUCTOS: seleccionamos los productos (familias) del master de demanda para el mes en cuestion
        PRODUCTS = master_demanda.loc[master_demanda['fecha'] == t, 'familia'].unique()
        for k in PRODUCTS:
            # PRODUCCION: Buscamos el sitio origen del producto y su producción máx en master de productos.
            # Debería ser solo UN origen
            nodos_prod = master_producto.loc[master_producto['familia'] == k, ['familia',
                                                                               'ubicacion_producto', 'produccion_max']]
            # Renombrar y agregar columnas de tipo y tiempo
            nodos_prod.columns = ['producto', 'nodo', 'valor']
            nodos_prod['tipo'] = 'produccion'
            nodos_prod['tiempo'] = t

            # DEMANDA: buscar todos los clientes para producto k en tiempo t. Los clientes los tomaremos como ciudades
            clientes_demanda = master_demanda.loc[(master_demanda['fecha'] == t) & (master_demanda['familia'] == k),
                                                  ['id_ciudad', 'cantidad']]
            # Renombrar y crear columnas restantes para que tenga estructura de `item_df`
            clientes_demanda.columns = ['nodo', 'valor']
            clientes_demanda['tiempo'] = t
            clientes_demanda['producto'] = k
            clientes_demanda['tipo'] = 'demanda'

            # FLUJO: los nodos restantes son de flujo. Estos son la diferencia de conjuntos entre todos los nodos de la
            # red, el nodo de produccion, y el nodo de demanda. Recordar que hay que borrar CLIENTE de los nodos únicos,
            # ya que en ITEMS ya estará representado como `clientes_demanda`
            nodos_flujo = list(set(nodos) - ({'CLIENTE'} | set(nodos_prod['nodo'])))
            nodos_flujo = pd.DataFrame(data={'tiempo': t, 'producto': k, 'nodo': nodos_flujo,
                                             'tipo': 'flujo', 'valor': 0})

            # ITEMS: Concatenar las secciones que iteran por producto a `item_df`
            item_df = pd.concat([item_df, nodos_prod, nodos_flujo, clientes_demanda], ignore_index=True)

        # ITEMS: Concatenar las restricciones estática y dinámica a `item_df`
        item_df = pd.concat([item_df, nodos_restr], ignore_index=True)

    return item_df


def build_activities(master_red, master_tarifario, master_demanda, master_ubicaciones):
    """
    Construye la tabla de Actividades que contiene 6 columnas: 'tiempo', 'producto', 'transporte', 'origen', 'destino', 'costo'.
    Esos origenes y destinos pueden ser id_locaciones para comunicaciones entre nodos de la infraestructura de Esenttia,
    o pueden ser id_ciudades para las entregas a clientes. En esta tabla se evidencian todas las actividades de distribución
    y almacenamiento de la red, así como sus costos
    :param master_ubicaciones:
    :param master_demanda:
    :param master_red:
    :param master_tarifario:
    :return:
    """

    # Delimitar cuantos meses hay para t
    MONTHS = sorted(master_demanda['fecha'].unique())

    # Abrir red infraestructra, seleccionar columnas relevantes ['origen', 'destino']
    master_red = master_red.loc[:, ['id_locacion_origen', 'id_locacion_destino']]

    # Abrir master tarifario, seleccionar columnas relevantes
    master_tarifario = master_tarifario[['id_ciudad_origen', 'id_ciudad_destino', 'capacidad', 'costo']]

    # Crear DF final con estructura definida en documentación
    actividad_df = pd.DataFrame(columns=['tiempo', 'producto', 'transporte', 'origen', 'destino', 'costo'])

    for t in MONTHS:
        # PRODUCTOS: seleccionamos los productos (familias) del master de demanda para el mes `t`
        PRODUCTS = master_demanda.loc[master_demanda['fecha'] == t, 'familia'].unique()
        for k in PRODUCTS:
            # ALMACENAMIENTO: crear actividad de almacenamiento a partir de los nodos que tengan valor diferente a cero
            # en capacidad_est en el master de ubicaciones. Es decir, que no sean NaN
            nodos_alm = master_ubicaciones.loc[~master_ubicaciones['capacidad_est'].isna(),
                                               ['id_locacion', 'costo_almacenamiento']]
            # Para distinguir almacenamiento (mov. en dimension tiempo) de demás actividades, agregar 'ALMACENAMIENTO'
            nodos_alm['id_locacion'] = nodos_alm['id_locacion'] + '_ALMACENAMIENTO'

            # Renombramos columnas
            nodos_alm.columns = ['origen', 'costo']

            # Agregar columna destino, que es una copia de la columna origen, producto, tiempo, y transporte
            nodos_alm['destino'] = nodos_alm['origen'].copy()
            nodos_alm['tiempo'] = t
            nodos_alm['producto'] = k
            nodos_alm['transporte'] = np.nan

            # TRANSPORTE: Reemplazar CLIENTE de master_red por `id_ciudad` de `master_demanda`. Haremos un DF de la
            # demanda, para luego hacerle un join con master_red de acuerdo a los sitios que pueden suplir CLIENTE
            clientes_demanda = master_demanda.loc[(master_demanda['fecha'] == t) & (master_demanda['familia'] == k),
                                                  'id_ciudad'].to_frame()
            clientes_demanda['key'] = 'CLIENTE'

            # Separamos master_red entre los que tienen en destino CLIENTE y los que no
            master_red_cliente = master_red.loc[master_red['id_locacion_destino'] == 'CLIENTE', :]
            master_red_no_cliente = master_red.loc[~(master_red['id_locacion_destino'] == 'CLIENTE'), :]
            # Cruzar `master_red_cliente` con `clientes_demanda`
            master_red_cliente = master_red_cliente.merge(clientes_demanda, left_on=['id_locacion_destino'],
                                                          right_on=['key'], how='inner')
            master_red_cliente = master_red_cliente.drop(columns=['id_locacion_destino', 'key'])
            master_red_cliente = master_red_cliente.rename(columns={'id_ciudad': 'id_locacion_destino'})

            # Volvemos a unir master_red_cliente con master_red
            master_red_clean = pd.concat([master_red_no_cliente, master_red_cliente], ignore_index=True)

            # Join entre tarifario y master de red
            # Se hace inner join porque si no hay vehículos que transporten, no puede existir arco en el `master_red`.
            nodos_trans = master_red_clean.merge(master_tarifario,
                                                 left_on=['id_locacion_origen', 'id_locacion_destino'],
                                                 right_on=['id_ciudad_origen', 'id_ciudad_destino'], how='inner')

            # Renombramos columnas específicas para que tengan formato de `actividad_df`
            nodos_trans = nodos_trans.rename(columns={'id_locacion_origen': 'origen',
                                                      'id_locacion_destino': 'destino',
                                                      'capacidad': 'transporte'})

            # Filtrar columnas relevantes
            nodos_trans = nodos_trans.loc[:, ['transporte', 'origen', 'destino', 'costo']]

            # Crear columnas restantes para tener estructura de `actividad_df`
            nodos_trans['tiempo'] = t
            nodos_trans['producto'] = k

            # ACIVIDADES: Concatenar nodos con transportes y almacenamiento a `actividad_df`
            actividad_df = pd.concat([actividad_df, nodos_trans, nodos_alm], ignore_index=True)

    return actividad_df


def matriz_coef(items_df: pd.DataFrame, actividades_df: pd.DataFrame):
    """
    v.2
    Función optimizada para crear la matriz de coeficientes con base a las actividades (columnas) e ítems (filas)
    ingresadas. Explota la velocidad de procesamiento de pd.merge() para realizar el cruce de condiciones por escenario
    o flujo.

    Retorna un np.array de coeficientes, siendo los indices `items_df`, y las columnas `actividades_df`.

    :param items_df: pd.DataFrame con los items del problema
    :param actividades_df: pd.DataFrame con las actividades (flujos) del problema
    :return: np.array con los coeficientes de entrada y salida de las actividades, en relación a las restricciones
    """
    coef_mat = np.zeros((items_df.shape[0], actividades_df.shape[0]))

    # Crear DFs para manejar tema de mutabilidad y columnas de indice de items y actividades
    actividades_df = actividades_df.copy()
    items_df = items_df.copy()
    actividades_df['idy'] = actividades_df.index
    items_df['idx'] = items_df.index

    # Al ser seis grupos de condiciones, serían 6 JOIN. CONDICIONES:
    # ENTRADA DE FLUJO. al ser INNER, no habrá valores nulos
    cond1 = pd.merge(items_df, actividades_df, left_on=['tiempo', 'producto', 'nodo'],
                     right_on=['tiempo', 'producto', 'origen'], how='inner')
    cond1['valor_mat'] = cond1['transporte'].copy()

    # SALIDA DE FLUJO
    cond2 = pd.merge(items_df, actividades_df, left_on=['tiempo', 'producto', 'nodo'],
                     right_on=['tiempo', 'producto', 'destino'], how='inner')
    cond2['valor_mat'] = -cond2['transporte'].copy()

    # ENTRADA INPUT A ALMACENAMIENTO
    cond3_items = items_df.copy()
    cond3_items.loc[:, 'nodo'] = cond3_items.loc[:, 'nodo'] + '_ALMACENAMIENTO'
    cond3 = pd.merge(cond3_items, actividades_df, left_on=['tiempo', 'producto', 'nodo'],
                     right_on=['tiempo', 'producto', 'origen'], how='inner')
    cond3['valor_mat'] = 1
    del cond3_items

    # SALIDA OUTPUT ALMACENAMIENTO
    cond4_items = items_df.copy()
    cond4_items.loc[:, 'tiempo'] -= 1
    cond4_items.loc[:, 'nodo'] = cond4_items.loc[:, 'nodo'] + '_ALMACENAMIENTO'
    cond4 = pd.merge(cond4_items, actividades_df, left_on=['tiempo', 'producto', 'nodo'],
                     right_on=['tiempo', 'producto', 'destino'], how='inner')
    cond4['valor_mat'] = -1
    del cond4_items

    # MAXIMO ALMACENAMIENTO (CAP ESTATICA)
    cond5_items = items_df.loc[items_df['tipo'] == 'capacidad_est'].copy()
    cond5_items.loc[:, 'nodo'] = cond5_items.loc[:, 'nodo'] + '_ALMACENAMIENTO'
    cond5 = pd.merge(cond5_items, actividades_df, left_on=['tiempo', 'nodo'], right_on=['tiempo', 'origen'],
                     how='inner')
    cond5['valor_mat'] = 1
    del cond5_items

    # MAXIMO FLUJO (CAP DINAMICA)
    cond6_items = items_df.loc[items_df['tipo'] == 'capacidad_din']
    cond6 = pd.merge(cond6_items, actividades_df, left_on=['tiempo', 'nodo'], right_on=['tiempo', 'destino'],
                     how='inner')
    cond6['valor_mat'] = cond6['transporte'].copy()
    del cond6_items

    condiciones = pd.concat([cond1, cond2, cond3, cond4, cond5, cond6], ignore_index=True)

    # Crear matriz de coeficiente a partir de tabla de condiciones
    for index, condicion in condiciones.iterrows():
        coef_mat[condicion['idx'], condicion['idy']] = condicion['valor_mat']

    return coef_mat

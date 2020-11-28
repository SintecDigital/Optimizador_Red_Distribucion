import pandas as pd
from creacion_items_actividades import *
from optimization import *
import time

"""
En este script se encuentran las funciones que ajustan los elementos del output. Aqui deberia ir lo que convierte
los DFs de output a .csv y los almacena. Con eso, logramos tener las cosas más organizadas. También incluiremos aquí
una función que ejecute de forma mensual el procedimiento de la demanda y que entregue los outputs consolidados.
También se desarrollarán funciones que entreguen un output que tenga más sentido para el usuario en temas de costos
"""


def carga_datos(folder_path='input/'):
    """
    Esta función no la usaremos ya que no volveremos a guardar los datos en la carpeta input por separado.
    :param folder_path:
    :return: DATASETS: lista que contiene los DFs a usar
    """

    # Abrir bases de datos
    DATASET_NAMES = ['master_red_infraestructura.csv', 'master_ubicaciones.csv', 'master_demanda.csv',
                     'master_producto.csv',
                     'master_tarifario.csv']
    DATASETS = [pd.read_csv(folder_path + x) for x in DATASET_NAMES]

    return DATASETS


def ejecucion(DATASETS: dict):
    """
    Función que corre el optimizador. Construye el escenario desde la carga de datos hata la construccion de inputs de
    la herramienta.

    :param DATASETS: diccionario con los DFs a analizar
    :return:
    """

    # Función para medir tiempo de rendimiento
    start_time = time.time()

    # ejecutamos build_items() para construir tabla de items
    items = build_items(DATASETS['master_red_infraestructura'], DATASETS['master_ubicaciones'],
                        DATASETS['master_demanda'], DATASETS['master_producto'])

    # Ejecutamos build_activities() construir tabla de actividades
    actividades = build_activities(DATASETS['master_red_infraestructura'], DATASETS['master_tarifario'],
                                   DATASETS['master_demanda'], DATASETS['master_ubicaciones'])

    # Ejecutamos matriz_coef() para construir matriz de coeficientes
    func_time = time.time()
    print("Inicio de construcción de matriz")
    matriz = matriz_coef(items, actividades)
    print(f"Tiempo construccion matriz: {time.time() - func_time}")

    # Correr optimizador
    modelo = [x for x in optimizacion(items_df=items, actividades_df=actividades, coef_mat=matriz)]

    # Mostar valor óptimo y tiempo total
    print("--- Tiempo optimización: %s segundos ---" % (time.time() - start_time))

    costo = modelo[2]
    print(f"El costo total óptimo es {costo} COP\n")

    # Creamos las tablas de output del modelo
    decision = df_variables(modelo[1], actividades)
    restriccion = df_restricciones(decision, items, matriz)

    return decision, restriccion, costo, items, actividades, matriz


def guardar_outputs(df_list, df_names, output_path='output/'):
    """
    Retorna los outputs de la herramienta en formato .csv y los almacena en la carpeta output/ . Estos outputs son
    las variables de decision con sus actividades, los items con sus el valor de las cantidades restringidas, y una tabla
    de costos donde se encuentre separado Distribución (inicialmente sin distinguir T1 y T2), Exportación, Almacenamiento,
    y costo de movimiento dinámico.

    :return:
    """
    for df, name in zip(df_list, df_names):
        df.to_csv(output_path + name, index=True)

    return 0

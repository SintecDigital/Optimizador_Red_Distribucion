from creacion_items_actividades import *
import numpy as np
import time


def matriz_test(df_items, df_actividades, funcs, n_iters=10):
    """
    Testea qué funciones de creación de matrices son más rapidas, a través de la realización de n_iters, para luego
    entregar el tiempo promedio de ejecución. También compara que los resultados de las funciones sean idénticos.

    :param funcs: lista de funciones a testear
    """
    times_dict = {}
    matrices = []

    for func in funcs:
        times_dict[func] = []
        for i in range(n_iters):
            init_time = time.time()
            if i == 0:
                matrices.append(func(df_items, df_actividades))
            else:
                func(df_items, df_actividades)
            times_dict[func].append(time.time() - init_time)

        # Calcular promedio
        times_dict[func] = sum(times_dict[func]) / n_iters
    print("\nLas matrices son %d" % (np.array_equal(matrices[0], matrices[1])))
    return times_dict


if __name__ == '__main__':
    # Abrir tests

    DATA_PATH = 'datamaster.xlsx'

    # Necesitamos las primeras cuatro hojas del .xlsx
    DATASET_NAMES = ['master_producto', 'master_ubicaciones', 'master_demanda',
                     'master_tarifario', 'master_red_infraestructura']
    DATASETS = [pd.read_excel(DATA_PATH, sheet_name=i) for i in DATASET_NAMES]

    # ejecutamos build_items() para construir tabla de items
    items = build_items(DATASETS[4], DATASETS[1], DATASETS[2], DATASETS[0])

    # Ejecutamos build_activities() construir tabla de actividades
    actividades = build_activities(DATASETS[0], DATASETS[4], DATASETS[2], DATASETS[1])

    # Ejecutamos matriz_coef() para construir matriz de coeficientes
    print(matriz_test(items, actividades, [matriz_coef, matriz_coef_v1], n_iters=5))

    matriz = matriz_coef(items, actividades)
    matriz_df = pd.DataFrame(data=matriz, index=items, columns=actividades)
    matriz_df.to_csv('test_input/output/output_coef.csv')

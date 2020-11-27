import cvxpy as cp
import pandas as pd
import numpy as np


def optimizacion(items_df, actividades_df, coef_mat):
    """
    v2. Se construye el modelo a partir de la API de CVXPY
    Desarrolla la optimización de un problema de transporte a partir de una matriz de coeficientes, un vector de
    restricciones, y un vector de costos. Se usa un constructor de modelo más veloz para reducir el tiempo de procesa-
    miento.

    :param items_df:
    :param actividades_df:
    :param coef_mat:
    :return:
    """

    print("Proceso de optimización ha comenzado")

    # INPUT: Ajustar items_df para que la demanda sea negativa
    items_df.loc[items_df['tipo'] == 'demanda', 'valor'] *= -1

    # VARIABLES:
    X = cp.Variable(actividades_df['costo'].T.shape)

    # FUNCION OBJETIVO
    obj = cp.Minimize(actividades_df['costo'].values @ X)

    # RESTRICCIONES
    restricciones = [X >= 0]  # Restriccion de no-negatividad de las variables
    for i in range(items_df.shape[0]):
        if items_df['tipo'][i] in ['demanda', 'flujo']:
            restricciones.append(coef_mat[i].T @ X == items_df['valor'][i])
        else:
            restricciones.append(coef_mat[i].T @ X <= items_df['valor'][i])
    # DECLARAR PROBLEMA
    prob = cp.Problem(obj, restricciones)

    # Crear objetos para el resultado y las variables de resultado
    obj_val = prob.solve(solver=cp.GLPK, verbose=True)
    variables = X.value

    return prob, variables, obj_val


def df_variables(variables, df_actividades):
    """
    v2, usa output de CVXPY como insumo, lo cual reduce 30x el tiempo de ejecución.

    Esta función construye un DataFrame de decisión donde se adjuntan las variables encontradas por el procedimiento
    de optimización. Dicho DataFrame se construye a partir de las actividades usadas y las variables encontradas. Al
    venir en una lista ordenada por variable, solo se concatena al df_variables

    :param variables: np.array con los valores encontrados por el solver
    :param df_actividades:
    :return:
    """

    # Añadir valores a df_actividades
    df_actividades['valor_decision'] = variables

    return df_actividades


def df_restricciones(df_decision:pd.DataFrame, df_items, matriz_coef:np.array):
    """
    Calcula el valor de las restricciones de acuerdo a la solución propuesta por el optimizador. Este resultado es
    obtenido por multiplicación matricial entre la matriz coeficientes (i, a) y variables de decisión (a, 1), lo que
    resulta en un vector (i, 1)

    :param df_items:
    :param df_decision: pd.DataFrame que resulta de df_variables(), que contiene en orden las variables de decisión
    :param matriz_coef: Matriz de coeficientes con número de filas len(items) y columnas len(actividades)
    :return: df_items: pd.DataFrame que contiene los items del modelo. Dado que los items imponen las restricciones,
    se agrega el valor de las restricciones cumplidas a este DF
    """
    variables = df_decision['valor_decision'].values

    # Reshape arreglos a 2D
    variables = np.reshape(variables, (-1, 1))

    # Producto de matrices y agregar valor a df_items
    restricciones = matriz_coef @ variables
    df_items['cumplimiento_restriccion'] = restricciones

    return df_items

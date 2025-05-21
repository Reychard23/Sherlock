# data_processing.py

import pandas as pd
import numpy as np
import os
from typing import Dict, Any, List, Tuple, Set
from datetime import date


# === Función para cargar datos desde rutas de archivos ===
# NOTA: En el entorno de Render/FastAPI, estas rutas apuntarán a archivos
# temporales creados por el microservicio en el sistema de archivos efímero de Render,
# NO a archivos en tu máquina local.
def load_local_dataframes(data_folder_path: str, index_filepath: str) -> Tuple[Dict[str, pd.DataFrame], Dict[tuple[str, str], dict[str, str]], set[tuple[str, str, str]], List[str]]:
    """
    Lee archivos Excel desde una carpeta temporal y el archivo indice.xlsx,
    aplica la limpieza y renombrado según el índice, y devuelve un diccionario
    de DataFrames en memoria junto con los diccionarios de mapeo/drop.

    Args:
        data_folder_path: Ruta a la carpeta temporal donde están los archivos Excel.
        index_filepath: Ruta completa al archivo indice.xlsx temporal.

    Returns:
        Una tupla conteniendo:
        - Un diccionario donde las claves son nombres generados para los DataFrames
          procesados (ArchivoBase_Hoja) y los valores son los DataFrames de Pandas.
        - El rename_dict construido desde el índice.
        - El drop_set construido desde el índice.
        - Una lista de advertencias encontradas durante la carga.
    """
    processed_dfs: Dict[str, pd.DataFrame] = {}
    advertencias_carga: List[str] = []

    print(f"Intentando leer índice desde: {index_filepath}")
    try:
        # Forzar el tipo de dato de las columnas clave para evitar problemas de mezcla de tipos
        indice_df = pd.read_excel(
            index_filepath, dtype={'ArchivoBase': str, 'Hoja': str, 'CampoFuente': str})
        print("Índice cargado exitosamente.")
        # print(indice_df.head()) # Para depuración

        # Asegúrate de que las columnas críticas existan
        required_cols = ['ArchivoBase', 'Hoja', 'CampoFuente',
                         'CampoDestino', 'TipoDato', 'Accion']
        if not all(col in indice_df.columns for col in required_cols):
            advertencias_carga.append(
                f"ADVERTENCIA: El archivo índice.xlsx debe contener las columnas: {required_cols}. Faltan algunas.")
            return {}, {}, set(), advertencias_carga

        # Eliminar filas donde CampoFuente o CampoDestino sean NaN para evitar errores en el mapeo
        indice_df.dropna(subset=['CampoFuente', 'CampoDestino'], inplace=True)

        # Construir diccionarios de renombrado y el conjunto de columnas a dropear
        rename_dict: Dict[tuple[str, str], dict[str, str]] = {}
        drop_set: Set[tuple[str, str, str]] = set()

        for _, row in indice_df.iterrows():
            file_name = row['ArchivoBase']
            sheet_name = row['Hoja']
            source_col = row['CampoFuente']
            target_col = row['CampoDestino']
            action = str(row['Accion']).strip().lower()

            df_key = (file_name, sheet_name)

            if df_key not in rename_dict:
                rename_dict[df_key] = {}

            if action == 'homologar' and pd.notna(source_col) and pd.notna(target_col):
                rename_dict[df_key][source_col] = target_col
            elif action == 'eliminar' and pd.notna(source_col):
                drop_set.add((file_name, sheet_name, source_col))
            # print(f"Procesando índice: {file_name}, {sheet_name}, {source_col}, {target_col}, {action}") # Depuración

    except FileNotFoundError:
        advertencias_carga.append(
            f"ERROR: El archivo índice.xlsx no se encontró en la ruta: {index_filepath}")
        print(
            f"ERROR: El archivo índice.xlsx no se encontró en la ruta: {index_filepath}")
        return {}, {}, set(), advertencias_carga
    except Exception as e:
        advertencias_carga.append(
            f"ERROR: No se pudo leer el archivo índice.xlsx. Asegúrate de que es un archivo Excel válido. Error: {e}")
        print(f"ERROR: No se pudo leer el archivo índice.xlsx. Error: {e}")
        return {}, {}, set(), advertencias_carga

    print("Iniciando carga de archivos de datos...")
    # Itera sobre los archivos en la carpeta de datos
    for filename in os.listdir(data_folder_path):
        # Excluir archivos temporales de Excel
        if filename.endswith('.xlsx') and not filename.startswith('~'):
            filepath = os.path.join(data_folder_path, filename)
            try:
                # Cargar todas las hojas del archivo Excel
                xls = pd.ExcelFile(filepath)
                sheet_names = xls.sheet_names

                for sheet_name in sheet_names:
                    df_key_for_lookup = (filename, sheet_name)
                    df_name_for_storage = f"{filename.replace('.xlsx', '')}_{sheet_name}"

                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    original_columns = set(df.columns)

                    # Aplicar renombrado
                    current_rename_map = rename_dict.get(
                        df_key_for_lookup, {})
                    if current_rename_map:
                        cols_to_rename = {
                            k: v for k, v in current_rename_map.items() if k in df.columns}
                        if cols_to_rename:
                            df.rename(columns=cols_to_rename, inplace=True)
                            # print(f"Renombradas columnas en {df_name_for_storage}: {cols_to_rename}") # Depuración
                        else:
                            advertencias_carga.append(
                                f"ADVERTENCIA: No se encontraron columnas para renombrar en {df_name_for_storage} según el índice. Mapeo esperado: {current_rename_map}")
                    # else:
                        # print(f"No hay reglas de renombrado para {df_name_for_storage} en el índice.") # Depuración

                    # Aplicar eliminación de columnas
                    cols_to_drop = [col for (
                        f, s, col) in drop_set if f == filename and s == sheet_name and col in df.columns]
                    if cols_to_drop:
                        df.drop(columns=cols_to_drop, inplace=True)
                        # print(f"Eliminadas columnas en {df_name_for_storage}: {cols_to_drop}") # Depuración

                    # Verificar si todas las columnas "CampoDestino" que deberían estar presentes después del renombrado
                    # realmente están en el DataFrame.
                    expected_cols_after_rename = set(
                        current_rename_map.values())
                    actual_cols_after_processing = set(df.columns)

                    missing_expected_cols = expected_cols_after_rename - \
                        actual_cols_after_processing

                    if missing_expected_cols:
                        advertencias_carga.append(
                            f"ADVERTENCIA: Después de procesar {df_name_for_storage}, faltan columnas destino esperadas según el índice: {list(missing_expected_cols)}. Posiblemente las columnas fuente no existían o fueron eliminadas antes de renombrar.")

                    processed_dfs[df_name_for_storage] = df
                    print(f"Cargado y procesado: {df_name_for_storage}")

            except Exception as e:
                advertencias_carga.append(
                    f"ERROR: No se pudo procesar el archivo {filename}, hoja {sheet_name}. Error: {e}")
                print(
                    f"ERROR: No se pudo procesar el archivo {filename}, hoja {sheet_name}. Error: {e}")

    return processed_dfs, rename_dict, drop_set, advertencias_carga


# === Función para enriquecer DataFrames ===
def enrich_dataframes(dfs: Dict[str, pd.DataFrame], advertencias_enriquecimiento: List[str]) -> Dict[str, pd.DataFrame]:
    print("\n--- Iniciando enriquecimiento de DataFrames ---")

    # DF1: df_atenciones (Atenciones_Atenciones)
    df_atenciones = dfs.get('Atenciones_Atenciones')
    if df_atenciones is not None and not df_atenciones.empty:
        # Conversión de tipos
        for col in ['Fecha_Atencion', 'Fecha_Creacion_Atencion', 'Fecha_Modificacion_Atencion']:
            if col in df_atenciones.columns:
                df_atenciones[col] = pd.to_datetime(
                    df_atenciones[col], errors='coerce')
        for col in ['Paciente_ID', 'Sucursal_ID', 'Tratamiento_ID', 'Atencion_ID', 'Presupuesto_ID']:
            if col in df_atenciones.columns:
                df_atenciones[col] = pd.to_numeric(
                    # Usar Int64 para NaN
                    df_atenciones[col], errors='coerce').astype('Int64')
        for col in ['Precio_Atencion_CLP', 'Costo_Atencion_CLP', 'Porcentaje_Descuento_Atencion', 'Monto_Descuento_Atencion_CLP', 'Monto_Final_Atencion_CLP']:
            if col in df_atenciones.columns:
                df_atenciones[col] = pd.to_numeric(
                    df_atenciones[col], errors='coerce')

        # Eliminar duplicados en 'Atencion_ID'
        if 'Atencion_ID' in df_atenciones.columns:
            atenciones_initial_rows = len(df_atenciones)
            df_atenciones.drop_duplicates(
                subset=['Atencion_ID'], inplace=True)
            if len(df_atenciones) < atenciones_initial_rows:
                advertencias_enriquecimiento.append(
                    f"ADVERTENCIA: Se eliminaron {atenciones_initial_rows - len(df_atenciones)} duplicados de 'Atencion_ID' en df_atenciones.")
            print(f"df_atenciones procesado. Filas: {len(df_atenciones)}")
        else:
            advertencias_enriquecimiento.append(
                "ADVERTENCIA: La columna 'Atencion_ID' no se encuentra en df_atenciones. No se pudieron eliminar duplicados.")
    else:
        advertencias_enriquecimiento.append(
            "ADVERTENCIA: 'Atenciones_Atenciones' no encontrado o vacío. No se pudo enriquecer.")
        # Asegura que df_atenciones sea un DataFrame vacío si no se encuentra o está vacío
        df_atenciones = pd.DataFrame()

    # DF2: df_pagos (Atenciones_Pagos)
    df_pagos = dfs.get('Atenciones_Pagos')
    if df_pagos is not None and not df_pagos.empty:
        # Conversión de tipos
        for col in ['Monto_Pagado_CLP', 'Monto_Devuelto_CLP']:
            if col in df_pagos.columns:
                df_pagos[col] = pd.to_numeric(
                    df_pagos[col], errors='coerce')
        for col in ['Atencion_ID', 'Forma_Pago_ID', 'Paciente_ID', 'Sucursal_ID', 'Tratamiento_ID']:
            if col in df_pagos.columns:
                df_pagos[col] = pd.to_numeric(
                    df_pagos[col], errors='coerce').astype('Int64')
        if 'Fecha_Pago' in df_pagos.columns:
            df_pagos['Fecha_Pago'] = pd.to_datetime(
                df_pagos['Fecha_Pago'], errors='coerce')

        # Agrupar pagos por Atención_ID
        if all(col in df_pagos.columns for col in ['Atencion_ID', 'Monto_Pagado_CLP', 'Monto_Devuelto_CLP']):
            df_pagos_agrupado = df_pagos.groupby('Atencion_ID').agg(
                Monto_Pagado_Acumulado=('Monto_Pagado_CLP', 'sum'),
                Monto_Devuelto_Acumulado=('Monto_Devuelto_CLP', 'sum')
            ).reset_index()
            print(
                f"df_pagos procesado y agrupado. Filas: {len(df_pagos_agrupado)}")
        else:
            df_pagos_agrupado = pd.DataFrame()
            advertencias_enriquecimiento.append(
                "ADVERTENCIA: Columnas necesarias para agrupar pagos (Atencion_ID, Monto_Pagado_CLP, Monto_Devuelto_CLP) no encontradas en 'Atenciones_Pagos'.")
    else:
        advertencias_enriquecimiento.append(
            "ADVERTENCIA: 'Atenciones_Pagos' no encontrado o vacío. No se pudo enriquecer.")
        df_pagos_agrupado = pd.DataFrame()

    # DF3: df_presupuestos (Presupuestos_Presupuestos)
    df_presupuestos = dfs.get('Presupuestos_Presupuestos')
    if df_presupuestos is not None and not df_presupuestos.empty:
        # Conversión de tipos
        for col in ['Presupuesto_ID', 'Paciente_ID', 'Sucursal_ID']:
            if col in df_presupuestos.columns:
                df_presupuestos[col] = pd.to_numeric(
                    df_presupuestos[col], errors='coerce').astype('Int64')
        for col in ['Monto_Total_Presupuesto_CLP', 'Monto_Pagado_Presupuesto_CLP']:
            if col in df_presupuestos.columns:
                df_presupuestos[col] = pd.to_numeric(
                    df_presupuestos[col], errors='coerce')
        for col in ['Fecha_Creacion_Presupuesto', 'Fecha_Cierre_Presupuesto']:
            if col in df_presupuestos.columns:
                df_presupuestos[col] = pd.to_datetime(
                    df_presupuestos[col], errors='coerce')

        # Eliminar duplicados en 'Presupuesto_ID'
        if 'Presupuesto_ID' in df_presupuestos.columns:
            presupuestos_initial_rows = len(df_presupuestos)
            df_presupuestos.drop_duplicates(
                subset=['Presupuesto_ID'], inplace=True)
            if len(df_presupuestos) < presupuestos_initial_rows:
                advertencias_enriquecimiento.append(
                    f"ADVERTENCIA: Se eliminaron {presupuestos_initial_rows - len(df_presupuestos)} duplicados de 'Presupuesto_ID' en df_presupuestos.")
            print(
                f"df_presupuestos procesado. Filas: {len(df_presupuestos)}")
        else:
            advertencias_enriquecimiento.append(
                "ADVERTENCIA: La columna 'Presupuesto_ID' no se encuentra en df_presupuestos. No se pudieron eliminar duplicados.")
    else:
        advertencias_enriquecimiento.append(
            "ADVERTENCIA: 'Presupuestos_Presupuestos' no encontrado o vacío. No se pudo enriquecer.")
        df_presupuestos = pd.DataFrame()

    # DF4: df_pacientes (Pacientes_Pacientes)
    df_pacientes = dfs.get('Pacientes_Pacientes')
    if df_pacientes is not None and not df_pacientes.empty:
        # Conversión de tipos
        for col in ['Paciente_ID', 'Sucursal_ID_Paciente']:
            if col in df_pacientes.columns:
                df_pacientes[col] = pd.to_numeric(
                    df_pacientes[col], errors='coerce').astype('Int64')
        for col in ['Fecha_Nacimiento_Paciente', 'Fecha_Creacion_Paciente']:
            if col in df_pacientes.columns:
                df_pacientes[col] = pd.to_datetime(
                    df_pacientes[col], errors='coerce')

        # Eliminar duplicados en 'Paciente_ID'
        if 'Paciente_ID' in df_pacientes.columns:
            pacientes_initial_rows = len(df_pacientes)
            df_pacientes.drop_duplicates(
                subset=['Paciente_ID'], inplace=True)
            if len(df_pacientes) < pacientes_initial_rows:
                advertencias_enriquecimiento.append(
                    f"ADVERTENCIA: Se eliminaron {pacientes_initial_rows - len(df_pacientes)} duplicados de 'Paciente_ID' en df_pacientes.")
            print(f"df_pacientes procesado. Filas: {len(df_pacientes)}")
        else:
            advertencias_enriquecimiento.append(
                "ADVERTENCIA: La columna 'Paciente_ID' no se encuentra en df_pacientes. No se pudieron eliminar duplicados.")

        # Edad del paciente
        if all(col in df_pacientes.columns for col in ['Fecha_Nacimiento_Paciente']):
            today = date.today()
            df_pacientes['Edad_Paciente'] = df_pacientes['Fecha_Nacimiento_Paciente'].apply(
                lambda dob: today.year - dob.year -
                ((today.month, today.day) < (dob.month, dob.day)
                 ) if pd.notna(dob) else np.nan
            ).astype('Int64')
        else:
            advertencias_enriquecimiento.append(
                "ADVERTENCIA: 'Fecha_Nacimiento_Paciente' no encontrada en df_pacientes. No se pudo calcular 'Edad_Paciente'.")
    else:
        advertencias_enriquecimiento.append(
            "ADVERTENCIA: 'Pacientes_Pacientes' no encontrado o vacío. No se pudo enriquecer.")
        df_pacientes = pd.DataFrame()

    # DF5: df_sucursales (Dentalink_Sucursales)
    df_sucursales = dfs.get('Dentalink_Sucursales')
    if df_sucursales is not None and not df_sucursales.empty:
        # Conversión de tipos
        if 'Sucursal_ID' in df_sucursales.columns:
            df_sucursales['Sucursal_ID'] = pd.to_numeric(
                df_sucursales['Sucursal_ID'], errors='coerce').astype('Int64')
        print(f"df_sucursales procesado. Filas: {len(df_sucursales)}")
    else:
        advertencias_enriquecimiento.append(
            "ADVERTENCIA: 'Dentalink_Sucursales' no encontrado o vacío. No se pudo enriquecer.")
        df_sucursales = pd.DataFrame()

    # DF6: df_tratamientos (Dentalink_Tratamientos)
    df_tratamientos = dfs.get('Dentalink_Tratamientos')
    if df_tratamientos is not None and not df_tratamientos.empty:
        # Conversión de tipos
        if 'Tratamiento_ID' in df_tratamientos.columns:
            df_tratamientos['Tratamiento_ID'] = pd.to_numeric(
                df_tratamientos['Tratamiento_ID'], errors='coerce').astype('Int64')
        print(f"df_tratamientos procesado. Filas: {len(df_tratamientos)}")
    else:
        advertencias_enriquecimiento.append(
            "ADVERTENCIA: 'Dentalink_Tratamientos' no encontrado o vacío. No se pudo enriquecer.")
        df_tratamientos = pd.DataFrame()

    # Almacenar los DataFrames enriquecidos en el diccionario `dfs`
    dfs['Atenciones_Atenciones'] = df_atenciones
    dfs['Atenciones_Pagos_Agrupado'] = df_pagos_agrupado  # Guarda el DF agrupado
    dfs['Presupuestos_Presupuestos'] = df_presupuestos
    dfs['Pacientes_Pacientes'] = df_pacientes
    dfs['Dentalink_Sucursales'] = df_sucursales
    dfs['Dentalink_Tratamientos'] = df_tratamientos

    print("--- Enriquecimiento de DataFrames completado ---")
    return dfs


# === Función para unir DataFrames ===
def join_dataframes(dfs: Dict[str, pd.DataFrame], advertencias_union: List[str]) -> Dict[str, pd.DataFrame]:
    print("\n--- Iniciando unión de DataFrames ---")

    df_atenciones = dfs.get('Atenciones_Atenciones')
    df_pagos_agrupado = dfs.get('Atenciones_Pagos_Agrupado')
    df_presupuestos = dfs.get('Presupuestos_Presupuestos')
    df_pacientes = dfs.get('Pacientes_Pacientes')
    df_sucursales = dfs.get('Dentalink_Sucursales')
    df_tratamientos = dfs.get('Dentalink_Tratamientos')

    # Unir df_atenciones con df_pagos_agrupado
    if not df_atenciones.empty and not df_pagos_agrupado.empty and 'Atencion_ID' in df_atenciones.columns and 'Atencion_ID' in df_pagos_agrupado.columns:
        df_merged = pd.merge(df_atenciones, df_pagos_agrupado,
                             on='Atencion_ID', how='left')
        print(
            f"Unido df_atenciones con df_pagos_agrupado. Filas: {len(df_merged)}")
    elif not df_atenciones.empty:
        df_merged = df_atenciones.copy()
        advertencias_union.append(
            "ADVERTENCIA: No se pudo unir df_atenciones con df_pagos_agrupado. df_pagos_agrupado está vacío o faltan columnas.")
    else:
        df_merged = pd.DataFrame()
        advertencias_union.append(
            "ADVERTENCIA: df_atenciones está vacío, no se pudo iniciar la unión de DataFrames.")

    # Unir df_merged (atenciones + pagos) con df_presupuestos
    if not df_merged.empty and not df_presupuestos.empty and 'Presupuesto_ID' in df_merged.columns and 'Presupuesto_ID' in df_presupuestos.columns:
        # Seleccionar solo las columnas necesarias de df_presupuestos para evitar duplicados si ya existen
        cols_to_merge_from_presupuestos = [
            col for col in df_presupuestos.columns if col not in df_merged.columns or col == 'Presupuesto_ID']
        df_merged = pd.merge(df_merged, df_presupuestos[cols_to_merge_from_presupuestos],
                             on='Presupuesto_ID', how='left', suffixes=('', '_presupuesto'))
        print(f"Unido df_merged con df_presupuestos. Filas: {len(df_merged)}")
    elif not df_presupuestos.empty:
        advertencias_union.append(
            "ADVERTENCIA: No se pudo unir df_merged con df_presupuestos. df_merged está vacío o faltan columnas.")

    # Unir df_merged con df_pacientes
    if not df_merged.empty and not df_pacientes.empty and 'Paciente_ID' in df_merged.columns and 'Paciente_ID' in df_pacientes.columns:
        # Seleccionar solo las columnas necesarias de df_pacientes
        cols_to_merge_from_pacientes = [
            col for col in df_pacientes.columns if col not in df_merged.columns or col == 'Paciente_ID']
        df_merged = pd.merge(df_merged, df_pacientes[cols_to_merge_from_pacientes],
                             on='Paciente_ID', how='left', suffixes=('', '_paciente'))
        print(f"Unido df_merged con df_pacientes. Filas: {len(df_merged)}")
    elif not df_pacientes.empty:
        advertencias_union.append(
            "ADVERTENCIA: No se pudo unir df_merged con df_pacientes. df_merged está vacío o faltan columnas.")

    # Unir df_merged con df_sucursales
    if not df_merged.empty and not df_sucursales.empty and 'Sucursal_ID' in df_merged.columns and 'Sucursal_ID' in df_sucursales.columns:
        # Renombrar 'Nombre' en df_sucursales para evitar conflicto
        df_sucursales_renamed = df_sucursales.rename(
            columns={'Nombre': 'Nombre_Sucursal'})
        cols_to_merge_from_sucursales = [
            col for col in df_sucursales_renamed.columns if col not in df_merged.columns or col == 'Sucursal_ID']
        df_merged = pd.merge(df_merged, df_sucursales_renamed[cols_to_merge_from_sucursales],
                             on='Sucursal_ID', how='left', suffixes=('', '_sucursal'))
        print(f"Unido df_merged con df_sucursales. Filas: {len(df_merged)}")
    elif not df_sucursales.empty:
        advertencias_union.append(
            "ADVERTENCIA: No se pudo unir df_merged con df_sucursales. df_merged está vacío o faltan columnas.")

    # Unir df_merged con df_tratamientos
    if not df_merged.empty and not df_tratamientos.empty and 'Tratamiento_ID' in df_merged.columns and 'Tratamiento_ID' in df_tratamientos.columns:
        # Renombrar 'Nombre' en df_tratamientos para evitar conflicto
        df_tratamientos_renamed = df_tratamientos.rename(
            columns={'Nombre': 'Nombre_Tratamiento', 'Costo': 'Costo_Tratamiento', 'Precio': 'Precio_Tratamiento'})
        cols_to_merge_from_tratamientos = [
            col for col in df_tratamientos_renamed.columns if col not in df_merged.columns or col == 'Tratamiento_ID']
        df_merged = pd.merge(df_merged, df_tratamientos_renamed[cols_to_merge_from_tratamientos],
                             on='Tratamiento_ID', how='left', suffixes=('', '_tratamiento'))
        print(f"Unido df_merged con df_tratamientos. Filas: {len(df_merged)}")
    elif not df_tratamientos.empty:
        advertencias_union.append(
            "ADVERTENCIA: No se pudo unir df_merged con df_tratamientos. df_merged está vacío o faltan columnas.")

    dfs['df_analisis_final'] = df_merged

    print("--- Unión de DataFrames completada ---")
    return dfs


# === Función para realizar análisis de perfil de pacientes ===
def analyze_patient_profile(df_analisis_final: pd.DataFrame, all_advertencias: List[str]) -> Dict[str, pd.DataFrame]:
    print("\n--- Iniciando análisis de perfil de pacientes ---")
    analysis_results: Dict[str, pd.DataFrame] = {}

    if not df_analisis_final.empty:
        # Definir las columnas necesarias para el análisis de perfil
        cols_analisis_needed_in_df = [
            'Paciente_ID', 'Fecha_Atencion', 'Fecha_Creacion_Atencion', 'Monto_Final_Atencion_CLP',
            'Estado_Atencion', 'Nombre_Sucursal', 'Segmento_Paciente', 'Estado_Civil', 'Edad_Paciente',
            'Ocupacion_Paciente', 'Comuna_Paciente', 'Nivel_Socioeconomico', 'Nivel_Educacional',
            'Prevision_Paciente'
        ]

        if all(col in df_analisis_final.columns for col in cols_analisis_needed_in_df):
            # Filtrar atenciones con estado 'Realizado' o 'Pagado'
            df_filtered = df_analisis_final[
                df_analisis_final['Estado_Atencion'].isin(
                    ['Realizado', 'Pagado'])
            ].copy()

            if df_filtered.empty:
                all_advertencias.append(
                    "ADVERTENCIA: No hay atenciones 'Realizado' o 'Pagado' para el análisis de perfil.")
                print(
                    "  No hay atenciones 'Realizado' o 'Pagado' para el análisis de perfil.")
                return analysis_results

            # Identificar pacientes nuevos en el período
            # Se considera nuevo si la Fecha_Creacion_Atencion es la primera que tiene el paciente
            df_filtered['Fecha_Primera_Atencion_Paciente'] = df_filtered.groupby('Paciente_ID')[
                'Fecha_Creacion_Atencion'].transform('min')

            # Calcular la fecha del primer día del mes actual para el filtro de pacientes nuevos
            # Esto debería ser dinámico, o pasado como parámetro si se quiere analizar un mes específico
            # Por ahora, usamos el mes de la última fecha de atención disponible como referencia.
            # O mejor, usamos la fecha de hoy para un análisis "actual".
            today_date = pd.to_datetime(date.today())
            first_day_of_current_month = today_date.replace(day=1)

            # Pacientes con su primera atención en el mes actual
            df_filtered['Paciente_Nuevo_Este_Mes'] = (
                df_filtered['Fecha_Primera_Atencion_Paciente'].dt.to_period(
                    'M') == first_day_of_current_month.to_period('M')
            )

            # Seleccionar solo las atenciones realizadas por pacientes nuevos en el mes actual
            df_pacientes_nuevos_atendidos = df_filtered[
                df_filtered['Paciente_Nuevo_Este_Mes']
            ].copy()

            if df_pacientes_nuevos_atendidos.empty:
                all_advertencias.append(
                    "ADVERTENCIA: No se encontraron pacientes nuevos atendidos en el mes actual para el análisis de perfil.")
                print(
                    "  No se encontraron pacientes nuevos atendidos en el mes actual para el análisis de perfil.")
                return analysis_results

            # --- Análisis de Perfil de Pacientes Nuevos Atendidos (del mes actual) ---
            profile_dimensions_atendidos = [
                'Nombre_Sucursal', 'Segmento_Paciente', 'Estado_Civil', 'Edad_Paciente',
                'Ocupacion_Paciente', 'Comuna_Paciente', 'Nivel_Socioeconomico', 'Nivel_Educacional',
                'Prevision_Paciente'
            ]

            print(
                f"  Analizando el perfil de {df_pacientes_nuevos_atendidos['Paciente_ID'].nunique()} pacientes nuevos atendidos este mes.")
            for dim in profile_dimensions_atendidos:
                if dim in df_pacientes_nuevos_atendidos.columns:
                    # Contar pacientes únicos por dimensión
                    profile_summary = df_pacientes_nuevos_atendidos.groupby(dim).agg(
                        Cantidad_Pacientes_Nuevos=('Paciente_ID', 'nunique'),
                        Monto_Total_Atenciones=(
                            'Monto_Final_Atencion_CLP', 'sum')
                    ).reset_index()

                    total_pacientes_nuevos = profile_summary['Cantidad_Pacientes_Nuevos'].sum(
                    )
                    if total_pacientes_nuevos > 0:
                        profile_summary['Porcentaje_Pacientes_Nuevos'] = (
                            profile_summary['Cantidad_Pacientes_Nuevos'] /
                            total_pacientes_nuevos * 100
                        ).round(2)
                    else:
                        profile_summary['Porcentaje_Pacientes_Nuevos'] = 0.0

                    profile_summary = profile_summary.sort_values(
                        by='Cantidad_Pacientes_Nuevos', ascending=False)
                    analysis_results[f'Perfil_Pacientes_Nuevos_Atendidos_Por_{dim}'] = profile_summary
                    print(
                        f"\n--- Perfil de Pacientes Nuevos Atendidos por {dim} ---")
                    print(profile_summary.head(10))
                else:
                    all_advertencias.append(
                        f"ADVERTENCIA: La dimensión '{dim}' no se encontró en el DataFrame para el análisis de perfil de pacientes nuevos atendidos.")
                    print(
                        f"  ADVERTENCIA: La dimensión '{dim}' no se encontró para el análisis de perfil de pacientes nuevos atendidos.")
        else:
            missing = [
                col for col in cols_analisis_needed_in_df if col not in df_analisis_final.columns]
            all_advertencias.append(
                f"ADVERTENCIA: Faltan columnas esenciales en el DataFrame final para realizar análisis de perfil: {missing}. Verifica los pasos de enriquecimiento y unión.")
            print(
                f"ADVERTENCIA: Faltan columnas esenciales en el DataFrame final para realizar análisis de perfil: {missing}. Verifica los pasos de enriquecimiento y unión.")
    else:
        print(
            "El DataFrame final para análisis está vacío o no se pudo crear. No se puede realizar el análisis de perfil.")

    print("--- Análisis de perfil de pacientes completado ---")
    return analysis_results

# Nueva función principal que orquesta todo el procesamiento


def run_data_processing(data_folder_path: str, index_filepath: str) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """
    Orquesta la carga, enriquecimiento, unión y análisis de DataFrames.

    Args:
        data_folder_path: Ruta a la carpeta donde están los archivos Excel.
        index_filepath: Ruta completa al archivo indice.xlsx.

    Returns:
        Una tupla conteniendo:
        - Un diccionario de DataFrames procesados, incluyendo el final para análisis.
        - Una lista de todas las advertencias generadas durante el proceso.
    """
    all_advertencias: List[str] = []

    # 1. Cargar datos locales
    processed_dfs, rename_dict, drop_set, advertencias_carga = load_local_dataframes(
        data_folder_path, index_filepath)
    all_advertencias.extend(advertencias_carga)
    print(
        f"\nDataFrames cargados y procesados inicialmente: {list(processed_dfs.keys())}")

    # 2. Enriquecer DataFrames
    advertencias_enriquecimiento: List[str] = []
    dfs_enriched = enrich_dataframes(
        processed_dfs, advertencias_enriquecimiento)
    all_advertencias.extend(advertencias_enriquecimiento)
    print(
        f"\nDataFrames después del enriquecimiento: {list(dfs_enriched.keys())}")

    # 3. Unir DataFrames
    advertencias_union: List[str] = []
    dfs_joined = join_dataframes(dfs_enriched, advertencias_union)
    all_advertencias.extend(advertencias_union)
    print(f"\nDataFrames después de la unión: {list(dfs_joined.keys())}")

    # 4. Realizar análisis de perfil de pacientes
    df_analisis_final = dfs_joined.get('df_analisis_final')
    if df_analisis_final is not None and not df_analisis_final.empty:
        analysis_results = analyze_patient_profile(
            df_analisis_final, all_advertencias)
        # Añadir los resultados del análisis al diccionario principal de DFs
        for key, df in analysis_results.items():
            dfs_joined[key] = df
    else:
        all_advertencias.append(
            "ADVERTENCIA: df_analisis_final está vacío. No se pudo realizar el análisis de perfil.")
        print("ADVERTENCIA: df_analisis_final está vacío. No se pudo realizar el análisis de perfil.")

    print("\n--- Resumen de Advertencias ---")
    if all_advertencias:
        for adv in all_advertencias:
            print(adv)
    else:
        print("No se encontraron advertencias.")
    print("-------------------------------\n")

    return dfs_joined, all_advertencias


# Bloque para ejecutar la función de procesamiento si el script se ejecuta directamente (para pruebas locales)
if __name__ == "__main__":
    print("Este script está diseñado para ser importado como un módulo por 'main.py'.")
    print("Las rutas de archivos se gestionarán de forma temporal por FastAPI en el servidor.")
    # Si quieres probarlo localmente sin FastAPI, necesitarías crear archivos de prueba.
    # Ejemplo de cómo podrías probarlo localmente (requiere archivos de prueba):
    # import os
    # try:
    #     current_script_path = os.path.dirname(os.path.abspath(__file__))
    #     # Asumiendo que 'data' es una carpeta en el mismo nivel que tu proyecto principal
    #     # y 'indice.xlsx - Hoja1.csv' está en la raíz del proyecto o en una carpeta específica.
    #     # Ajusta estas rutas según tu estructura de carpetas local para las pruebas.
    #     # data_folder_for_testing = os.path.join(current_script_path, 'temp_excel_data_for_test')
    #     # index_file_for_testing = os.path.join(current_script_path, 'indice.xlsx - Hoja1.csv')
    #
    #     # if os.path.exists(data_folder_for_testing) and os.path.exists(index_file_for_testing):
    #     #     print(f"Probando la ejecución local con data_folder: {data_folder_for_testing} y index_file: {index_file_for_testing}")
    #     #     processed_dataframes, all_warnings = run_data_processing(data_folder_for_testing, index_file_for_testing)
    #     #     print("\n--- Resultados del procesamiento local (primeros 5 registros) ---")
    #     #     for df_name, df in processed_dataframes.items():
    #     #         print(f"\nDataFrame: {df_name}")
    #     #         if not df.empty:
    #     #             print(df.head())
    #     #         else:
    #     #             print("  Vacío")
    #     # else:
    #     #     print("Advertencia: No se encontraron las rutas de prueba para los archivos de datos e índice.")
    #     #     print(f"Verificar: {data_folder_for_testing} y {index_file_for_testing}")
    # except Exception as e:
    #     print(f"Error durante la ejecución de prueba: {e}")

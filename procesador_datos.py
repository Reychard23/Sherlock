import pandas as pd
import numpy as np
import io
from typing import Dict, Any, List, Tuple, Set, Optional

# --- Función 1: Carga y Limpieza Inicial ---


def load_dataframes_from_uploads(
    data_files: List[Any],
    index_file: Any,
) -> Tuple[Dict[str, pd.DataFrame], Dict[tuple[str, str], dict[str, str]], set[tuple[str, str, str]], List[str]]:
    processed_dfs: Dict[str, pd.DataFrame] = {}
    advertencias_carga: List[str] = []
    rename_map_details: Dict[tuple[str, str], dict[str, str]] = {}
    drop_columns_set: set[tuple[str, str, str]] = set()

    print(
        f"--- Log Sherlock (BG Task - load_data): Intentando leer índice (Excel) desde: {index_file.filename}")
    try:
        index_file.file.seek(0)
        content_indice_bytes = index_file.file.read()
        indice_df = pd.read_excel(io.BytesIO(
            content_indice_bytes), sheet_name=0)
        print(
            "--- Log Sherlock (BG Task - load_data): Índice (Excel) cargado correctamente.")

        col_map = {
            'Archivo_Idx': 'Archivo', 'Hoja_Idx': 'Sheet', 'Original_Idx': 'Columna',
            'Nuevo_Idx': 'Nombre unificado', 'Accion_Idx': 'Acción'
        }
        columnas_requeridas_en_indice = list(col_map.values())
        if not all(col in indice_df.columns for col in columnas_requeridas_en_indice):
            missing_cols = [
                col for col in columnas_requeridas_en_indice if col not in indice_df.columns]
            msg = f"ADVERTENCIA CRÍTICA: Faltan columnas en índice (Excel): {missing_cols}."
            advertencias_carga.append(msg)
            print(msg)
            return {}, {}, set(), advertencias_carga

        for _, row in indice_df.iterrows():
            nombre_archivo_indice_con_ext = str(
                row[col_map['Archivo_Idx']]).strip()
            archivo_base_idx = nombre_archivo_indice_con_ext[:-5] if nombre_archivo_indice_con_ext.lower().endswith(".xlsx") else \
                (nombre_archivo_indice_con_ext[:-4] if nombre_archivo_indice_con_ext.lower(
                ).endswith(".xls") else nombre_archivo_indice_con_ext)
            hoja_idx = str(row[col_map['Hoja_Idx']]).strip() if pd.notna(
                row[col_map['Hoja_Idx']]) else 'default'
            original_col_idx = str(row[col_map['Original_Idx']]).strip()
            nuevo_col_idx = str(row[col_map['Nuevo_Idx']]).strip()
            accion_val_idx = str(row[col_map['Accion_Idx']]).strip().upper()

            if accion_val_idx == 'DROP':
                drop_columns_set.add(
                    (archivo_base_idx, hoja_idx, original_col_idx))
            elif accion_val_idx == 'KEEP':
                if pd.notna(nuevo_col_idx) and nuevo_col_idx != original_col_idx:
                    if (archivo_base_idx, hoja_idx) not in rename_map_details:
                        rename_map_details[(archivo_base_idx, hoja_idx)] = {}
                    rename_map_details[(archivo_base_idx, hoja_idx)
                                       ][original_col_idx] = nuevo_col_idx

        print(
            f"--- Log Sherlock (BG Task - load_data): Rename map ({len(rename_map_details)}) y Drop set ({len(drop_columns_set)}) construidos.")
    except Exception as e:
        msg = f"ERROR CRÍTICO al leer índice (Excel) '{index_file.filename}': {e}"
        advertencias_carga.append(msg)
        print(msg)
        import traceback
        traceback.print_exc()
        return {}, {}, set(), advertencias_carga

    print(
        f"--- Log Sherlock (BG Task - load_data): Iniciando procesamiento de {len(data_files)} archivos de datos...")
    for i_file, uploaded_file_obj in enumerate(data_files):
        original_filename_con_ext = uploaded_file_obj.filename
        print(
            f"--- Log Sherlock (BG Task - load_data): ({i_file+1}/{len(data_files)}) Procesando: {original_filename_con_ext}")
        try:
            base_name_uploaded_file = original_filename_con_ext[:-5] if original_filename_con_ext.lower(
            ).endswith(".xlsx") else original_filename_con_ext.split('.')[0]
            df_sheets: Dict[str, pd.DataFrame] = {}
            try:
                uploaded_file_obj.file.seek(0)
                file_content_bytes = uploaded_file_obj.file.read()
                df_sheets = pd.read_excel(io.BytesIO(
                    file_content_bytes), sheet_name=None)
            except Exception as e_read_excel:
                print(
                    f"    ERROR al leer Excel '{original_filename_con_ext}': {e_read_excel}. Intentando como CSV...")
                try:
                    uploaded_file_obj.file.seek(0)
                    file_content_bytes_csv = uploaded_file_obj.file.read()
                    df_temp = pd.read_csv(io.BytesIO(
                        file_content_bytes_csv), sep=None, engine='python', encoding='utf-8-sig', on_bad_lines='warn')
                    df_sheets['default_csv_sheet'] = df_temp
                    print(f"    '{original_filename_con_ext}' leído como CSV.")
                except Exception as e_read_csv:
                    advertencias_carga.append(
                        f"ERROR: No se pudo leer '{original_filename_con_ext}' como Excel ni CSV: {e_read_csv}")
                    print(
                        f"    ERROR al leer '{original_filename_con_ext}' como Excel y CSV: {e_read_csv}")
                    continue

            print(
                f"    '{original_filename_con_ext}' leído. Hojas encontradas: {list(df_sheets.keys())}")
            for sheet_name_actual_excel, df_original in df_sheets.items():
                print(
                    f"    Inspeccionando hoja: '{sheet_name_actual_excel}' de '{original_filename_con_ext}'")

                fila_indice_para_hoja = indice_df[
                    (indice_df[col_map['Archivo_Idx']].str.replace(r'\.xlsx?$', '', case=False, regex=True) == base_name_uploaded_file) &
                    (indice_df[col_map['Hoja_Idx']] == sheet_name_actual_excel)
                ]
                if fila_indice_para_hoja.empty:
                    print(
                        f"      Hoja '{sheet_name_actual_excel}' no en índice para '{base_name_uploaded_file}'. Omitiendo.")
                    continue

                print(
                    f"      Procesando hoja '{sheet_name_actual_excel}' según el índice.")
                df_cleaned = df_original.copy()
                current_rename_dict = rename_map_details.get((base_name_uploaded_file, sheet_name_actual_excel),
                                                             rename_map_details.get((base_name_uploaded_file, 'default'), {}))
                df_cleaned.rename(columns=current_rename_dict, inplace=True)

                cols_originales_a_eliminar_hoja_actual = {
                    col_name for (ab_idx, hoja_idx, col_name) in drop_columns_set
                    if ab_idx == base_name_uploaded_file and (hoja_idx == sheet_name_actual_excel or hoja_idx == 'default')
                }
                columnas_finales_a_dropear_en_df_cleaned = []
                for original_col_to_drop in cols_originales_a_eliminar_hoja_actual:
                    nombre_actual_de_col_a_dropear = current_rename_dict.get(
                        original_col_to_drop, original_col_to_drop)
                    if nombre_actual_de_col_a_dropear in df_cleaned.columns:
                        columnas_finales_a_dropear_en_df_cleaned.append(
                            nombre_actual_de_col_a_dropear)
                if columnas_finales_a_dropear_en_df_cleaned:
                    df_cleaned.drop(columns=list(
                        set(columnas_finales_a_dropear_en_df_cleaned)), inplace=True, errors='ignore')
                    print(
                        f"        Columnas eliminadas: {len(columnas_finales_a_dropear_en_df_cleaned)}")

                df_key_name = f"{base_name_uploaded_file}_df"
                if df_key_name in processed_dfs:
                    advertencias_carga.append(
                        f"ADVERTENCIA: DF '{df_key_name}' (de '{original_filename_con_ext}') ya existe. Se SOBREESCRIBIRÁ.")
                processed_dfs[df_key_name] = df_cleaned
                print(f"      DataFrame '{df_key_name}' almacenado.")
                break
        except Exception as e_file:
            msg = f"ERROR procesando archivo de datos '{original_filename_con_ext}': {e_file}"
            advertencias_carga.append(msg)
            print(msg)
            import traceback
            traceback.print_exc()

    if not processed_dfs:
        advertencias_carga.append(
            "ADVERTENCIA CRÍTICA: No se procesó ningún DataFrame de datos.")
    print("--- Log Sherlock (BG Task - load_data): FIN de carga y limpieza inicial de DataFrames ---")
    return processed_dfs, rename_map_details, drop_columns_set, advertencias_carga

# --- Función 2: Utilidad para Obtener DF ---


def get_df_by_type(
    processed_dfs: Dict[str, pd.DataFrame],
    df_key_buscado: str,
    advertencias_list: List[str]
) -> Optional[pd.DataFrame]:
    if df_key_buscado in processed_dfs:
        print(
            f"--- Log Sherlock (BG Task - get_df): DataFrame '{df_key_buscado}' encontrado.")
        return processed_dfs[df_key_buscado].copy()
    else:
        msg = f"ADVERTENCIA en get_df_by_type: No se encontró DataFrame '{df_key_buscado}'. Disponibles: {list(processed_dfs.keys())}"
        advertencias_list.append(msg)
        print(msg)
        return None

# --- Función 3: El Cerebro del Procesamiento y Enriquecimiento ---


def generar_insights_pacientes(
    processed_dfs: Dict[str, pd.DataFrame],
    all_advertencias: List[str]
) -> Dict[str, pd.DataFrame]:

    resultados_dfs: Dict[str, pd.DataFrame] = {}
    print("--- Log Sherlock (BG Task - MODO DIAGNÓSTICO): Inicio de generar_insights_pacientes ---")
    print(
        f"--- Log Sherlock (BG Task - MODO DIAGNÓSTICO): DataFrames limpios disponibles: {list(processed_dfs.keys())}")

    # --- INICIO DEL DIAGNÓSTICO DE COLUMNAS ---
    # Este bloque solo imprimirá las columnas de cada DataFrame importante y luego los guardará
    # en Supabase con un prefijo "diagnostico_" para que podamos inspeccionarlos.
    print("\n--- INICIO DIAGNÓSTICO DE COLUMNAS DE DATAFRAMES LIMPIOS ---")

    dataframes_a_inspeccionar = [
        "Pacientes_Nuevos_df", "Tipos de pacientes_df", "Lada_df",
        "Citas_Pacientes_df", "Citas_Motivo_df", "Presupuesto por Accion_df",
        "Acciones_df", "Movimiento_df", "Tabla_Procedimientos_df", "Sucursal_df",
        "Tratamiento Generado Mex_df"
    ]

    for df_key in dataframes_a_inspeccionar:
        df_a_inspeccionar = get_df_by_type(
            processed_dfs, df_key, all_advertencias)

        if df_a_inspeccionar is not None:
            # Imprimir la lista de columnas en los logs de Render
            print(
                f"\n--- Log Sherlock (DIAGNÓSTICO): Columnas para '{df_key}' ---")
            column_list = df_a_inspeccionar.columns.tolist()
            print(column_list)

            # Guardamos una copia del DataFrame limpio para inspeccionarlo en Supabase
            resultados_dfs[f"diagnostico_{df_key.lower()}"] = df_a_inspeccionar.copy(
            )
        else:
            print(
                f"\n--- Log Sherlock (DIAGNÓSTICO): DataFrame '{df_key}' NO fue encontrado en processed_dfs.\n")

    print("--- FIN DIAGNÓSTICO DE COLUMNAS ---\n")
    # --- FIN DEL DIAGNÓSTICO ---

    all_advertencias.append(
        "MODO DIAGNÓSTICO COMPLETADO: Se imprimieron las columnas de los DFs limpios en los logs. Revisa los logs de Render.")
    print("--- Log Sherlock (BG Task - MODO DIAGNÍSTICO): Fin. Devolviendo DFs limpios para inspección.")

    return resultados_dfs

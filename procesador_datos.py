import pandas as pd
import numpy as np
import io
from typing import Dict, Any, List, Tuple, Set, Optional

# ---------------------------------------------------------------------------
# FUNCIÓN load_dataframes_from_uploads
# (Esta es la que lee el índice y los archivos de datos,
# los limpia según el índice y devuelve el diccionario `processed_dfs`
# con los DataFrames "_df")
# Debes usar la ÚLTIMA VERSIÓN de esta función que te proporcioné,
# la que maneja correctamente los nombres de columna de tu indice.xlsx real
# y la lógica de procesar solo la hoja especificada en el índice por archivo.
# ---------------------------------------------------------------------------


def load_dataframes_from_uploads(
    data_files: List[Any],
    index_file: Any,
) -> Tuple[Dict[str, pd.DataFrame], Dict[tuple[str, str], dict[str, str]], set[tuple[str, str, str]], List[str]]:
    # ... (TODO EL CÓDIGO COMPLETO DE ESTA FUNCIÓN COMO LO ACORDAMOS)
    # ... (Incluye la lectura de indice.xlsx, el col_map, el bucle por data_files,
    #      la lectura de hojas, el renombrado, la eliminación,
    #      y la creación de processed_dfs con claves como "NombreArchivo_df")
    # ...
    # Ejemplo de la parte final de esta función:
    # print("--- Log Sherlock (BG Task - load_data): FIN de carga y limpieza inicial de DataFrames ---")
    # return processed_dfs, rename_map_details, drop_columns_set, advertencias_carga

    # --- COPIA AQUÍ LA ÚLTIMA VERSIÓN COMPLETA DE load_dataframes_from_uploads QUE TE DI ---
    # Asegúrate de que esta función devuelva `processed_dfs` correctamente.
    # (La que te di en la respuesta donde afinamos la lectura del índice y el procesamiento de una hoja)
    processed_dfs: Dict[str, pd.DataFrame] = {}
    advertencias_carga: List[str] = []
    # Aunque no lo uses directamente en generar_insights, la firma lo pide
    rename_map_details: Dict[tuple[str, str], dict[str, str]] = {}
    # Aunque no lo uses directamente en generar_insights, la firma lo pide
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
            'Archivo_Idx': 'Archivo',
            'Hoja_Idx': 'Sheet',
            'Original_Idx': 'Columna',
            'Nuevo_Idx': 'Nombre unificado',
            'Accion_Idx': 'Acción'
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
                        f"ADVERTENCIA: DF '{df_key_name}' (de '{original_filename_con_ext}', hoja '{sheet_name_actual_excel}') ya existe. Se SOBREESCRIBIRÁ.")
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


# ---------------------------------------------------------------------------
# FUNCIÓN get_df_by_type
# (Pequeña utilidad para obtener un DF del diccionario `processed_dfs`)
# ---------------------------------------------------------------------------
def get_df_by_type(
    processed_dfs: Dict[str, pd.DataFrame],
    df_key_buscado: str,
    advertencias_list: List[str]
) -> Optional[pd.DataFrame]:
    # ... (CÓDIGO COMPLETO DE ESTA FUNCIÓN COMO LO ACORDAMOS) ...
    # (La que busca por "NombreArchivo_df")
    if df_key_buscado in processed_dfs:
        print(
            f"--- Log Sherlock (BG Task - get_df): DataFrame '{df_key_buscado}' encontrado.")
        return processed_dfs[df_key_buscado].copy()
    else:
        msg = f"ADVERTENCIA CRÍTICA en get_df_by_type: No se encontró el DataFrame con clave '{df_key_buscado}'. Disponibles: {list(processed_dfs.keys())}"
        advertencias_list.append(msg)
        print(msg)
        return None

# ---------------------------------------------------------------------------
# FUNCIÓN generar_insights_pacientes (LA QUE TE ACABO DE DAR EN LA RESPUESTA ANTERIOR)
# (Esta es la función grande que toma `processed_dfs` y hace todos los
#  enriquecimientos, merges, cálculos de edad, perfiles, y prepara
#  TODOS los DataFrames finales para Supabase)
# ---------------------------------------------------------------------------


def generar_insights_pacientes(
    processed_dfs: Dict[str, pd.DataFrame],
    all_advertencias: List[str]
) -> Dict[str, pd.DataFrame]:

    # --- COPIA AQUÍ EL CÓDIGO COMPLETO DE LA FUNCIÓN generar_insights_pacientes
    # --- QUE TE DI EN MI RESPUESTA INMEDIATAMENTE ANTERIOR.
    # --- (La que empieza con "resultados_dfs: Dict[str, pd.DataFrame] = {}"
    # --- y termina con "return resultados_dfs")
    resultados_dfs: Dict[str, pd.DataFrame] = {}
    print(
        f"--- Log Sherlock (BG Task - generar_insights): Inicio. DF limpios disponibles: {list(processed_dfs.keys())}")

    # --- 1. Cargar y Guardar Tablas de Dimensiones "Tal Cual" (después de limpieza del índice) ---
    dimension_mapping = {
        "Tipos de pacientes_df": "dimension_tipos_pacientes",
        "Tabla_Procedimientos_df": "dimension_procedimientos",
        "Sucursal_df": "dimension_sucursales",
        "Lada_df": "dimension_lada",
        "Tratamiento Generado Mex_df": "dimension_tratamientos_generados"
    }
    for df_key, table_name in dimension_mapping.items():
        df_dim = get_df_by_type(processed_dfs, df_key, all_advertencias)
        if df_dim is not None:
            # Antes de guardar, asegurar que no haya columnas duplicadas si el índice es el mismo
            # Esto es más para DFs de hechos, pero por si acaso.
            # Para dimensiones, usualmente queremos todas sus columnas.
            resultados_dfs[table_name] = df_dim.copy()
            print(
                f"--- Log Sherlock (BG Task - generar_insights): Dimensión '{table_name}' preparada para guardar ({len(df_dim)} filas).")
        else:
            all_advertencias.append(
                f"Advertencia: No se encontró el DataFrame base para la dimensión '{table_name}' (esperaba clave '{df_key}').")

    # --- 2. Procesar y Enriquecer DataFrame de Pacientes ---
    df_pacientes_base = get_df_by_type(
        processed_dfs, "Pacientes_Nuevos_df", all_advertencias)
    if df_pacientes_base is None:
        all_advertencias.append(
            "ERROR CRÍTICO: 'Pacientes_Nuevos_df' no disponible. Muchos análisis fallarán.")
        # No podemos continuar sin pacientes si los perfiles dependen de ellos
        # y otros hechos también.
        return resultados_dfs  # Devolver DFs de dimensión cargados hasta ahora
    else:
        df_pacientes_enriquecido = df_pacientes_base.copy()
        print(
            f"--- Log Sherlock (BG Task - generar_insights): Base Pacientes ('Pacientes_Nuevos_df'): {len(df_pacientes_enriquecido)} filas.")

        # a. Calcular Edad
        col_fecha_nac_unificado = 'Fecha de nacimiento'
        if col_fecha_nac_unificado in df_pacientes_enriquecido.columns:
            try:
                df_pacientes_enriquecido[col_fecha_nac_unificado] = pd.to_datetime(
                    df_pacientes_enriquecido[col_fecha_nac_unificado], errors='coerce')
                if not df_pacientes_enriquecido[col_fecha_nac_unificado].isnull().all():
                    current_time_mex = pd.Timestamp.now(
                        tz='America/Mexico_City')
                    if df_pacientes_enriquecido[col_fecha_nac_unificado].dt.tz is None:
                        df_pacientes_enriquecido[col_fecha_nac_unificado] = df_pacientes_enriquecido[col_fecha_nac_unificado].dt.tz_localize(
                            'America/Mexico_City', ambiguous='infer', nonexistent='NaT')
                    else:
                        df_pacientes_enriquecido[col_fecha_nac_unificado] = df_pacientes_enriquecido[col_fecha_nac_unificado].dt.tz_convert(
                            'America/Mexico_City')
                    time_difference = current_time_mex - \
                        df_pacientes_enriquecido[col_fecha_nac_unificado]
                    if not time_difference.isnull().all():
                        df_pacientes_enriquecido['Edad_float'] = time_difference / \
                            pd.Timedelta(days=1) / 365.25
                        df_pacientes_enriquecido['Edad'] = df_pacientes_enriquecido['Edad_float'].astype(
                            'Int64')
                        print(
                            f"--- Log Sherlock (BG Task - generar_insights): Columna 'Edad' calculada para {len(df_pacientes_enriquecido[df_pacientes_enriquecido['Edad'].notna()])} pacientes.")
                    else:
                        all_advertencias.append(
                            "Advertencia: 'time_difference' para Edad de pacientes es NaT.")
                        df_pacientes_enriquecido['Edad'] = pd.NA
                else:
                    all_advertencias.append(
                        f"Advertencia: Columna '{col_fecha_nac_unificado}' sin fechas válidas para Edad.")
                    df_pacientes_enriquecido['Edad'] = pd.NA
            except Exception as e_edad:
                all_advertencias.append(
                    f"Advertencia: Error al calcular Edad: {e_edad}")
                df_pacientes_enriquecido['Edad'] = pd.NA
                import traceback
                print("--- TRACEBACK ERROR EDAD (Pacientes) ---")
                traceback.print_exc()
                print("--------------------------")
        else:
            all_advertencias.append(
                f"Advertencia: Columna '{col_fecha_nac_unificado}' no encontrada en Pacientes para Edad.")
            df_pacientes_enriquecido['Edad'] = pd.NA

        # b. Enriquecer Pacientes con Origen
        col_tipo_dentalink_pac = 'Tipo Dentalink'
        col_origen_pac_catalogo = 'Paciente_Origen'
        if 'dimension_tipos_pacientes' in resultados_dfs:
            df_dim_tipos_pac = resultados_dfs['dimension_tipos_pacientes']
            if col_tipo_dentalink_pac in df_pacientes_enriquecido.columns and \
               col_tipo_dentalink_pac in df_dim_tipos_pac.columns and \
               col_origen_pac_catalogo in df_dim_tipos_pac.columns:
                df_pacientes_enriquecido[col_tipo_dentalink_pac] = df_pacientes_enriquecido[col_tipo_dentalink_pac].astype(
                    str)
                df_dim_tipos_pac[col_tipo_dentalink_pac] = df_dim_tipos_pac[col_tipo_dentalink_pac].astype(
                    str)
                df_origen_para_merge = df_dim_tipos_pac[[
                    col_tipo_dentalink_pac, col_origen_pac_catalogo]].drop_duplicates(subset=[col_tipo_dentalink_pac])
                df_pacientes_enriquecido = pd.merge(
                    df_pacientes_enriquecido, df_origen_para_merge, on=col_tipo_dentalink_pac, how='left')
                print(
                    f"--- Log Sherlock (BG Task - generar_insights): Pacientes enriquecidos con '{col_origen_pac_catalogo}'.")
            else:
                all_advertencias.append(
                    f"Advertencia: Faltan cols para merge Pacientes y Tipos de Pacientes ('{col_tipo_dentalink_pac}', '{col_origen_pac_catalogo}').")
        else:
            all_advertencias.append(
                "Advertencia: 'dimension_tipos_pacientes' no disponible para enriquecer origen.")

        # c. Enriquecer Pacientes con Ciudad (desde dimension_lada)
        col_celular_pac = 'Celular'
        col_ciudad_lada = 'Ciudad'
        if 'dimension_lada' in resultados_dfs:
            df_dim_lada = resultados_dfs['dimension_lada']
            if col_celular_pac in df_pacientes_enriquecido.columns and \
               col_celular_pac in df_dim_lada.columns and \
               col_ciudad_lada in df_dim_lada.columns:
                # Para el merge con LADA, usualmente se extrae el prefijo del celular del paciente.
                # Esta es una simplificación y puede necesitar una lógica más robusta para extraer la LADA del número completo.
                # Asumiendo que 'Celular' en Lada_df son los prefijos o números completos que matchean.
                try:
                    df_pacientes_enriquecido[col_celular_pac] = df_pacientes_enriquecido[col_celular_pac].astype(
                        str).str.strip()
                    df_dim_lada[col_celular_pac] = df_dim_lada[col_celular_pac].astype(
                        str).str.strip()

                    # Si Lada_df.Celular son prefijos, necesitas extraer el prefijo del Celular del paciente.
                    # Ejemplo: df_pacientes_enriquecido['LADA_EXTRAIDA'] = df_pacientes_enriquecido[col_celular_pac].str[:N] (N=longitud de LADA)
                    # Y luego el merge sería on left_on='LADA_EXTRAIDA', right_on=col_celular_pac (de Lada)
                    # Por simplicidad, intentamos un merge directo si los 'Celular' pudieran coincidir.
                    df_ciudad_para_merge = df_dim_lada[[
                        col_celular_pac, col_ciudad_lada]].drop_duplicates(subset=[col_celular_pac])
                    df_pacientes_enriquecido = pd.merge(
                        df_pacientes_enriquecido, df_ciudad_para_merge, on=col_celular_pac, how='left', suffixes=('', '_dlada'))

                    # Manejar posible colisión de nombres para 'Ciudad'
                    if f'{col_ciudad_lada}_dlada' in df_pacientes_enriquecido.columns:
                        if col_ciudad_lada not in df_pacientes_enriquecido.columns:  # Si no existía la columna 'Ciudad'
                            df_pacientes_enriquecido.rename(
                                columns={f'{col_ciudad_lada}_dlada': col_ciudad_lada}, inplace=True)
                        # Si ya existía 'Ciudad', la de LADA tiene prioridad (o la lógica que definas)
                        else:
                            df_pacientes_enriquecido[col_ciudad_lada] = df_pacientes_enriquecido[
                                f'{col_ciudad_lada}_dlada']
                            df_pacientes_enriquecido.drop(
                                columns=[f'{col_ciudad_lada}_dlada'], inplace=True)
                    print(
                        f"--- Log Sherlock (BG Task - generar_insights): Pacientes enriquecidos con '{col_ciudad_lada}' de Lada.")
                except Exception as e_lada:
                    all_advertencias.append(
                        f"Advertencia: Error en merge con Lada: {e_lada}")
            else:
                all_advertencias.append(
                    f"Advertencia: Faltan cols para merge Pacientes y Lada ('{col_celular_pac}', '{col_ciudad_lada}').")
        else:
            all_advertencias.append(
                "Advertencia: 'dimension_lada' no disponible para enriquecer ciudad.")

        resultados_dfs['hechos_pacientes'] = df_pacientes_enriquecido.copy()
        print(
            f"--- Log Sherlock (BG Task - generar_insights): 'hechos_pacientes' preparado con {len(df_pacientes_enriquecido)} filas.")

    # --- 4. Procesar y Enriquecer DataFrame de Citas ---
    print(f"--- Log Sherlock (BG Task - generar_insights): Iniciando procesamiento de Citas...")
    df_citas_pac_base = get_df_by_type(
        processed_dfs, "Citas_Pacientes_df", all_advertencias)
    df_citas_mot_base = get_df_by_type(
        processed_dfs, "Citas_Motivo_df", all_advertencias)
    hechos_citas_df = None  # Inicializamos

    if df_citas_pac_base is not None and df_citas_mot_base is not None:
        col_id_cita = 'ID_Cita'
        col_id_paciente_citas = 'ID_Paciente'
        col_fecha_cita = 'Fecha Cita'
        col_cita_asistida_unif = 'Cita_asistida'
        col_cita_duplicada_unif = 'Cita duplicada'
        col_fecha_creacion_cita_unif = 'Fecha de creación cita'
        col_sucursal_cita_unif = 'Sucursal'
        col_id_trat_cita_unif = 'ID_Tratamiento'

        cols_nec_citaspac = [col_id_cita, col_id_paciente_citas,
                             col_fecha_cita, col_cita_asistida_unif, col_cita_duplicada_unif]
        cols_nec_citasmot = [col_id_cita, col_fecha_creacion_cita_unif,
                             col_sucursal_cita_unif, col_id_trat_cita_unif, col_id_paciente_citas, col_fecha_cita]

        if not all(c in df_citas_pac_base.columns for c in cols_nec_citaspac):
            all_advertencias.append(
                f"ADVERTENCIA: Faltan cols en Citas_Pacientes_df: {[c for c in cols_nec_citaspac if c not in df_citas_pac_base.columns]}.")
        elif not all(c in df_citas_mot_base.columns for c in cols_nec_citasmot):
            all_advertencias.append(
                f"ADVERTENCIA: Faltan cols en Citas_Motivo_df: {[c for c in cols_nec_citasmot if c not in df_citas_mot_base.columns]}.")
        else:
            df_citas_pac_base[col_fecha_cita] = pd.to_datetime(
                df_citas_pac_base[col_fecha_cita], errors='coerce')
            try:
                df_citas_pac_base[col_cita_asistida_unif] = pd.to_numeric(
                    df_citas_pac_base[col_cita_asistida_unif], errors='coerce').fillna(0).astype(int)
                df_citas_pac_base[col_cita_duplicada_unif] = pd.to_numeric(
                    df_citas_pac_base[col_cita_duplicada_unif], errors='coerce').fillna(0).astype(int)
            except Exception as e_conv_bool:
                all_advertencias.append(
                    f"Error convirtiendo '{col_cita_asistida_unif}' o '{col_cita_duplicada_unif}' a int: {e_conv_bool}")

            df_citas_pac_filtrado = df_citas_pac_base[df_citas_pac_base[col_cita_duplicada_unif] == 0].copy(
            )
            print(
                f"--- Log Sherlock (BG Task - generar_insights): Citas_Pacientes_df filtrado (sin duplicados): {len(df_citas_pac_filtrado)} filas.")

            df_citas_mot_base[col_fecha_creacion_cita_unif] = pd.to_datetime(
                df_citas_mot_base[col_fecha_creacion_cita_unif], errors='coerce')
            if col_fecha_cita in df_citas_mot_base.columns:
                df_citas_mot_base[col_fecha_cita] = pd.to_datetime(
                    df_citas_mot_base[col_fecha_cita], errors='coerce')

            df_citas_pac_filtrado[col_id_cita] = df_citas_pac_filtrado[col_id_cita].astype(
                str)
            df_citas_mot_base[col_id_cita] = df_citas_mot_base[col_id_cita].astype(
                str)

            columnas_de_citas_motivo_a_unir = [col_id_cita, col_fecha_creacion_cita_unif, 'Hora Inicio Cita',
                                               'Hora Fin Cita', 'Motivo Cita', col_sucursal_cita_unif, col_id_trat_cita_unif]
            columnas_de_citas_motivo_a_unir_existentes = [
                c for c in columnas_de_citas_motivo_a_unir if c in df_citas_mot_base.columns]

            hechos_citas_df = pd.merge(
                df_citas_pac_filtrado,
                df_citas_mot_base[columnas_de_citas_motivo_a_unir_existentes].drop_duplicates(
                    subset=[col_id_cita]),
                on=col_id_cita, how='left'
            )
            print(
                f"--- Log Sherlock (BG Task - generar_insights): Merge Citas_Pacientes y Citas_Motivo. Filas: {len(hechos_citas_df)}")

            df_citas_atendidas_calc = hechos_citas_df[(
                hechos_citas_df[col_cita_asistida_unif] == 1) & pd.notna(hechos_citas_df[col_fecha_cita])]
            if not df_citas_atendidas_calc.empty:
                primera_cita_atendida = df_citas_atendidas_calc.groupby(
                    col_id_paciente_citas)[col_fecha_cita].min().reset_index()
                primera_cita_atendida.rename(
                    columns={col_fecha_cita: 'Fecha_Primera_Cita_Atendida_Real'}, inplace=True)
                hechos_citas_df[col_id_paciente_citas] = hechos_citas_df[col_id_paciente_citas].astype(
                    str)
                primera_cita_atendida[col_id_paciente_citas] = primera_cita_atendida[col_id_paciente_citas].astype(
                    str)
                hechos_citas_df = pd.merge(
                    hechos_citas_df, primera_cita_atendida, on=col_id_paciente_citas, how='left')
                print(
                    f"--- Log Sherlock (BG Task - generar_insights): 'Fecha_Primera_Cita_Atendida_Real' unida a citas.")
            else:
                all_advertencias.append(
                    "Advertencia: No hay citas atendidas para calcular 'Fecha_Primera_Cita_Atendida_Real'.")
                hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'] = pd.NaT

            today = pd.Timestamp('today').normalize()
            hechos_citas_df['Etiqueta_Cita_Paciente'] = 'Indeterminada'
            hechos_citas_df[col_fecha_cita] = pd.to_datetime(
                hechos_citas_df[col_fecha_cita], errors='coerce')

            cond_nunca_atendido_antes = hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].isnull(
            )
            cond_esta_es_primera_atendida = (hechos_citas_df[col_fecha_cita].notnull() & hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].notnull() &
                                             (hechos_citas_df[col_fecha_cita] == hechos_citas_df['Fecha_Primera_Cita_Atendida_Real']))
            cond_paciente_es_nuevo_para_esta_cita = cond_nunca_atendido_antes | cond_esta_es_primera_atendida
            hechos_citas_df.loc[cond_paciente_es_nuevo_para_esta_cita & (
                hechos_citas_df[col_fecha_cita] >= today), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo en Agenda"
            hechos_citas_df.loc[cond_paciente_es_nuevo_para_esta_cita & (hechos_citas_df[col_fecha_cita] < today) & (
                hechos_citas_df[col_cita_asistida_unif] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo Atendido"
            hechos_citas_df.loc[cond_paciente_es_nuevo_para_esta_cita & (hechos_citas_df[col_fecha_cita] < today) & (
                hechos_citas_df[col_cita_asistida_unif] == 0), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo No Atendido"
            cond_paciente_es_recurrente = (~cond_paciente_es_nuevo_para_esta_cita) & (hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].notnull()) & \
                                          (hechos_citas_df[col_fecha_cita].notnull() & (
                                              hechos_citas_df[col_fecha_cita] > hechos_citas_df['Fecha_Primera_Cita_Atendida_Real']))
            cond_mismo_mes_debut = hechos_citas_df[col_fecha_cita].notnull() & hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].notnull() & \
                (hechos_citas_df[col_fecha_cita].dt.to_period(
                    'M') == hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].dt.to_period('M'))
            hechos_citas_df.loc[cond_paciente_es_recurrente & cond_mismo_mes_debut & (
                hechos_citas_df[col_cita_asistida_unif] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Atendido Mismo Mes que Debutó"
            hechos_citas_df.loc[cond_paciente_es_recurrente & (~cond_mismo_mes_debut) & (
                hechos_citas_df[col_fecha_cita] >= today), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente en Agenda"
            hechos_citas_df.loc[cond_paciente_es_recurrente & (~cond_mismo_mes_debut) & (hechos_citas_df[col_fecha_cita] < today) & (
                hechos_citas_df[col_cita_asistida_unif] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente Atendido"
            hechos_citas_df.loc[cond_paciente_es_recurrente & (~cond_mismo_mes_debut) & (hechos_citas_df[col_fecha_cita] < today) & (
                hechos_citas_df[col_cita_asistida_unif] == 0), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente No Atendido"

            print(
                f"--- Log Sherlock (BG Task - generar_insights): Etiquetas de citas calculadas. Distribución:\n{hechos_citas_df['Etiqueta_Cita_Paciente'].value_counts(dropna=False)}")
            # Asegurar que la columna existe, aunque sea con NaT
            if 'Fecha_Primera_Cita_Atendida_Real' not in hechos_citas_df.columns:
                hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'] = pd.NaT
            resultados_dfs['hechos_citas'] = hechos_citas_df.copy()
    else:
        all_advertencias.append(
            "ADVERTENCIA: Faltan DFs base de Citas ('Citas_Pacientes_df' o 'Citas_Motivo_df').")

    # --- 5. Procesar Presupuestos (`Presupuesto por Accion_df`) ---
    df_presupuestos_base = get_df_by_type(
        processed_dfs, "Presupuesto por Accion_df", all_advertencias)
    if df_presupuestos_base is not None:
        df_presupuestos_enriquecido = df_presupuestos_base.copy()
        col_id_proc_presup = 'ID_Procedimiento'
        col_precio_orig_presup = 'Procedimiento_precio_original'
        col_precio_pac_presup = 'Procedimiento_precio_paciente'
        # Asumiendo que esta columna existe y está unificada
        col_id_pac_presup = 'ID_Paciente'

        if 'dimension_procedimientos' in resultados_dfs and col_id_proc_presup in df_presupuestos_enriquecido.columns:
            df_dim_proc = resultados_dfs['dimension_procedimientos']
            if 'ID_Procedimiento' in df_dim_proc.columns:
                df_presupuestos_enriquecido[col_id_proc_presup] = df_presupuestos_enriquecido[col_id_proc_presup].astype(
                    str)
                df_dim_proc['ID_Procedimiento'] = df_dim_proc['ID_Procedimiento'].astype(
                    str)
                cols_from_dim_proc = [
                    'Nombre_procedimiento', 'Categoria_procedimiento', 'Subcategoria_procedimiento']
                cols_from_dim_proc_exist = [
                    c for c in cols_from_dim_proc if c in df_dim_proc.columns]
                df_proc_details_merge = df_dim_proc[[
                    'ID_Procedimiento'] + cols_from_dim_proc_exist].drop_duplicates(subset=['ID_Procedimiento'])
                df_presupuestos_enriquecido = pd.merge(df_presupuestos_enriquecido, df_proc_details_merge,
                                                       left_on=col_id_proc_presup, right_on='ID_Procedimiento',
                                                       how='left', suffixes=('', '_dimproc'))
                if col_id_proc_presup != 'ID_Procedimiento' and 'ID_Procedimiento_dimproc' in df_presupuestos_enriquecido.columns:  # Chequeo post-merge
                    df_presupuestos_enriquecido.drop(
                        columns=['ID_Procedimiento_dimproc'], inplace=True)
                # Si la clave de dim_proc se llama igual que la de hechos
                elif 'ID_Procedimiento' in df_presupuestos_enriquecido.columns and col_id_proc_presup in df_presupuestos_enriquecido.columns and 'ID_Procedimiento' != col_id_proc_presup:
                    pass  # No hacer nada si los nombres de clave de merge ya eran iguales

        if col_precio_orig_presup in df_presupuestos_enriquecido.columns and col_precio_pac_presup in df_presupuestos_enriquecido.columns:
            df_presupuestos_enriquecido[col_precio_orig_presup] = pd.to_numeric(
                df_presupuestos_enriquecido[col_precio_orig_presup], errors='coerce')
            df_presupuestos_enriquecido[col_precio_pac_presup] = pd.to_numeric(
                df_presupuestos_enriquecido[col_precio_pac_presup], errors='coerce')
            df_presupuestos_enriquecido['Descuento_Presupuestado_Detalle'] = df_presupuestos_enriquecido[
                col_precio_orig_presup] - df_presupuestos_enriquecido[col_precio_pac_presup]

        if 'hechos_pacientes' in resultados_dfs and col_id_pac_presup in df_presupuestos_enriquecido.columns:
            df_pac_temp = resultados_dfs['hechos_pacientes']
            # Las que existen en hechos_pacientes
            cols_pac_a_traer = ['ID_Paciente',
                                'Paciente_Origen', 'Edad', 'Sexo', 'Ciudad']
            cols_pac_a_traer_exist = [
                c for c in cols_pac_a_traer if c in df_pac_temp.columns]

            df_presupuestos_enriquecido[col_id_pac_presup] = df_presupuestos_enriquecido[col_id_pac_presup].astype(
                str)
            df_pac_temp['ID_Paciente'] = df_pac_temp['ID_Paciente'].astype(str)

            df_presupuestos_enriquecido = pd.merge(df_presupuestos_enriquecido, df_pac_temp[cols_pac_a_traer_exist].drop_duplicates(subset=['ID_Paciente']),
                                                   left_on=col_id_pac_presup, right_on='ID_Paciente',
                                                   how='left', suffixes=('', '_pacienteinfo'))
            # Limpiar IDs duplicados de paciente
            if col_id_pac_presup != 'ID_Paciente' and 'ID_Paciente_pacienteinfo' in df_presupuestos_enriquecido.columns:
                df_presupuestos_enriquecido.drop(
                    columns=['ID_Paciente_pacienteinfo'], inplace=True)
            elif 'ID_Paciente' in df_presupuestos_enriquecido.columns and col_id_pac_presup in df_presupuestos_enriquecido.columns and 'ID_Paciente' != col_id_pac_presup:
                pass

        resultados_dfs['hechos_presupuesto_detalle'] = df_presupuestos_enriquecido.copy()
        print(
            f"--- Log Sherlock (BG Task - generar_insights): 'hechos_presupuesto_detalle' preparado con {len(df_presupuestos_enriquecido)} filas.")
    else:
        all_advertencias.append(
            "INFO: DataFrame 'Presupuesto por Accion_df' no disponible.")

    # --- 6. Procesar Acciones Realizadas (`Acciones_df`) ---
    df_acciones_base = get_df_by_type(
        processed_dfs, "Acciones_df", all_advertencias)
    if df_acciones_base is not None:
        df_acciones_enriquecido = df_acciones_base.copy()
        col_id_proc_accion = 'ID_Procedimiento'
        col_id_pac_accion = 'ID_Paciente'

        if 'dimension_procedimientos' in resultados_dfs and col_id_proc_accion in df_acciones_enriquecido.columns:
            df_dim_proc = resultados_dfs['dimension_procedimientos']
            if 'ID_Procedimiento' in df_dim_proc.columns:
                df_acciones_enriquecido[col_id_proc_accion] = df_acciones_enriquecido[col_id_proc_accion].astype(
                    str)
                df_dim_proc['ID_Procedimiento'] = df_dim_proc['ID_Procedimiento'].astype(
                    str)
                cols_from_dim_proc = [
                    'Nombre_procedimiento', 'Categoria_procedimiento', 'Subcategoria_procedimiento']
                cols_from_dim_proc_exist = [
                    c for c in cols_from_dim_proc if c in df_dim_proc.columns]
                df_proc_details_merge = df_dim_proc[[
                    'ID_Procedimiento'] + cols_from_dim_proc_exist].drop_duplicates(subset=['ID_Procedimiento'])
                df_acciones_enriquecido = pd.merge(df_acciones_enriquecido, df_proc_details_merge,
                                                   left_on=col_id_proc_accion, right_on='ID_Procedimiento',
                                                   how='left', suffixes=('', '_dimproc'))
                if col_id_proc_accion != 'ID_Procedimiento' and 'ID_Procedimiento_dimproc' in df_acciones_enriquecido.columns:
                    df_acciones_enriquecido.drop(
                        columns=['ID_Procedimiento_dimproc'], inplace=True)

        if 'hechos_pacientes' in resultados_dfs and col_id_pac_accion in df_acciones_enriquecido.columns:
            df_pac_temp = resultados_dfs['hechos_pacientes']
            cols_pac_a_traer = ['ID_Paciente',
                                'Paciente_Origen', 'Edad', 'Sexo', 'Ciudad']
            cols_pac_a_traer_exist = [
                c for c in cols_pac_a_traer if c in df_pac_temp.columns]
            df_acciones_enriquecido[col_id_pac_accion] = df_acciones_enriquecido[col_id_pac_accion].astype(
                str)
            df_pac_temp['ID_Paciente'] = df_pac_temp['ID_Paciente'].astype(str)
            df_acciones_enriquecido = pd.merge(df_acciones_enriquecido, df_pac_temp[cols_pac_a_traer_exist].drop_duplicates(subset=['ID_Paciente']),
                                               left_on=col_id_pac_accion, right_on='ID_Paciente',
                                               how='left', suffixes=('', '_pacienteinfo'))
            if col_id_pac_accion != 'ID_Paciente' and 'ID_Paciente_pacienteinfo' in df_acciones_enriquecido.columns:
                df_acciones_enriquecido.drop(
                    columns=['ID_Paciente_pacienteinfo'], inplace=True)

        resultados_dfs['hechos_acciones_realizadas'] = df_acciones_enriquecido.copy()
        print(
            f"--- Log Sherlock (BG Task - generar_insights): 'hechos_acciones_realizadas' preparado con {len(df_acciones_enriquecido)} filas.")
    else:
        all_advertencias.append("INFO: DataFrame 'Acciones_df' no disponible.")

    # --- 7. Procesar Pagos (`Movimiento_df`) ---
    df_movimiento_base = get_df_by_type(
        processed_dfs, "Movimiento_df", all_advertencias)
    if df_movimiento_base is not None:
        df_movimiento_procesado = df_movimiento_base.copy()
        col_id_pago_mov = 'ID_Pago'
        col_total_pago_mov = 'Total Pago'
        col_abono_libre_mov = 'Abono Libre'
        col_pagado_detalle_mov = 'Pagado_ID_Detalle_Presupuesto'
        col_id_detalle_presup_mov = 'ID_Detalle Presupuesto'
        col_id_pac_mov = 'ID_Paciente'

        if col_id_pago_mov in df_movimiento_procesado.columns:
            df_movimiento_procesado[col_total_pago_mov] = pd.to_numeric(
                df_movimiento_procesado[col_total_pago_mov], errors='coerce').fillna(0)
            df_movimiento_procesado[col_abono_libre_mov] = pd.to_numeric(
                df_movimiento_procesado[col_abono_libre_mov], errors='coerce').fillna(0)

            cols_tx_first = {
                'ID_Paciente': 'first',
                'Pago_fecha_recepcion': 'first',
                col_total_pago_mov: 'first',
                col_abono_libre_mov: 'first',
                'Medio de pago': 'first',  # Asume que existe y está unificada
                'Nombre Banco': 'first',  # Asume que existe y está unificada
                'Sucursal': 'first'  # Asume que existe y está unificada
            }
            # Filtrar el diccionario de agregación para incluir solo columnas existentes en el DataFrame
            agg_dict_tx = {k: v for k, v in cols_tx_first.items(
            ) if k in df_movimiento_procesado.columns}

            if agg_dict_tx:  # Solo agrupar si hay columnas para agregar
                hechos_pagos_transacciones = df_movimiento_procesado.groupby(
                    col_id_pago_mov, as_index=False).agg(agg_dict_tx)
                hechos_pagos_transacciones.rename(columns={col_abono_libre_mov: 'Monto_Abono_Libre_Original_En_Tx',
                                                           col_total_pago_mov: 'Total_Pago_Transaccion'}, inplace=True)
                if 'hechos_pacientes' in resultados_dfs and 'ID_Paciente' in hechos_pagos_transacciones.columns:
                    df_pac_temp = resultados_dfs['hechos_pacientes']
                    cols_pac_a_traer = [
                        'ID_Paciente', 'Paciente_Origen', 'Edad', 'Sexo', 'Ciudad']
                    cols_pac_a_traer_exist = [
                        c for c in cols_pac_a_traer if c in df_pac_temp.columns]
                    hechos_pagos_transacciones['ID_Paciente'] = hechos_pagos_transacciones['ID_Paciente'].astype(
                        str)
                    df_pac_temp['ID_Paciente'] = df_pac_temp['ID_Paciente'].astype(
                        str)
                    hechos_pagos_transacciones = pd.merge(hechos_pagos_transacciones, df_pac_temp[cols_pac_a_traer_exist].drop_duplicates(subset=['ID_Paciente']),
                                                          on='ID_Paciente', how='left', suffixes=('', '_pacienteinfo'))
                    # No es necesario dropear ID_Paciente_pacienteinfo porque el merge es on='ID_Paciente'

                resultados_dfs['hechos_pagos_transacciones'] = hechos_pagos_transacciones.copy(
                )
                print(
                    f"--- Log Sherlock (BG Task - generar_insights): 'hechos_pagos_transacciones' preparado con {len(hechos_pagos_transacciones)} filas.")
            else:
                all_advertencias.append(
                    f"Advertencia: No hay columnas suficientes para crear 'hechos_pagos_transacciones' desde Movimiento_df.")

            cols_para_aplicaciones = {
                col_id_pago_mov: 'ID_Pago',
                col_id_detalle_presup_mov: 'ID_Detalle_Presupuesto',
                'ID_Paciente': 'ID_Paciente',
                'ID_Tratamiento': 'ID_Tratamiento',
                col_pagado_detalle_mov: 'Monto_Aplicado_Al_Detalle',
                'Procedimiento_Pieza_Tratada': 'Procedimiento_Pieza_Tratada_Pago',
                'Monto procedimiento': 'Monto_Proc_Original_En_Pago_Linea'
            }
            cols_para_aplicaciones_existentes_keys = [
                k for k, v in cols_para_aplicaciones.items() if k in df_movimiento_procesado.columns]

            hechos_pagos_aplicaciones_detalle = df_movimiento_procesado[cols_para_aplicaciones_existentes_keys].copy(
            )
            # Renombrar usando solo las claves que existen
            rename_map_aplicaciones = {
                k: cols_para_aplicaciones[k] for k in cols_para_aplicaciones_existentes_keys}
            hechos_pagos_aplicaciones_detalle.rename(
                columns=rename_map_aplicaciones, inplace=True)

            if 'Monto_Aplicado_Al_Detalle' in hechos_pagos_aplicaciones_detalle.columns:
                hechos_pagos_aplicaciones_detalle['Monto_Aplicado_Al_Detalle'] = pd.to_numeric(
                    hechos_pagos_aplicaciones_detalle['Monto_Aplicado_Al_Detalle'], errors='coerce').fillna(0)
            if 'Monto_Proc_Original_En_Pago_Linea' in hechos_pagos_aplicaciones_detalle.columns:
                hechos_pagos_aplicaciones_detalle['Monto_Proc_Original_En_Pago_Linea'] = pd.to_numeric(
                    hechos_pagos_aplicaciones_detalle['Monto_Proc_Original_En_Pago_Linea'], errors='coerce').fillna(0)

            if 'hechos_pacientes' in resultados_dfs and 'ID_Paciente' in hechos_pagos_aplicaciones_detalle.columns:
                df_pac_temp = resultados_dfs['hechos_pacientes']
                cols_pac_a_traer = ['ID_Paciente',
                                    'Paciente_Origen', 'Edad', 'Sexo', 'Ciudad']
                cols_pac_a_traer_exist = [
                    c for c in cols_pac_a_traer if c in df_pac_temp.columns]
                hechos_pagos_aplicaciones_detalle['ID_Paciente'] = hechos_pagos_aplicaciones_detalle['ID_Paciente'].astype(
                    str)
                df_pac_temp['ID_Paciente'] = df_pac_temp['ID_Paciente'].astype(
                    str)
                hechos_pagos_aplicaciones_detalle = pd.merge(hechos_pagos_aplicaciones_detalle, df_pac_temp[cols_pac_a_traer_exist].drop_duplicates(subset=['ID_Paciente']),
                                                             on='ID_Paciente', how='left', suffixes=('', '_pacienteinfo'))

            resultados_dfs['hechos_pagos_aplicaciones_detalle'] = hechos_pagos_aplicaciones_detalle.copy()
            print(
                f"--- Log Sherlock (BG Task - generar_insights): 'hechos_pagos_aplicaciones_detalle' preparado con {len(hechos_pagos_aplicaciones_detalle)} filas.")
        else:
            all_advertencias.append(
                f"Advertencia: Columna '{col_id_pago_mov}' no encontrada en Movimiento_df.")
    else:
        all_advertencias.append(
            "INFO: DataFrame 'Movimiento_df' no disponible.")

    # --- 8. Procesar Gastos (`Tabla Gastos Aliadas Mexico_df`) ---
    df_gastos_base = get_df_by_type(
        processed_dfs, "Tabla Gastos Aliadas Mexico_df", all_advertencias)
    if df_gastos_base is not None:
        df_gastos_enriquecido = df_gastos_base.copy()
        resultados_dfs['hechos_gastos'] = df_gastos_enriquecido.copy()
        print(
            f"--- Log Sherlock (BG Task - generar_insights): 'hechos_gastos' preparado con {len(df_gastos_enriquecido)} filas.")
    else:
        all_advertencias.append(
            "INFO: DataFrame 'Tabla Gastos Aliadas Mexico_df' no disponible.")

    # --- 9. Generar Perfiles de Pacientes (usando hechos_pacientes) ---
    if 'hechos_pacientes' in resultados_dfs:
        df_pac_para_perfil = resultados_dfs['hechos_pacientes']
        print(
            f"--- Log Sherlock (BG Task - generar_insights): Columnas de 'hechos_pacientes' para perfiles: {df_pac_para_perfil.columns.tolist()}")

        col_id_pac_perfil = 'ID_Paciente'
        col_edad_calc_perfil = 'Edad'
        col_sexo_unif_perfil = 'Sexo'
        col_origen_pac_enr_perfil = col_origen_pac_catalogo

        cols_necesarias_para_perfil_final = [
            col_id_pac_perfil, col_edad_calc_perfil, col_sexo_unif_perfil, col_origen_pac_enr_perfil]
        if not all(c in df_pac_para_perfil.columns for c in cols_necesarias_para_perfil_final):
            missing_cols_perfil = [
                c for c in cols_necesarias_para_perfil_final if c not in df_pac_para_perfil.columns]
            all_advertencias.append(
                f"ADVERTENCIA: Faltan columnas en 'hechos_pacientes' para perfil ({missing_cols_perfil}).")
        elif ('Edad' in df_pac_para_perfil and df_pac_para_perfil[col_edad_calc_perfil].isnull().all()) and \
             (col_origen_pac_enr_perfil in df_pac_para_perfil and df_pac_para_perfil[col_origen_pac_enr_perfil].isnull().all()):
            all_advertencias.append(
                f"Advertencia: Columnas '{col_edad_calc_perfil}' y/o '{col_origen_pac_enr_perfil}' todas nulas en 'hechos_pacientes'. Perfil podría ser vacío.")
        else:
            try:
                # Columnas para agrupar, asegurando que existan
                groupby_cols_perfil = []
                if col_edad_calc_perfil in df_pac_para_perfil.columns:
                    groupby_cols_perfil.append(col_edad_calc_perfil)
                if col_sexo_unif_perfil in df_pac_para_perfil.columns:
                    groupby_cols_perfil.append(col_sexo_unif_perfil)
                if col_origen_pac_enr_perfil in df_pac_para_perfil.columns:
                    groupby_cols_perfil.append(col_origen_pac_enr_perfil)

                # Necesitamos al menos dos dimensiones para agrupar y el ID para contar
                if len(groupby_cols_perfil) >= 2 and col_id_pac_perfil in df_pac_para_perfil.columns:
                    print(
                        f"--- Log Sherlock (BG Task - generar_insights): Intentando perfil con columnas: {groupby_cols_perfil}...")
                    df_perfil_agrupado = df_pac_para_perfil.groupby(
                        groupby_cols_perfil,
                        dropna=False
                    )[col_id_pac_perfil].nunique().reset_index(name='Numero_Pacientes')

                    if not df_perfil_agrupado.empty:
                        resultados_dfs['perfil_edad_sexo_origen_paciente'] = df_perfil_agrupado
                        print(
                            f"--- Log Sherlock (BG Task - generar_insights): 'perfil_edad_sexo_origen_paciente' generado con {len(df_perfil_agrupado)} filas.")
                    else:
                        all_advertencias.append(
                            "INFO: 'perfil_edad_sexo_origen_paciente' resultó vacío.")
                else:
                    all_advertencias.append(
                        f"Advertencia: No hay suficientes columnas para generar 'perfil_edad_sexo_origen_paciente'. Columnas para groupby: {groupby_cols_perfil}, ID Paciente: {col_id_pac_perfil in df_pac_para_perfil.columns}")

            except Exception as e_perfil_final:
                all_advertencias.append(
                    f"Error generando perfil edad-sexo-origen: {e_perfil_final}")
                print(
                    f"--- Log Sherlock (BG Task - generar_insights): ERROR perfil edad-sexo-origen: {e_perfil_final}")
                import traceback
                traceback.print_exc()
    else:
        all_advertencias.append(
            "Advertencia: 'hechos_pacientes' no disponible, no se puede generar perfil.")

    print(
        f"--- Log Sherlock (BG Task - generar_insights): Fin. DataFrames finales listos ({len(resultados_dfs)}): {list(resultados_dfs.keys())}")
    return resultados_dfs

# Fin de procesador_datos.py

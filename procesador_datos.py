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
    print(
        f"--- Log Sherlock (BG Task - generar_insights): Inicio. DF limpios disponibles: {list(processed_dfs.keys())}")

    # --- PASO 0: DIAGNÓSTICO DE COLUMNAS ---
    # Imprimiremos las columnas de los DFs clave tal como llegan de la función de carga.
    print("\n--- INICIO DIAGNÓSTICO DE COLUMNAS DE DATAFRAMES LIMPIOS ---")
    dataframes_a_inspeccionar = [
        "Pacientes_Nuevos_df", "Tipos de pacientes_df", "Lada_df",
        "Citas_Pacientes_df", "Citas_Motivo_df", "Presupuesto por Accion_df",
        "Acciones_df", "Movimiento_df", "Tabla_Procedimientos_df", "Sucursal_df"
    ]
    for df_key in dataframes_a_inspeccionar:
        df_a_inspeccionar = get_df_by_type(
            processed_dfs, df_key, all_advertencias)
        if df_a_inspeccionar is not None:
            print(
                f"--- Columnas para '{df_key}': {df_a_inspeccionar.columns.tolist()}")
    print("--- FIN DIAGNÓSTICO DE COLUMNAS ---\n")
    # --------------------------------------------

    try:
        # --- 1. Preparar Tablas de Dimensiones ---
        dimension_mapping = {
            "Tipos de pacientes_df": "dimension_tipos_pacientes", "Tabla_Procedimientos_df": "dimension_procedimientos",
            "Sucursal_df": "dimension_sucursales", "Lada_df": "dimension_lada",
            "Tratamiento Generado Mex_df": "dimension_tratamientos_generados"
        }
        for df_key, table_name in dimension_mapping.items():
            df_dim = get_df_by_type(processed_dfs, df_key, all_advertencias)
            if df_dim is not None:
                resultados_dfs[table_name] = df_dim.copy()

        # --- 2. Procesar Pacientes ---
        df_pacientes_base = get_df_by_type(
            processed_dfs, "Pacientes_Nuevos_df", all_advertencias)
        if df_pacientes_base is None:
            raise ValueError("'Pacientes_Nuevos_df' no encontrado, abortando.")

        df_pacientes_enriquecido = df_pacientes_base.copy()

        # Calcular Edad
        col_fecha_nac = 'Fecha de nacimiento'
        if col_fecha_nac in df_pacientes_enriquecido.columns:
            # ... (Lógica de cálculo de Edad que ya teníamos y que es robusta) ...
            df_pacientes_enriquecido[col_fecha_nac] = pd.to_datetime(
                df_pacientes_enriquecido[col_fecha_nac], errors='coerce')
            if not df_pacientes_enriquecido[col_fecha_nac].isnull().all():
                current_time_mex = pd.Timestamp.now(tz='America/Mexico_City')
                if df_pacientes_enriquecido[col_fecha_nac].dt.tz is None:
                    df_pacientes_enriquecido[col_fecha_nac] = df_pacientes_enriquecido[col_fecha_nac].dt.tz_localize(
                        'America/Mexico_City', ambiguous='infer', nonexistent='NaT')
                else:
                    df_pacientes_enriquecido[col_fecha_nac] = df_pacientes_enriquecido[col_fecha_nac].dt.tz_convert(
                        'America/Mexico_City')
                time_difference = current_time_mex - \
                    df_pacientes_enriquecido[col_fecha_nac]
                if not time_difference.isnull().all():
                    df_pacientes_enriquecido['Edad'] = (
                        time_difference / pd.Timedelta(days=1) / 365.25).astype('Int64')
                    print(f"--- Log Sherlock (BG Task): Columna 'Edad' calculada.")
        else:
            all_advertencias.append(
                f"Advertencia de Diagnóstico: Columna '{col_fecha_nac}' NO encontrada en Pacientes_Nuevos_df.")
            df_pacientes_enriquecido['Edad'] = pd.NA

        # Enriquecer con Origen
        # ... (La lógica de enriquecimiento que ya teníamos, la cual depende de los nombres correctos) ...
        col_tipo_dentalink = 'Tipo Dentalink'
        col_origen = 'Paciente_Origen'
        if 'dimension_tipos_pacientes' in resultados_dfs and col_tipo_dentalink in df_pacientes_enriquecido.columns:
            df_dim_tipos_pac = resultados_dfs['dimension_tipos_pacientes']
            if col_tipo_dentalink in df_dim_tipos_pac.columns and col_origen in df_dim_tipos_pac.columns:
                df_origen_merge = df_dim_tipos_pac[[
                    col_tipo_dentalink, col_origen]].drop_duplicates(subset=[col_tipo_dentalink])
                df_pacientes_enriquecido = pd.merge(
                    df_pacientes_enriquecido, df_origen_merge, on=col_tipo_dentalink, how='left')

        resultados_dfs['hechos_pacientes'] = df_pacientes_enriquecido.copy()

  # Dentro de la función generar_insights_pacientes, REEMPLAZA la sección de citas con esto:

    # --- 3. Procesar y Enriquecer DataFrame de Citas ---
    print(f"--- Log Sherlock (BG Task - generar_insights): Iniciando procesamiento de Citas...")
    df_citas_pac_base = get_df_by_type(
        processed_dfs, "Citas_Pacientes_df", all_advertencias)
    df_citas_mot_base = get_df_by_type(
        processed_dfs, "Citas_Motivo_df", all_advertencias)

    # INICIALIZAMOS LA VARIABLE A NONE PARA EVITAR EL ERROR "is not defined"
    hechos_citas_df = None

    if df_citas_pac_base is not None and df_citas_mot_base is not None:
        try:
            # Nombres de columna unificados de tu índice (verifica que sean exactos)
            col_id_cita = 'ID_Cita'
            col_id_paciente_citas = 'ID_Paciente'
            col_fecha_cita = 'Fecha Cita'
            col_cita_asistida_unif = 'Cita_asistida'
            col_cita_duplicada_unif = 'Cita duplicada'
            col_fecha_creacion_cita_unif = 'Fecha de creación cita'
            col_sucursal_cita_unif = 'Sucursal'
            col_id_trat_cita_unif = 'ID_Tratamiento'

            # a. Pre-procesamiento y validación de columnas
            cols_nec_citaspac = [col_id_cita, col_id_paciente_citas,
                                 col_fecha_cita, col_cita_asistida_unif, col_cita_duplicada_unif]
            cols_nec_citasmot = [col_id_cita, col_fecha_creacion_cita_unif,
                                 col_sucursal_cita_unif, col_id_trat_cita_unif]

            if not all(c in df_citas_pac_base.columns for c in cols_nec_citaspac) or \
               not all(c in df_citas_mot_base.columns for c in cols_nec_citasmot):
                all_advertencias.append(
                    "ADVERTENCIA: Faltan columnas en DFs de Citas. Abortando procesamiento de citas.")
                raise ValueError(
                    "Faltan columnas base para procesar citas. Revisa los logs de diagnóstico de columnas.")

            # b. Limpieza y conversión de tipos
            df_citas_pac_base[col_cita_asistida_unif] = pd.to_numeric(
                df_citas_pac_base[col_cita_asistida_unif], errors='coerce').fillna(0).astype(int)
            df_citas_pac_base[col_cita_duplicada_unif] = pd.to_numeric(
                df_citas_pac_base[col_cita_duplicada_unif], errors='coerce').fillna(0).astype(int)
            df_citas_pac_filtrado = df_citas_pac_base[df_citas_pac_base[col_cita_duplicada_unif] == 0].copy(
            )
            print(
                f"--- Log Sherlock (BG Task - generar_insights): Citas_Pacientes_df filtrado (sin duplicados): {len(df_citas_pac_filtrado)} filas.")

            # c. Merge de DFs de Citas
            df_citas_pac_filtrado[col_id_cita] = df_citas_pac_filtrado[col_id_cita].astype(
                str)
            df_citas_mot_base[col_id_cita] = df_citas_mot_base[col_id_cita].astype(
                str)

            columnas_de_citas_motivo_a_unir_existentes = [
                c for c in cols_nec_citasmot if c in df_citas_mot_base.columns]

            hechos_citas_df = pd.merge(
                df_citas_pac_filtrado,
                df_citas_mot_base[columnas_de_citas_motivo_a_unir_existentes].drop_duplicates(
                    subset=[col_id_cita]),
                on=col_id_cita, how='left'
            )
            print(
                f"--- Log Sherlock (BG Task - generar_insights): Merge Citas_Pacientes y Citas_Motivo. Filas: {len(hechos_citas_df)}")

            # d. Calcular Fecha_Primera_Cita_Atendida_Real
            hechos_citas_df[col_fecha_cita] = pd.to_datetime(
                hechos_citas_df[col_fecha_cita], errors='coerce')
            df_atendidas_calc = hechos_citas_df[(
                hechos_citas_df[col_cita_asistida_unif] == 1) & pd.notna(hechos_citas_df[col_fecha_cita])]
            if not df_atendidas_calc.empty:
                primera_cita_atendida = df_atendidas_calc.groupby(
                    col_id_paciente_citas)[col_fecha_cita].min().reset_index()
                primera_cita_atendida.rename(
                    columns={col_fecha_cita: 'Fecha_Primera_Cita_Atendida_Real'}, inplace=True)
                hechos_citas_df[col_id_paciente_citas] = hechos_citas_df[col_id_paciente_citas].astype(
                    str)
                primera_cita_atendida[col_id_paciente_citas] = primera_cita_atendida[col_id_paciente_citas].astype(
                    str)
                hechos_citas_df = pd.merge(
                    hechos_citas_df, primera_cita_atendida, on=col_id_paciente_citas, how='left')
            else:
                hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'] = pd.NaT

            # e. Etiquetar cada cita
            # ... (Toda la lógica de etiquetado con .loc que ya teníamos) ...
            today = pd.Timestamp('today').normalize()
            hechos_citas_df['Etiqueta_Cita_Paciente'] = 'Indeterminada'
            cond_nunca_atendido_antes = hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].isnull(
            )
            cond_esta_es_primera_atendida = (hechos_citas_df[col_fecha_cita].notnull() & hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].notnull(
            ) & (hechos_citas_df[col_fecha_cita] == hechos_citas_df['Fecha_Primera_Cita_Atendida_Real']))
            cond_paciente_es_nuevo_para_esta_cita = cond_nunca_atendido_antes | cond_esta_es_primera_atendida
            hechos_citas_df.loc[cond_paciente_es_nuevo_para_esta_cita & (
                hechos_citas_df[col_fecha_cita] >= today), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo en Agenda"
            hechos_citas_df.loc[cond_paciente_es_nuevo_para_esta_cita & (hechos_citas_df[col_fecha_cita] < today) & (
                hechos_citas_df[col_cita_asistida_unif] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo Atendido"
            hechos_citas_df.loc[cond_paciente_es_nuevo_para_esta_cita & (hechos_citas_df[col_fecha_cita] < today) & (
                hechos_citas_df[col_cita_asistida_unif] == 0), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo No Atendido"
            cond_paciente_es_recurrente = (~cond_paciente_es_nuevo_para_esta_cita) & (hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].notnull()) & (
                hechos_citas_df[col_fecha_cita].notnull() & (hechos_citas_df[col_fecha_cita] > hechos_citas_df['Fecha_Primera_Cita_Atendida_Real']))
            cond_mismo_mes_debut = hechos_citas_df[col_fecha_cita].notnull() & hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].notnull(
            ) & (hechos_citas_df[col_fecha_cita].dt.to_period('M') == hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].dt.to_period('M'))
            hechos_citas_df.loc[cond_paciente_es_recurrente & cond_mismo_mes_debut & (
                hechos_citas_df[col_cita_asistida_unif] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Atendido Mismo Mes que Debutó"
            hechos_citas_df.loc[cond_paciente_es_recurrente & (~cond_mismo_mes_debut) & (
                hechos_citas_df[col_fecha_cita] >= today), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente en Agenda"
            hechos_citas_df.loc[cond_paciente_es_recurrente & (~cond_mismo_mes_debut) & (hechos_citas_df[col_fecha_cita] < today) & (
                hechos_citas_df[col_cita_asistida_unif] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente Atendido"
            hechos_citas_df.loc[cond_paciente_es_recurrente & (~cond_mismo_mes_debut) & (hechos_citas_df[col_fecha_cita] < today) & (
                hechos_citas_df[col_cita_asistida_unif] == 0), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente No Atendido"

            print(
                f"--- Log Sherlock (BG Task - generar_insights): Etiquetas de citas calculadas.")

        except KeyError as e_key_citas:
            all_advertencias.append(
                f"ERROR DE CLAVE procesando citas: No se encontró la columna {e_key_citas}. Se aborta el procesamiento de citas.")
            print(
                f"--- Log Sherlock (BG Task - generar_insights): ERROR DE CLAVE procesando citas: No se encontró la columna {e_key_citas}.")
            # `hechos_citas_df` se mantendrá como None
        except Exception as e_citas:
            all_advertencias.append(f"Error procesando citas: {e_citas}")
            print(
                f"--- Log Sherlock (BG Task - generar_insights): ERROR general procesando citas: {e_citas}")
            # `hechos_citas_df` podría tener un estado intermedio o ser None, dependiendo de dónde ocurrió el error.
            # Por seguridad, lo reseteamos a None.
            hechos_citas_df = None
            import traceback
            traceback.print_exc()

    else:
        if df_citas_pac_base is None:
            all_advertencias.append(
                "ADVERTENCIA: DataFrame 'Citas_Pacientes_df' no disponible.")
        if df_citas_mot_base is None:
            all_advertencias.append(
                "ADVERTENCIA: DataFrame 'Citas_Motivo_df' no disponible.")

    # Al final de la sección, añadir el DF de citas a los resultados SOLO SI SE CREÓ
    if hechos_citas_df is not None:
        if 'Fecha_Primera_Cita_Atendida_Real' not in hechos_citas_df.columns:
            # Asegurar que la columna exista
            hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'] = pd.NaT
        resultados_dfs['hechos_citas'] = hechos_citas_df.copy()
        print(
            f"--- Log Sherlock (BG Task - generar_insights): 'hechos_citas' preparado con {len(hechos_citas_df)} filas.")

        # --- 4. Procesar Presupuestos, Acciones, Pagos ---
        # Presupuestos
        df_presupuestos = get_df_by_type(
            processed_dfs, "Presupuesto por Accion_df", all_advertencias)
        if df_presupuestos is not None:
            # Aquí iría el enriquecimiento con detalles de procedimiento y paciente,
            # una vez que confirmemos los nombres de las columnas.
            # Por ejemplo, para el descuento:
            col_precio_orig = 'Procedimiento_precio_original'
            col_precio_pac = 'Procedimiento_precio_paciente'
            if col_precio_orig in df_presupuestos.columns and col_precio_pac in df_presupuestos.columns:
                df_presupuestos[col_precio_orig] = pd.to_numeric(
                    df_presupuestos[col_precio_orig], errors='coerce')
                df_presupuestos[col_precio_pac] = pd.to_numeric(
                    df_presupuestos[col_precio_pac], errors='coerce')
                df_presupuestos['Descuento_Presupuestado_Detalle'] = df_presupuestos[col_precio_orig] - \
                    df_presupuestos[col_precio_pac]
                print(
                    "--- Log Sherlock (BG Task): Columna 'Descuento_Presupuestado_Detalle' calculada.")
            else:
                all_advertencias.append(
                    f"Advertencia de Diagnóstico: Columnas para descuento ('{col_precio_orig}', '{col_precio_pac}') NO encontradas en Presupuesto por Accion_df.")

            resultados_dfs['hechos_presupuesto_detalle'] = df_presupuestos.copy()

        # ... (Lógica similar para Acciones y Pagos) ...

        # --- 5. Generar Perfiles ---
        if 'hechos_pacientes' in resultados_dfs:
            df_pac_para_perfil = resultados_dfs['hechos_pacientes']
            col_id_pac = 'ID_Paciente'
            col_edad = 'Edad'
            col_sexo = 'Sexo'
            col_origen_pac = 'Paciente_Origen'

            groupby_cols = []
            if col_edad in df_pac_para_perfil.columns:
                groupby_cols.append(col_edad)
            if col_sexo in df_pac_para_perfil.columns:
                groupby_cols.append(col_sexo)
            if col_origen_pac in df_pac_para_perfil.columns:
                groupby_cols.append(col_origen_pac)

            if col_id_pac in df_pac_para_perfil.columns and len(groupby_cols) > 1:
                perfil = df_pac_para_perfil.groupby(groupby_cols, dropna=False)[
                    col_id_pac].nunique().reset_index(name='Numero_Pacientes')
                if not perfil.empty:
                    resultados_dfs['perfil_edad_sexo_origen_paciente'] = perfil
            else:
                all_advertencias.append(
                    "Advertencia de Diagnóstico: Faltan columnas para generar el perfil de paciente.")

    except KeyError as e:
        print(
            f"--- Log Sherlock (BG Task - generar_insights): ¡¡¡ERROR DE CLAVE (KeyError)!!! No se encontró la columna: {e}")
        all_advertencias.append(
            f"FATAL: No se encontró la columna {e}. El proceso de enriquecimiento se detuvo.")
        # Imprimir las columnas del DataFrame que podría estar causando el problema si podemos identificarlo
    except Exception as e_general:
        print(
            f"--- Log Sherlock (BG Task - generar_insights): ¡¡¡ERROR GENERAL!!! Ocurrió un error inesperado: {e_general}")
        import traceback
        traceback.print_exc()
        all_advertencias.append(f"FATAL: Error inesperado: {e_general}")

    print(
        f"--- Log Sherlock (BG Task - generar_insights): Fin. DataFrames finales listos ({len(resultados_dfs)}): {list(resultados_dfs.keys())}")
    return resultados_dfs

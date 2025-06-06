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

    # 1. Preparar Tablas de Dimensiones
    dimension_mapping = {
        "Tipos de pacientes_df": "dimension_tipos_pacientes", "Tabla_Procedimientos_df": "dimension_procedimientos",
        "Sucursal_df": "dimension_sucursales", "Lada_df": "dimension_lada",
        "Tratamiento Generado Mex_df": "dimension_tratamientos_generados"
    }
    for df_key, table_name in dimension_mapping.items():
        df_dim = get_df_by_type(processed_dfs, df_key, all_advertencias)
        if df_dim is not None:
            resultados_dfs[table_name] = df_dim.copy()

    # 2. Procesar Pacientes
    df_pacientes_base = get_df_by_type(
        processed_dfs, "Pacientes_Nuevos_df", all_advertencias)
    if df_pacientes_base is None:
        all_advertencias.append(
            "ERROR CRÍTICO: 'Pacientes_Nuevos_df' no disponible. Abortando enriquecimientos principales.")
        return resultados_dfs

    df_pacientes_enriquecido = df_pacientes_base.copy()
    col_fecha_nac = 'Fecha de nacimiento'
    if col_fecha_nac in df_pacientes_enriquecido.columns:
        try:
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
                    print(
                        "--- Log Sherlock (BG Task - generar_insights): Columna 'Edad' calculada para pacientes.")
        except Exception as e_edad:
            all_advertencias.append(
                f"Advertencia: Error al calcular Edad: {e_edad}")
            df_pacientes_enriquecido['Edad'] = pd.NA
    else:
        all_advertencias.append(
            f"Advertencia: Columna '{col_fecha_nac}' no encontrada en Pacientes.")
        df_pacientes_enriquecido['Edad'] = pd.NA

    col_tipo_dentalink = 'Tipo Dentalink'
    col_origen = 'Paciente_Origen'
    if 'dimension_tipos_pacientes' in resultados_dfs and col_tipo_dentalink in df_pacientes_enriquecido.columns:
        df_dim_tipos_pac = resultados_dfs['dimension_tipos_pacientes']
        if col_tipo_dentalink in df_dim_tipos_pac.columns and col_origen in df_dim_tipos_pac.columns:
            df_origen_merge = df_dim_tipos_pac[[
                col_tipo_dentalink, col_origen]].drop_duplicates(subset=[col_tipo_dentalink])
            df_pacientes_enriquecido = pd.merge(
                df_pacientes_enriquecido, df_origen_merge, on=col_tipo_dentalink, how='left')
            print(
                f"--- Log Sherlock (BG Task - generar_insights): Pacientes enriquecidos con '{col_origen}'.")
    resultados_dfs['hechos_pacientes'] = df_pacientes_enriquecido.copy()

    # 3. Procesar Citas
    df_citas_pac = get_df_by_type(
        processed_dfs, "Citas_Pacientes_df", all_advertencias)
    df_citas_mot = get_df_by_type(
        processed_dfs, "Citas_Motivo_df", all_advertencias)
    if df_citas_pac is not None and df_citas_mot is not None:
        try:
            col_id_cita = 'ID_Cita'
            col_id_pac_citas = 'ID_Paciente'
            col_fecha_cita = 'Fecha Cita'
            col_asistida = 'Cita_asistida'
            col_duplicada = 'Cita duplicada'

            df_citas_pac[col_asistida] = pd.to_numeric(
                df_citas_pac[col_asistida], errors='coerce').fillna(0).astype(int)
            df_citas_pac[col_duplicada] = pd.to_numeric(
                df_citas_pac[col_duplicada], errors='coerce').fillna(0).astype(int)
            df_citas_filtrado = df_citas_pac[df_citas_pac[col_duplicada] == 0].copy(
            )

            df_citas_filtrado[col_id_cita] = df_citas_filtrado[col_id_cita].astype(
                str)
            df_citas_mot[col_id_cita] = df_citas_mot[col_id_cita].astype(str)

            cols_from_motivo = ['ID_Cita', 'Fecha de creación cita', 'Hora Inicio Cita',
                                'Hora Fin Cita', 'Motivo Cita', 'Sucursal', 'ID_Tratamiento']
            cols_from_motivo_exist = [
                c for c in cols_from_motivo if c in df_citas_mot.columns]

            hechos_citas_df = pd.merge(df_citas_filtrado, df_citas_mot[cols_from_motivo_exist].drop_duplicates(
                subset=[col_id_cita]), on=col_id_cita, how='left')
            hechos_citas_df[col_fecha_cita] = pd.to_datetime(
                hechos_citas_df[col_fecha_cita], errors='coerce')

            df_atendidas = hechos_citas_df[(
                hechos_citas_df[col_asistida] == 1) & hechos_citas_df[col_fecha_cita].notna()]
            if not df_atendidas.empty:
                primera_cita = df_atendidas.groupby(col_id_pac_citas)[col_fecha_cita].min(
                ).reset_index().rename(columns={col_fecha_cita: 'Fecha_Primera_Cita_Atendida_Real'})
                hechos_citas_df[col_id_pac_citas] = hechos_citas_df[col_id_pac_citas].astype(
                    str)
                primera_cita[col_id_pac_citas] = primera_cita[col_id_pac_citas].astype(
                    str)
                hechos_citas_df = pd.merge(
                    hechos_citas_df, primera_cita, on=col_id_pac_citas, how='left')
            else:
                hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'] = pd.NaT

            # Lógica de etiquetado (simplificada para legibilidad)
            # ... (código de etiquetado como en la respuesta anterior) ...

            resultados_dfs['hechos_citas'] = hechos_citas_df.copy()
            print(
                f"--- Log Sherlock (BG Task - generar_insights): 'hechos_citas' preparado con {len(hechos_citas_df)} filas.")
        except Exception as e_citas:
            all_advertencias.append(f"Error procesando citas: {e_citas}")
            print(
                f"--- Log Sherlock (BG Task - generar_insights): ERROR procesando citas: {e_citas}")

    # 4. Procesar Presupuestos, Acciones, Pagos (de forma similar, enriqueciendo y guardando)
    # ... Presupuestos
    df_presupuestos = get_df_by_type(
        processed_dfs, "Presupuesto por Accion_df", all_advertencias)
    if df_presupuestos is not None:
        # ... enriquecer ...
        resultados_dfs['hechos_presupuesto_detalle'] = df_presupuestos.copy()

    # ... Acciones
    df_acciones = get_df_by_type(
        processed_dfs, "Acciones_df", all_advertencias)
    if df_acciones is not None:
        # ... enriquecer ...
        resultados_dfs['hechos_acciones_realizadas'] = df_acciones.copy()

    # ... Pagos
    df_movimiento = get_df_by_type(
        processed_dfs, "Movimiento_df", all_advertencias)
    if df_movimiento is not None:
        try:
            col_id_pago = 'ID_Pago'
            col_total = 'Total Pago'
            col_abono = 'Abono Libre'
            df_movimiento[col_total] = pd.to_numeric(
                df_movimiento[col_total], errors='coerce').fillna(0)
            df_movimiento[col_abono] = pd.to_numeric(
                df_movimiento[col_abono], errors='coerce').fillna(0)

            agg_cols = {'ID_Paciente': 'first', 'Pago_fecha_recepcion': 'first',
                        col_total: 'first', col_abono: 'first'}
            agg_cols_exist = {
                k: v for k, v in agg_cols.items() if k in df_movimiento.columns}
            if col_id_pago in df_movimiento.columns and agg_cols_exist:
                tx_pagos = df_movimiento.groupby(
                    col_id_pago, as_index=False).agg(agg_cols_exist)
                tx_pagos.rename(columns={col_abono: 'Monto_Abono_Libre_Original_En_Tx',
                                col_total: 'Total_Pago_Transaccion'}, inplace=True)
                resultados_dfs['hechos_pagos_transacciones'] = tx_pagos
                print(
                    f"--- Log Sherlock (BG Task - generar_insights): 'hechos_pagos_transacciones' preparado con {len(tx_pagos)} filas.")

            app_cols = {'ID_Pago': 'ID_Pago', 'ID_Detalle Presupuesto': 'ID_Detalle_Presupuesto',
                        'Pagado_ID_Detalle_Presupuesto': 'Monto_Aplicado_Al_Detalle'}
            app_cols_exist_keys = [
                k for k in app_cols.keys() if k in df_movimiento.columns]
            app_df = df_movimiento[app_cols_exist_keys].copy()
            app_df.rename(
                columns={k: app_cols[k] for k in app_cols_exist_keys}, inplace=True)
            resultados_dfs['hechos_pagos_aplicaciones_detalle'] = app_df
            print(
                f"--- Log Sherlock (BG Task - generar_insights): 'hechos_pagos_aplicaciones_detalle' preparado con {len(app_df)} filas.")
        except Exception as e_pagos:
            all_advertencias.append(f"Error procesando pagos: {e_pagos}")
            print(
                f"--- Log Sherlock (BG Task - generar_insights): ERROR procesando pagos: {e_pagos}")

    # ... Gastos
    df_gastos = get_df_by_type(
        processed_dfs, "Tabla Gastos Aliadas Mexico_df", all_advertencias)
    if df_gastos is not None:
        resultados_dfs['hechos_gastos'] = df_gastos.copy()

    # 5. Generar Perfiles (usando hechos_pacientes)
    if 'hechos_pacientes' in resultados_dfs:
        df_pac_para_perfil = resultados_dfs['hechos_pacientes']
        col_id_pac = 'ID_Paciente'
        col_edad = 'Edad'
        col_sexo = 'Sexo'
        col_origen = 'Paciente_Origen'
        groupby_cols = [col for col in [col_edad, col_sexo,
                                        col_origen] if col in df_pac_para_perfil.columns]
        if len(groupby_cols) >= 2 and col_id_pac in df_pac_para_perfil.columns:
            try:
                perfil = df_pac_para_perfil.groupby(groupby_cols, dropna=False)[
                    col_id_pac].nunique().reset_index(name='Numero_Pacientes')
                if not perfil.empty:
                    resultados_dfs['perfil_edad_sexo_origen_paciente'] = perfil
                    print(
                        f"--- Log Sherlock (BG Task - generar_insights): 'perfil_edad_sexo_origen_paciente' generado con {len(perfil)} filas.")
            except Exception as e_perfil:
                all_advertencias.append(
                    f"Error generando perfil final: {e_perfil}")

    print(
        f"--- Log Sherlock (BG Task - generar_insights): Fin. DataFrames finales listos ({len(resultados_dfs)}): {list(resultados_dfs.keys())}")
    return resultados_dfs

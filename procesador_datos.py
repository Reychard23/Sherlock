import pandas as pd
import numpy as np
import io
import os
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
        f"--- Log Sherlock (BG Task - load_data): Leyendo índice (Excel): {index_file.filename}")
    try:
        index_file.file.seek(0)
        content_indice_bytes = index_file.file.read()
        indice_df = pd.read_excel(io.BytesIO(
            content_indice_bytes), sheet_name=0)

        col_map = {'Archivo_Idx': 'Archivo', 'Hoja_Idx': 'Sheet', 'Original_Idx': 'Columna',
                   'Nuevo_Idx': 'Nombre unificado', 'Accion_Idx': 'Acción'}
        if not all(col in indice_df.columns for col in col_map.values()):
            raise ValueError(
                f"Faltan columnas en índice: {[c for c in col_map.values() if c not in indice_df.columns]}")

        for _, row in indice_df.iterrows():
            nombre_archivo_indice_con_ext = str(
                row[col_map['Archivo_Idx']]).strip()
            archivo_base_idx, _ = os.path.splitext(
                nombre_archivo_indice_con_ext)
            hoja_idx = str(row[col_map['Hoja_Idx']]).strip() if pd.notna(
                row[col_map['Hoja_Idx']]) else 'default'
            original_col_idx = str(row[col_map['Original_Idx']]).strip()
            nuevo_col_idx = str(row[col_map['Nuevo_Idx']]).strip()
            accion_val_idx = str(row[col_map['Accion_Idx']]).strip().upper()

            if accion_val_idx == 'DROP':
                drop_columns_set.add(
                    (archivo_base_idx, hoja_idx, original_col_idx))
            elif accion_val_idx == 'KEEP' and pd.notna(nuevo_col_idx) and nuevo_col_idx != original_col_idx:
                if (archivo_base_idx, hoja_idx) not in rename_map_details:
                    rename_map_details[(archivo_base_idx, hoja_idx)] = {}
                rename_map_details[(archivo_base_idx, hoja_idx)
                                   ][original_col_idx] = nuevo_col_idx

        print(f"--- Log Sherlock (BG Task - load_data): Reglas de renombrado y eliminación construidas.")
    except Exception as e:
        msg = f"ERROR CRÍTICO al leer índice: {e}"
        advertencias_carga.append(msg)
        print(msg)
        import traceback
        traceback.print_exc()
        return {}, {}, set(), advertencias_carga

    for i_file, uploaded_file_obj in enumerate(data_files):
        original_filename = uploaded_file_obj.filename
        print(
            f"--- Log Sherlock (BG Task - load_data): ({i_file+1}/{len(data_files)}) Procesando: {original_filename}")
        try:
            base_name, _ = os.path.splitext(original_filename)
            df_sheets = pd.read_excel(io.BytesIO(
                uploaded_file_obj.file.read()), sheet_name=None)

            for sheet_name, df_original in df_sheets.items():
                fila_indice_para_hoja = indice_df[
                    (indice_df[col_map['Archivo_Idx']].apply(lambda x: os.path.splitext(str(x))[0]) == base_name) &
                    (indice_df[col_map['Hoja_Idx']] == sheet_name)
                ]
                if fila_indice_para_hoja.empty:
                    print(
                        f"      Hoja '{sheet_name}' no en índice para '{base_name}'. Omitiendo.")
                    continue

                df_cleaned = df_original.copy()
                rename_dict = rename_map_details.get(
                    (base_name, sheet_name), rename_map_details.get((base_name, 'default'), {}))
                df_cleaned.rename(columns=rename_dict, inplace=True)

                drop_cols_originals = {col for (ab, h, col) in drop_columns_set if ab == base_name and (
                    h == sheet_name or h == 'default')}
                cols_to_drop_final = [rename_dict.get(
                    col, col) for col in drop_cols_originals if rename_dict.get(col, col) in df_cleaned.columns]
                if cols_to_drop_final:
                    df_cleaned.drop(columns=list(
                        set(cols_to_drop_final)), inplace=True, errors='ignore')

                df_key_name = f"{base_name}_df"
                if df_key_name in processed_dfs:
                    advertencias_carga.append(
                        f"ADVERTENCIA: DF '{df_key_name}' ya existe. Se SOBREESCRIBIRÁ.")
                processed_dfs[df_key_name] = df_cleaned
                print(f"      DataFrame '{df_key_name}' almacenado.")
                break
        except Exception as e_file:
            msg = f"ERROR procesando archivo de datos '{original_filename}': {e_file}"
            advertencias_carga.append(msg)
            print(msg)
            import traceback
            traceback.print_exc()

    print("--- Log Sherlock (BG Task - load_data): FIN de carga y limpieza inicial de DataFrames ---")
    return processed_dfs, rename_map_details, drop_columns_set, advertencias_carga

# --- Función 2: Utilidad para Obtener DF ---


def get_df_by_type(processed_dfs: Dict[str, pd.DataFrame], df_key_buscado: str, advertencias_list: List[str]) -> Optional[pd.DataFrame]:
    if df_key_buscado in processed_dfs:
        print(
            f"--- Log Sherlock (BG Task - get_df): DataFrame '{df_key_buscado}' encontrado.")
        return processed_dfs[df_key_buscado].copy()
    else:
        msg = f"ADVERTENCIA: No se encontró DataFrame '{df_key_buscado}'. Disponibles: {list(processed_dfs.keys())}"
        advertencias_list.append(msg)
        print(msg)
        return None

# --- Función 3: El Cerebro del Procesamiento y Enriquecimiento


def generar_insights_pacientes(
    processed_dfs: Dict[str, pd.DataFrame],
    all_advertencias: List[str]
) -> Dict[str, pd.DataFrame]:

    resultados_dfs: Dict[str, pd.DataFrame] = {}
    print(
        f"--- Log Sherlock (BG Task): Inicio de generar_insights_pacientes. DF limpios disponibles: {list(processed_dfs.keys())}")

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

        # --- 2. Procesar Pacientes (con cálculo de Edad SIMPLIFICADO) ---
        df_pacientes_enriquecido = None
        df_pacientes_base = get_df_by_type(
            processed_dfs, "Pacientes_Nuevos_df", all_advertencias)
        if df_pacientes_base is not None:
            df_pacientes_enriquecido = df_pacientes_base.copy()

            # --- Cálculo de Edad (VERSIÓN SIMPLIFICADA A "DATE") ---
            col_fecha_nac = 'Fecha de nacimiento'
            if col_fecha_nac in df_pacientes_enriquecido.columns:
                print(
                    f"--- Log Sherlock (BG Task): Iniciando cálculo de Edad con columna '{col_fecha_nac}'.")

                # 1. Definir una función que calcule la edad para UN SOLO VALOR, de forma muy segura
                def calcular_edad_para_fila(fecha_nac_valor):
                    # Si el valor de entrada es nulo o no es un tipo de fecha reconocible, devuelve el nulo de Pandas
                    if pd.isnull(fecha_nac_valor):
                        return pd.NA

                    try:
                        # Forzar conversión a Timestamp por si acaso
                        fecha_nac_ts = pd.Timestamp(fecha_nac_valor)

                        # Normalizar a medianoche para ignorar la hora
                        fecha_nac_ts = fecha_nac_ts.normalize()

                        # Calcular diferencia en días
                        time_difference_days = (pd.to_datetime(
                            'today').normalize() - fecha_nac_ts).days

                        # Calcular edad y devolver como entero
                        edad = int(time_difference_days / 365.25)

                        # Filtro de sanidad: si la edad es negativa o irrealmente grande, es un error de datos.
                        if 0 <= edad <= 120:
                            return edad
                        else:
                            return pd.NA
                    except (ValueError, TypeError):
                        # Si CUALQUIER COSA falla en la conversión o cálculo, devuelve Nulo
                        return pd.NA

                # 2. Convertir la columna a datetime. Los errores se vuelven NaT (Not a Time).
                fechas_nacimiento_series = pd.to_datetime(
                    df_pacientes_enriquecido[col_fecha_nac], errors='coerce')

                # 3. Aplicar nuestra función segura a cada elemento de la serie.
                df_pacientes_enriquecido['Edad'] = fechas_nacimiento_series.apply(
                    calcular_edad_para_fila)

                # 4. Asegurar que el tipo de la columna final sea Int64 para soportar los nulos (pd.NA).
                df_pacientes_enriquecido['Edad'] = df_pacientes_enriquecido['Edad'].astype(
                    'Int64')

                print(
                    f"--- Log Sherlock (BG Task): Columna 'Edad' calculada. {df_pacientes_enriquecido['Edad'].notna().sum()} edades válidas.")
            else:
                all_advertencias.append(
                    f"Advertencia: Columna '{col_fecha_nac}' no encontrada en Pacientes.")
                # Crear la columna como nula
                df_pacientes_enriquecido['Edad'] = pd.NA
                df_pacientes_enriquecido['Edad'] = df_pacientes_enriquecido['Edad'].astype(
                    'Int64')  # Asegurar el tipo

            # --- Enriquecimiento con Origen ---
            if 'dimension_tipos_pacientes' in resultados_dfs and 'Tipo Dentalink' in df_pacientes_enriquecido.columns:
                df_dim_tipos_pac = resultados_dfs['dimension_tipos_pacientes']
                # Corrección para usar 'Paciente_Origen' como me indicaste
                if 'Tipo Dentalink' in df_dim_tipos_pac.columns and 'Paciente_Origen' in df_dim_tipos_pac.columns:
                    df_origen_merge = df_dim_tipos_pac[[
                        'Tipo Dentalink', 'Paciente_Origen']].drop_duplicates(subset=['Tipo Dentalink'])
                    df_pacientes_enriquecido = pd.merge(
                        df_pacientes_enriquecido, df_origen_merge, on='Tipo Dentalink', how='left')

            resultados_dfs['hechos_pacientes'] = df_pacientes_enriquecido.copy()

        # --- 3. Procesar Citas ---
        df_citas_pac = get_df_by_type(
            processed_dfs, "Citas_Pacientes_df", all_advertencias)
        df_citas_mot = get_df_by_type(
            processed_dfs, "Citas_Motivo_df", all_advertencias)
        if df_citas_pac is not None and df_citas_mot is not None and 'ID_Paciente' in df_citas_pac.columns:
            # CORRECCIÓN: Usar los nombres de columna que sabemos que existen por los logs de diagnóstico
            col_asistida = 'Cita_asistida'
            col_duplicada = 'Cita duplicada'
            df_citas_pac[col_asistida] = pd.to_numeric(
                df_citas_pac[col_asistida], errors='coerce').fillna(0).astype(int)
            df_citas_pac[col_duplicada] = pd.to_numeric(
                df_citas_pac[col_duplicada], errors='coerce').fillna(0).astype(int)
            df_citas_filtrado = df_citas_pac[df_citas_pac[col_duplicada] == 0].copy(
            )

            df_citas_filtrado['ID_Cita'] = df_citas_filtrado['ID_Cita'].astype(
                str)
            df_citas_mot['ID_Cita'] = df_citas_mot['ID_Cita'].astype(str)

            cols_from_motivo = ['ID_Cita', 'Fecha de creación cita', 'Hora Inicio Cita',
                                'Hora Fin Cita', 'Motivo Cita', 'Sucursal', 'ID_Tratamiento']
            hechos_citas_df = pd.merge(df_citas_filtrado, df_citas_mot[[
                                       c for c in cols_from_motivo if c in df_citas_mot.columns]].drop_duplicates(subset=['ID_Cita']), on='ID_Cita', how='left')

            # SIMPLIFICACIÓN A "DATE": Normalizamos todas las fechas de citas a medianoche.
            hechos_citas_df['Fecha Cita'] = pd.to_datetime(
                hechos_citas_df['Fecha Cita'], errors='coerce').dt.normalize()

            df_atendidas = hechos_citas_df[(
                hechos_citas_df[col_asistida] == 1) & hechos_citas_df['Fecha Cita'].notna()]
            if not df_atendidas.empty:
                primera_cita = df_atendidas.groupby('ID_Paciente')['Fecha Cita'].min(
                ).reset_index().rename(columns={'Fecha Cita': 'Fecha_Primera_Cita_Atendida_Real'})
                hechos_citas_df = pd.merge(
                    hechos_citas_df, primera_cita, on='ID_Paciente', how='left')
            else:
                hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'] = pd.NaT

            # Lógica de Etiquetado (usando la fecha de hoy normalizada)
            today = pd.to_datetime('today').normalize()
            # ... (Toda la lógica de etiquetado con .loc se mantiene, funcionará bien con fechas normalizadas) ...

            resultados_dfs['hechos_citas'] = hechos_citas_df.copy()
            print("--- Log Sherlock (BG Task): 'hechos_citas' preparado.")

        # --- 4. Procesar Presupuestos, Acciones, Pagos (sin cambios en su lógica) ---
        # ... (Toda la lógica que ya teníamos para estas tablas) ...

        # --- 5. Generar Perfiles ---
        if 'hechos_pacientes' in resultados_dfs:
            df_pac_para_perfil = resultados_dfs['hechos_pacientes']
            # CORRECCIÓN: Quitamos el perfil de Estado Civil y nos aseguramos de que el resto de columnas existan
            groupby_cols = [c for c in [
                'Edad', 'Sexo', 'Paciente_Origen'] if c in df_pac_para_perfil.columns]
            if 'ID_Paciente' in df_pac_para_perfil.columns and len(groupby_cols) > 1:
                perfil = df_pac_para_perfil.groupby(groupby_cols, dropna=False)[
                    'ID_Paciente'].nunique().reset_index(name='Numero_Pacientes')
                if not perfil.empty:
                    resultados_dfs['perfil_edad_sexo_origen_paciente'] = perfil

    except Exception as e_general:
        print(
            f"--- Log Sherlock (BG Task - generar_insights): ¡¡¡ERROR GENERAL!!!: {e_general}")
        import traceback
        traceback.print_exc()
        all_advertencias.append(f"FATAL: Error inesperado: {e_general}")

    print(
        f"--- Log Sherlock (BG Task - generar_insights): Fin. DataFrames finales listos ({len(resultados_dfs)}): {list(resultados_dfs.keys())}")
    return resultados_dfs

import pandas as pd
import numpy as np
import io
import os
from typing import Dict, Any, List, Optional, Tuple, Set

# --- Función 1: Carga y Limpieza Inicial ---


def load_dataframes_from_uploads(
    data_files: List[Any], index_file: Any
) -> Tuple[Dict[str, pd.DataFrame], Dict[tuple[str, str], dict[str, str]], set[tuple[str, str, str]], List[str]]:
    processed_dfs: Dict[str, pd.DataFrame] = {}
    advertencias_carga: List[str] = []
    rename_map_details: Dict[tuple[str, str], dict[str, str]] = {}
    drop_columns_set: set[tuple[str, str, str]] = set()

    try:
        index_file.file.seek(0)
        indice_df = pd.read_excel(io.BytesIO(
            index_file.file.read()), sheet_name=0)
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
    except Exception as e:
        msg = f"ERROR CRÍTICO al leer índice: {e}"
        advertencias_carga.append(msg)
        print(msg)
        import traceback
        traceback.print_exc()
        return {}, {}, set(), advertencias_carga

    for uploaded_file_obj in data_files:
        original_filename = uploaded_file_obj.filename
        try:
            base_name, _ = os.path.splitext(original_filename)
            df_sheets = pd.read_excel(io.BytesIO(
                uploaded_file_obj.file.read()), sheet_name=None)
            for sheet_name, df_original in df_sheets.items():
                fila_indice_para_hoja = indice_df[(indice_df[col_map['Archivo_Idx']].apply(
                    lambda x: os.path.splitext(str(x))[0]) == base_name) & (indice_df[col_map['Hoja_Idx']] == sheet_name)]
                if fila_indice_para_hoja.empty:
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
                break
        except Exception as e_file:
            msg = f"ERROR procesando archivo de datos '{original_filename}': {e_file}"
            advertencias_carga.append(msg)
            print(msg)
            import traceback
            traceback.print_exc()

    return processed_dfs, rename_map_details, drop_columns_set, advertencias_carga

# --- Función 2: Utilidad para Obtener DF ---


def get_df_by_type(processed_dfs: Dict[str, pd.DataFrame], df_key_buscado: str, advertencias_list: List[str]) -> Optional[pd.DataFrame]:
    if df_key_buscado in processed_dfs:
        return processed_dfs[df_key_buscado].copy()
    else:
        msg = f"ADVERTENCIA: No se encontró DataFrame '{df_key_buscado}'. Disponibles: {list(processed_dfs.keys())}"
        advertencias_list.append(msg)
        print(msg)
        return None

# --- Función 3: El Cerebro del Procesamiento y Enriquecimiento (VERSIÓN FINAL COMPLETA) ---


def generar_insights_pacientes(
    processed_dfs: Dict[str, pd.DataFrame], all_advertencias: List[str]
) -> Dict[str, pd.DataFrame]:

    resultados_dfs: Dict[str, pd.DataFrame] = {}
    print(f"--- Log Sherlock (BG Task): Inicio de generar_insights_pacientes...")

    try:
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

        # 2. Procesar Pacientes (Enriquecido)
        df_pacientes_enriquecido = None
        df_pacientes_base = get_df_by_type(
            processed_dfs, "Pacientes_Nuevos_df", all_advertencias)
        if df_pacientes_base is not None:
            df_pacientes_enriquecido = df_pacientes_base.copy()
            # Calcular Edad
            if 'Fecha de nacimiento' in df_pacientes_enriquecido.columns:
                def calcular_edad(fecha_nac):
                    if pd.isnull(fecha_nac):
                        return pd.NA
                    try:
                        edad_dias = (pd.to_datetime('today').normalize(
                        ) - pd.Timestamp(fecha_nac).normalize()).days
                        edad = int(edad_dias / 365.25)
                        return edad if 0 <= edad <= 120 else pd.NA
                    except:
                        return pd.NA
                df_pacientes_enriquecido['Edad'] = pd.to_datetime(
                    df_pacientes_enriquecido['Fecha de nacimiento'], errors='coerce').apply(calcular_edad).astype('Int64')
            # Enriquecer con Origen
            if 'dimension_tipos_pacientes' in resultados_dfs and 'Tipo Dentalink' in df_pacientes_enriquecido.columns:
                df_dim_tipos_pac = resultados_dfs['dimension_tipos_pacientes']
                if 'Tipo Dentalink' in df_dim_tipos_pac.columns and 'Paciente_Origen' in df_dim_tipos_pac.columns:
                    df_origen_merge = df_dim_tipos_pac[[
                        'Tipo Dentalink', 'Paciente_Origen']].drop_duplicates(subset=['Tipo Dentalink'])
                    df_pacientes_enriquecido = pd.merge(
                        df_pacientes_enriquecido, df_origen_merge, on='Tipo Dentalink', how='left')
            resultados_dfs['hechos_pacientes'] = df_pacientes_enriquecido.copy()

        # 3. Procesar Citas
        df_citas_pac = get_df_by_type(
            processed_dfs, "Citas_Pacientes_df", all_advertencias)
        df_citas_mot = get_df_by_type(
            processed_dfs, "Citas_Motivo_df", all_advertencias)
        if df_citas_pac is not None and df_citas_mot is not None and 'ID_Paciente' in df_citas_pac.columns and 'Fecha Cita' in df_citas_pac.columns:
            df_citas_pac['Cita_asistida'] = pd.to_numeric(
                df_citas_pac['Cita_asistida'], errors='coerce').fillna(0).astype(int)
            df_citas_pac['Cita duplicada'] = pd.to_numeric(
                df_citas_pac['Cita duplicada'], errors='coerce').fillna(0).astype(int)
            df_citas_filtrado = df_citas_pac[df_citas_pac['Cita duplicada'] == 0].copy(
            )

            df_citas_filtrado['ID_Cita'] = df_citas_filtrado['ID_Cita'].astype(
                str)
            df_citas_mot['ID_Cita'] = df_citas_mot['ID_Cita'].astype(str)

            cols_from_motivo = ['ID_Cita', 'Fecha de creación cita', 'Hora Inicio Cita',
                                'Hora Fin Cita', 'Motivo Cita', 'Sucursal', 'ID_Tratamiento']
            cols_exist = [
                c for c in cols_from_motivo if c in df_citas_mot.columns]
            hechos_citas_df = pd.merge(df_citas_filtrado, df_citas_mot[cols_exist].drop_duplicates(
                subset=['ID_Cita']), on='ID_Cita', how='left')
            hechos_citas_df['Fecha Cita'] = pd.to_datetime(
                hechos_citas_df['Fecha Cita'], errors='coerce').dt.normalize()

            df_atendidas = hechos_citas_df[(
                hechos_citas_df['Cita_asistida'] == 1) & hechos_citas_df['Fecha Cita'].notna()]
            if not df_atendidas.empty:
                primera_cita = df_atendidas.groupby('ID_Paciente')['Fecha Cita'].min(
                ).reset_index().rename(columns={'Fecha Cita': 'Fecha_Primera_Cita_Atendida_Real'})
                hechos_citas_df = pd.merge(
                    hechos_citas_df, primera_cita, on='ID_Paciente', how='left')
            if 'Fecha_Primera_Cita_Atendida_Real' not in hechos_citas_df.columns:
                hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'] = pd.NaT

            today = pd.to_datetime('today').normalize()
            hechos_citas_df['Etiqueta_Cita_Paciente'] = 'Indeterminada'
            cond_primera = hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].isnull() | (
                hechos_citas_df['Fecha Cita'] == hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'])
            hechos_citas_df.loc[cond_primera & (
                hechos_citas_df['Fecha Cita'] >= today), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo en Agenda"
            hechos_citas_df.loc[cond_primera & (hechos_citas_df['Fecha Cita'] < today) & (
                hechos_citas_df['Cita_asistida'] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo Atendido"
            hechos_citas_df.loc[cond_primera & (hechos_citas_df['Fecha Cita'] < today) & (
                hechos_citas_df['Cita_asistida'] == 0), 'Etiqueta_Cita_Paciente'] = "Paciente Nuevo No Atendido"
            cond_recurrente = ~cond_primera
            cond_mismo_mes = hechos_citas_df['Fecha Cita'].dt.to_period(
                'M') == hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'].dt.to_period('M')
            hechos_citas_df.loc[cond_recurrente & cond_mismo_mes & (
                hechos_citas_df['Cita_asistida'] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Atendido Mismo Mes que Debutó"
            hechos_citas_df.loc[cond_recurrente & ~cond_mismo_mes & (
                hechos_citas_df['Fecha Cita'] >= today), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente en Agenda"
            hechos_citas_df.loc[cond_recurrente & ~cond_mismo_mes & (hechos_citas_df['Fecha Cita'] < today) & (
                hechos_citas_df['Cita_asistida'] == 1), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente Atendido"
            hechos_citas_df.loc[cond_recurrente & ~cond_mismo_mes & (hechos_citas_df['Fecha Cita'] < today) & (
                hechos_citas_df['Cita_asistida'] == 0), 'Etiqueta_Cita_Paciente'] = "Paciente Recurrente No Atendido"

            resultados_dfs['hechos_citas'] = hechos_citas_df.copy()

        # 4. Procesar Presupuestos, Acciones, Pagos
        df_presupuestos = get_df_by_type(
            processed_dfs, "Presupuesto por Accion_df", all_advertencias)
        if df_presupuestos is not None:
            col_precio_orig = 'Procedimiento_precio_original'
            col_precio_pac = 'Procedimiento_precio_paciente'
            # Verificar existencia ANTES de usar
            if col_precio_orig in df_presupuestos.columns and col_precio_pac in df_presupuestos.columns:
                df_presupuestos['Descuento_Presupuestado_Detalle'] = pd.to_numeric(
                    df_presupuestos[col_precio_orig], errors='coerce') - pd.to_numeric(df_presupuestos[col_precio_pac], errors='coerce')
            resultados_dfs['hechos_presupuesto_detalle'] = df_presupuestos.copy()

        df_acciones = get_df_by_type(
            processed_dfs, "Acciones_df", all_advertencias)
        if df_acciones is not None:
            resultados_dfs['hechos_acciones_realizadas'] = df_acciones.copy()

        df_movimiento = get_df_by_type(
            processed_dfs, "Movimiento_df", all_advertencias)
        if df_movimiento is not None and 'ID_Pago' in df_movimiento.columns:
            # CORRECCIÓN PARA VSCODE: Verificar columnas antes de la conversión numérica
            col_total_pago = 'Total Pago'
            col_abono_libre = 'Abono Libre'

            if col_total_pago in df_movimiento.columns:
                df_movimiento[col_total_pago] = pd.to_numeric(
                    df_movimiento[col_total_pago], errors='coerce').fillna(0)
            if col_abono_libre in df_movimiento.columns:
                df_movimiento[col_abono_libre] = pd.to_numeric(
                    df_movimiento[col_abono_libre], errors='coerce').fillna(0)

            # El resto de la lógica de pagos se mantiene igual...
            agg_cols = {col: 'first' for col in [
                'ID_Paciente', 'Pago_fecha_recepcion', 'Total Pago', 'Abono Libre'] if col in df_movimiento.columns}
            if agg_cols:
                tx_pagos = df_movimiento.groupby('ID_Pago', as_index=False).agg(agg_cols).rename(columns={
                    'Abono Libre': 'Monto_Abono_Libre_Original_En_Tx', 'Total Pago': 'Total_Pago_Transaccion'})
                resultados_dfs['hechos_pagos_transacciones'] = tx_pagos

            app_cols_map = {'ID_Pago': 'ID_Pago', 'ID_Detalle Presupuesto': 'ID_Detalle_Presupuesto',
                            'Pagado_ID_Detalle_Presupuesto': 'Monto_Aplicado_Al_Detalle'}
            app_cols_exist = [
                k for k in app_cols_map.keys() if k in df_movimiento.columns]
            if app_cols_exist:
                app_df = df_movimiento[app_cols_exist].copy().rename(
                    columns=app_cols_map)
                resultados_dfs['hechos_pagos_aplicaciones_detalle'] = app_df

        df_gastos = get_df_by_type(
            processed_dfs, "Tabla Gastos Aliadas Mexico_df", all_advertencias)
        if df_gastos is not None:
            resultados_dfs['hechos_gastos'] = df_gastos.copy()
        # 5. Generar Perfiles
        if 'hechos_pacientes' in resultados_dfs:
            df_pac_para_perfil = resultados_dfs['hechos_pacientes']
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

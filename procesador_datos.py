import pandas as pd
import numpy as np
import io
import os
from typing import Dict, Any, List, Optional, Tuple, Set


# --- Función de Ayuda: Reemplazar espacios con guiones bajos ---
def replace_spaces_with_underscores(name: str) -> str:
    """
    Reemplaza los espacios en un string con guiones bajos,
    pero mantiene la capitalización original.
    Ej: 'Nombre Paciente' -> 'Nombre_Paciente'
    """
    if not isinstance(name, str):
        return name
    # Reemplaza espacios y guiones con guiones bajos
    return name.replace(' ', '_').replace('-', '_')

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
                # Esta lógica busca la primera hoja que coincida, asumiendo una por archivo.
                df_cleaned = df_original.copy()
                rename_dict = rename_map_details.get(
                    (base_name, sheet_name), rename_map_details.get((base_name, 'default'), {}))
                if rename_dict:
                    df_cleaned.rename(columns=rename_dict, inplace=True)

                drop_cols_originals = {col for (ab, h, col) in drop_columns_set if ab == base_name and (
                    h == sheet_name or h == 'default')}

                # Nombres de columnas a eliminar después de un posible renombrado
                cols_to_drop_final = {rename_dict.get(
                    col, col) for col in drop_cols_originals}

                # Columnas que realmente existen en el DataFrame para evitar errores
                actual_cols_to_drop = [
                    col for col in cols_to_drop_final if col in df_cleaned.columns]

                if actual_cols_to_drop:
                    df_cleaned.drop(columns=actual_cols_to_drop,
                                    inplace=True, errors='ignore')

                # La clave del DF se crea a partir del nombre del archivo base, CON espacios si los tiene.
                df_key_name = f"{base_name}_df"
                if df_key_name in processed_dfs:
                    advertencias_carga.append(
                        f"ADVERTENCIA: DF '{df_key_name}' ya existe. Se SOBREESCRIBIRÁ.")
                processed_dfs[df_key_name] = df_cleaned
                # Solo procesamos la primera hoja que encontramos por archivo para simplificar
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

# --- Función 3: El Cerebro del Procesamiento y Enriquecimiento ---


def generar_insights_pacientes(
    processed_dfs: Dict[str, pd.DataFrame], all_advertencias: List[str]
) -> Dict[str, pd.DataFrame]:

    resultados_dfs: Dict[str, pd.DataFrame] = {}
    print(f"--- Log Sherlock (BG Task): Inicio de generar_insights_pacientes...")

    try:
        # --- PASO 1: Preparar Tablas de Dimensiones ---
        print("--- PASO 1: Preparando tablas de dimensiones...")
        dimension_mapping = {
            "Tipos de pacientes_df": "dimension_tipos_pacientes",
            "Tabla_Procedimientos_df": "dimension_procedimientos",
            "Sucursal_df": "dimension_sucursales",
            "Lada_df": "dimension_lada",
            "Tratamiento Generado Mex_df": "dimension_tratamientos_generados",
            "Medios_de_pago_df": "dimension_medios_de_pago"
        }
        for df_key, table_name in dimension_mapping.items():
            df_dim = get_df_by_type(processed_dfs, df_key, all_advertencias)
            if df_dim is not None:
                resultados_dfs[table_name] = df_dim.copy()

        # --- PASO 1.5: Añadir 'Sucursal' a dimension_tratamientos_generados ---
        print(
            "--- PASO 1.5: Enriqueciendo dimension_tratamientos_generados con Sucursal...")
        df_presupuesto_base = get_df_by_type(
            processed_dfs, "Presupuesto por Accion_df", all_advertencias)
        df_tratamientos_generados = resultados_dfs.get(
            "dimension_tratamientos_generados")

        if df_presupuesto_base is not None and df_tratamientos_generados is not None and 'ID_Tratamiento' in df_presupuesto_base and 'Sucursal' in df_presupuesto_base:
            # 1. Crear un mapa simple: ID_Tratamiento -> Sucursal, eliminando duplicados.
            mapa_sucursales = df_presupuesto_base[[
                'ID_Tratamiento', 'Sucursal']].drop_duplicates(subset=['ID_Tratamiento'])

            # 2. Unir (merge) con la tabla de tratamientos generados usando el ID_Tratamiento.
            df_tratamientos_actualizado = pd.merge(
                df_tratamientos_generados,
                mapa_sucursales,
                on='ID_Tratamiento',
                # Usamos 'left' para no perder tratamientos aunque no tengan sucursal.
                how='left'
            )

            # 3. Reemplazar el DataFrame viejo con el nuevo que ya incluye la sucursal.
            resultados_dfs['dimension_tratamientos_generados'] = df_tratamientos_actualizado
            print(
                "    - ¡Éxito! La columna 'Sucursal' ha sido agregada a dimension_tratamientos_generados.")
        else:
            all_advertencias.append(
                "ADVERTENCIA: No se pudo añadir 'Sucursal' a dimension_tratamientos_generados por falta de datos base.")
            print("    - ADVERTENCIA: Faltan datos para agregar la columna 'Sucursal'.")

        # --- PASO 2: Procesar y Enriquecer `hechos_pacientes` ---
        print("--- PASO 2: Procesando y enriqueciendo pacientes...")
        df_pacientes_enriquecido = None
        df_pacientes_base = get_df_by_type(
            processed_dfs, "Pacientes_Nuevos_df", all_advertencias)
        if df_pacientes_base is not None:
            df_pacientes_enriquecido = df_pacientes_base.copy()
            if 'Fecha_de_nacimiento' in df_pacientes_enriquecido.columns:
                def calcular_edad(fecha_nac):
                    if pd.isnull(fecha_nac):
                        return pd.NA
                    try:
                        edad_dias = (pd.to_datetime('today').normalize(
                        ) - pd.Timestamp(fecha_nac).normalize()).days
                        return int(edad_dias / 365.25) if 0 <= int(edad_dias / 365.25) <= 120 else pd.NA
                    except:
                        return pd.NA
                df_pacientes_enriquecido['Edad'] = pd.to_datetime(
                    df_pacientes_enriquecido['Fecha_de_nacimiento'], errors='coerce').apply(calcular_edad).astype('Int64')

            if 'dimension_tipos_pacientes' in resultados_dfs and 'Tipo_Dentalink' in df_pacientes_enriquecido.columns:
                df_dim_tipos_pac = resultados_dfs['dimension_tipos_pacientes']
                if 'Tipo_Dentalink' in df_dim_tipos_pac.columns and 'Paciente_Origen' in df_dim_tipos_pac.columns:
                    df_origen_merge = df_dim_tipos_pac[[
                        'Tipo_Dentalink', 'Paciente_Origen']].drop_duplicates(subset=['Tipo_Dentalink'])
                    df_pacientes_enriquecido = pd.merge(
                        df_pacientes_enriquecido, df_origen_merge, on='Tipo_Dentalink', how='left')
            resultados_dfs['hechos_pacientes'] = df_pacientes_enriquecido.copy()

        # --- PASO 3: Procesar y Enriquecer `hechos_citas` ---
        print("--- PASO 3: Procesando y enriqueciendo citas...")
        hechos_citas_df = None
        df_citas_pac = get_df_by_type(
            processed_dfs, "Citas_Pacientes_df", all_advertencias)
        df_citas_mot = get_df_by_type(
            processed_dfs, "Citas_Motivo_df", all_advertencias)

        if df_citas_pac is not None and df_citas_mot is not None and 'ID_Paciente' in df_citas_pac.columns and 'Fecha_Cita' in df_citas_pac.columns:
            try:
                col_id_cita, col_asistida, col_duplicada, col_id_paciente, col_fecha_cita = 'ID_Cita', 'Cita_asistida', 'Cita_duplicada', 'ID_Paciente', 'Fecha_Cita'
                df_citas_pac[col_asistida] = pd.to_numeric(
                    df_citas_pac[col_asistida], errors='coerce').fillna(0).astype(int)
                df_citas_pac[col_duplicada] = pd.to_numeric(
                    df_citas_pac[col_duplicada], errors='coerce').fillna(0).astype(int)
                df_citas_filtrado = df_citas_pac[df_citas_pac[col_duplicada] == 0].copy(
                )
                df_citas_filtrado[col_id_cita] = df_citas_filtrado[col_id_cita].astype(
                    str)
                df_citas_mot[col_id_cita] = df_citas_mot[col_id_cita].astype(
                    str)

                cols_from_motivo = ['ID_Cita', 'Cita_Creacion', 'Hora_Inicio_Cita',
                                    'Hora_Fin_Cita', 'Motivo_Cita', 'Sucursal', 'ID_Tratamiento']
                cols_exist = [
                    c for c in cols_from_motivo if c in df_citas_mot.columns]
                hechos_citas_df = pd.merge(df_citas_filtrado, df_citas_mot[cols_exist].drop_duplicates(
                    subset=[col_id_cita]), on=col_id_cita, how='left')

                hechos_citas_df[col_fecha_cita] = pd.to_datetime(
                    hechos_citas_df[col_fecha_cita], errors='coerce').dt.normalize()

                df_atendidas = hechos_citas_df[(hechos_citas_df[col_asistida] == 1) & (
                    hechos_citas_df[col_fecha_cita].notna())]
                if not df_atendidas.empty:
                    primera_cita = df_atendidas.groupby(col_id_paciente)[col_fecha_cita].min(
                    ).reset_index().rename(columns={col_fecha_cita: 'Fecha_Primera_Cita_Atendida_Real'})
                    hechos_citas_df = pd.merge(
                        hechos_citas_df, primera_cita, on=col_id_paciente, how='left')
                if 'Fecha_Primera_Cita_Atendida_Real' not in hechos_citas_df.columns:
                    hechos_citas_df['Fecha_Primera_Cita_Atendida_Real'] = pd.NaT

                # ... (resto de la lógica de etiquetas de citas) ...

                col_hora_inicio, col_hora_fin = 'Hora_Inicio_Cita', 'Hora_Fin_Cita'
                if col_hora_inicio in hechos_citas_df.columns and col_hora_fin in hechos_citas_df.columns:
                    # ... (lógica de duración de citas) ...
                    pass

            except Exception as e_citas:
                all_advertencias.append(
                    f"ERROR general procesando citas: {e_citas}")

        if hechos_citas_df is not None:
            resultados_dfs['hechos_citas'] = hechos_citas_df.copy()

        # --- PASO 4: Procesar Otros Hechos de Negocio ---
        print("--- PASO 4: Procesando presupuestos, acciones, pagos y gastos...")
        df_presupuestos = get_df_by_type(
            processed_dfs, "Presupuesto por Accion_df", all_advertencias)
        if df_presupuestos is not None:
            if 'Tratamiento_fecha_de_generacion' in df_presupuestos.columns:
                df_presupuestos['Tratamiento_fecha_de_generacion'] = pd.to_datetime(
                    df_presupuestos['Tratamiento_fecha_de_generacion'], errors='coerce')
            if 'Procedimiento_precio_original' in df_presupuestos.columns and 'Procedimiento_precio_paciente' in df_presupuestos.columns:
                df_presupuestos['Descuento_Presupuestado_Detalle'] = pd.to_numeric(
                    df_presupuestos['Procedimiento_precio_original'], errors='coerce') - pd.to_numeric(df_presupuestos['Procedimiento_precio_paciente'], errors='coerce')
            resultados_dfs['hechos_presupuesto_detalle'] = df_presupuestos.copy()

        df_acciones = get_df_by_type(
            processed_dfs, "Acciones_df", all_advertencias)
        if df_acciones is not None:
            df_acciones.reset_index(inplace=True)
            df_acciones.rename(
                columns={'index': 'ID_Accion_Unico'}, inplace=True)
            resultados_dfs['hechos_acciones_realizadas'] = df_acciones.copy()

        df_movimiento = get_df_by_type(
            processed_dfs, "Movimiento_df", all_advertencias)
        if df_movimiento is not None and 'ID_Pago' in df_movimiento.columns:
            if 'Total_Pago' in df_movimiento.columns:
                df_movimiento['Total_Pago'] = pd.to_numeric(
                    df_movimiento['Total_Pago'], errors='coerce').fillna(0)
            if 'Abono_Libre' in df_movimiento.columns:
                df_movimiento['Abono_Libre'] = pd.to_numeric(
                    df_movimiento['Abono_Libre'], errors='coerce').fillna(0)

            # Renombrar 'Medio_de_pago_dentalink' a 'ID_Medio_de_pago' si existe
            if 'Medio_de_pago_dentalink' in df_movimiento.columns:
                df_movimiento.rename(
                    columns={'Medio_de_pago_dentalink': 'ID_Medio_de_pago'}, inplace=True)

            agg_cols = {col: 'first' for col in ['ID_Paciente', 'Pago_fecha_recepcion', 'Total_Pago',
                                                 'Abono_Libre', 'ID_Medio_de_pago', 'Sucursal'] if col in df_movimiento.columns}
            if agg_cols:
                tx_pagos = df_movimiento.groupby('ID_Pago', as_index=False).agg(agg_cols).rename(columns={
                    'Abono_Libre': 'Monto_Abono_Libre_Original_En_Tx', 'Total_Pago': 'Total_Pago_Transaccion'})
                resultados_dfs['hechos_pagos_transacciones'] = tx_pagos

            app_cols_map = {'ID_Pago': 'ID_Pago', 'ID_Detalle_Presupuesto': 'ID_Detalle_Presupuesto',
                            'Pagado_ID_Detalle_Presupuesto': 'Monto_Aplicado_Al_Detalle', 'Pago_fecha_recepcion': 'pago_fecha_recepcion', 'Sucursal': 'Sucursal'}

            app_cols_exist = [
                k for k in app_cols_map.keys() if k in df_movimiento.columns]
            if app_cols_exist:
                app_df = df_movimiento[app_cols_exist].copy().rename(
                    columns=app_cols_map)
                resultados_dfs['hechos_pagos_aplicaciones_detalle'] = app_df

        df_gastos = get_df_by_type(
            processed_dfs, "Tabla Gastos Aliadas Mexico_df", all_advertencias)
        if df_gastos is not None:
            df_gastos.reset_index(inplace=True)
            df_gastos.rename(columns={'index': 'ID_Gasto_Unico'}, inplace=True)
            resultados_dfs['hechos_gastos'] = df_gastos.copy()

        # --- PASO 5: Generar Perfiles Agregados ---
        print("--- PASO 5: Generando perfiles de pacientes...")
        if 'hechos_pacientes' in resultados_dfs:
            df_pac_para_perfil = resultados_dfs['hechos_pacientes']
            groupby_cols = [c for c in [
                'Edad', 'Sexo', 'Paciente_Origen'] if c in df_pac_para_perfil.columns]
            if 'ID_Paciente' in df_pac_para_perfil.columns and len(groupby_cols) > 1:
                perfil = df_pac_para_perfil.groupby(groupby_cols, dropna=False)[
                    'ID_Paciente'].nunique().reset_index(name='Numero_Pacientes')
                if not perfil.empty:
                    resultados_dfs['perfil_edad_sexo_origen_paciente'] = perfil

        # --- PASO FINAL 1: Estandarizar espacios a guiones bajos en columnas ---
        print("--- PASO FINAL 1: Reemplazando espacios con guiones bajos en todas las columnas...")
        resultados_formateados_dfs: Dict[str, pd.DataFrame] = {}
        for table_name, df in resultados_dfs.items():
            df_copy = df.copy()
            # Aplica la función de reemplazo a cada nombre de columna
            df_copy.columns = [replace_spaces_with_underscores(
                col) for col in df_copy.columns]
            resultados_formateados_dfs[table_name] = df_copy
            print(f"    - Columnas de tabla '{table_name}' formateadas.")

        # --- PASO FINAL 2: Asegurar tipos de datos de fecha correctos ---
        print("--- PASO FINAL 2: Convirtiendo columnas de fecha al formato correcto...")

        # Lista de todas las columnas que deben ser de tipo fecha en el proyecto
        columnas_de_fecha = {
            'Procedimiento_Fecha_Realizacion',
            'Fecha_Cita',
            'Cita_Creacion',
            'Fecha_Primera_Cita_Atendida_Real',
            'Fecha_del_Gasto',
            'Fecha_de_nacimiento',
            'pago_fecha_recepcion',  # Usado en la tabla de aplicaciones de pago
            'Pago_fecha_recepcion',  # Usado en la tabla de transacciones de pago
            'Tratamiento_fecha_de_generacion'
        }

        # Bucle que recorre cada tabla final
        for table_name, df in resultados_formateados_dfs.items():
            # Bucle que recorre cada columna de la tabla actual
            for col in df.columns:
                if col in columnas_de_fecha:
                    print(
                        f"    - Convirtiendo columna de fecha '{col}' en tabla '{table_name}'.")
                    # 'coerce' es clave: si una fecha no se puede leer, la convierte en Nulo (NaT) y no detiene el proceso
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        # --- Estas son las dos últimas líneas que ya tenías ---
        print(
            f"--- Log Sherlock (BG Task): Fin de generar_insights_pacientes. DataFrames finales listos ({len(resultados_formateados_dfs)}): {list(resultados_formateados_dfs.keys())}")
        return resultados_formateados_dfs

    except Exception as e_general:
        print(
            f"--- Log Sherlock (BG Task - generar_insights): ¡¡¡ERROR GENERAL!!!: {e_general}")
        import traceback
        traceback.print_exc()
        all_advertencias.append(f"FATAL: Error inesperado: {e_general}")

    print(
        f"--- Log Sherlock (BG Task): Fin de generar_insights_pacientes. DataFrames finales listos ({len(resultados_dfs)}): {list(resultados_dfs.keys())}")
    return resultados_dfs

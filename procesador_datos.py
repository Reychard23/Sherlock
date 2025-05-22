import pandas as pd
import numpy as np
import io
from typing import Dict, Any, List, Tuple, Set, Optional


def load_dataframes_from_uploads(
    data_files: List[Any],
    index_file: Any,
) -> Tuple[Dict[str, pd.DataFrame], Dict[tuple[str, str], dict[str, str]], set[tuple[str, str, str]], List[str]]:
    processed_dfs: Dict[str, pd.DataFrame] = {}
    advertencias_carga: List[str] = []
    rename_map_details: Dict[tuple[str, str], dict[str, str]] = {}
    drop_columns_set: set[tuple[str, str, str]] = set()

    print(
        f"Intentando leer índice (Excel) desde el archivo: {index_file.filename}")
    try:
        index_file.file.seek(0)
        content_indice_bytes = index_file.file.read()
        # Asume la primera hoja de indice.xlsx
        indice_df = pd.read_excel(io.BytesIO(
            content_indice_bytes), sheet_name=0)
        print("Índice (Excel) cargado correctamente.")

        # Nombres de columna REALES de tu indice.xlsx
        # (Basado en tu archivo: 'Archivo', 'Sheet', 'Columna', 'Nombre unificado', 'Acción')
        col_map = {
            'Archivo_Idx': 'Archivo',       # Nombre que usaremos en Python -> Nombre en tu Excel
            'Hoja_Idx': 'Sheet',
            'Original_Idx': 'Columna',
            'Nuevo_Idx': 'Nombre unificado',
            'Accion_Idx': 'Acción'
        }

        # Verificar que las columnas necesarias existan en el índice
        columnas_requeridas_en_indice = list(col_map.values())
        if not all(col in indice_df.columns for col in columnas_requeridas_en_indice):
            missing_cols = [
                col for col in columnas_requeridas_en_indice if col not in indice_df.columns]
            msg = f"ADVERTENCIA CRÍTICA: Faltan columnas en el archivo índice (Excel): {missing_cols}."
            advertencias_carga.append(msg)
            print(msg)
            return {}, {}, set(), advertencias_carga

        # Construir los diccionarios de renombrado y eliminación
        for _, row in indice_df.iterrows():
            # Obtener el nombre base del archivo DESDE EL ÍNDICE (quitando extensión)
            nombre_archivo_indice_con_ext = str(
                row[col_map['Archivo_Idx']]).strip()
            if nombre_archivo_indice_con_ext.lower().endswith(".xlsx"):
                archivo_base_idx = nombre_archivo_indice_con_ext[:-5]
            elif nombre_archivo_indice_con_ext.lower().endswith(".xls"):  # Por si acaso
                archivo_base_idx = nombre_archivo_indice_con_ext[:-4]
            else:
                archivo_base_idx = nombre_archivo_indice_con_ext  # Asumir ya no tiene extensión

            hoja_idx = str(row[col_map['Hoja_Idx']]).strip() if pd.notna(
                # 'default' si la hoja no se especifica
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
            f"Rename map details (para {len(rename_map_details)} comb. archivo/hoja) construido.")
        print(f"Drop columns set ({len(drop_columns_set)} reglas) construido.")

    except Exception as e:
        # ... (manejo de error como lo tenías) ...
        msg = f"ERROR CRÍTICO al leer o procesar el archivo índice (Excel) '{index_file.filename}': {e}"
        advertencias_carga.append(msg)
        print(msg)
        import traceback
        traceback.print_exc()
        return {}, {}, set(), advertencias_carga

    # Procesar cada archivo de datos subido
    for uploaded_file_obj in data_files:
        original_filename_con_ext = uploaded_file_obj.filename  # ej: "Pacientes_Nuevos.xlsx"
        print(
            f"Procesando archivo de datos subido: {original_filename_con_ext}")
        try:
            # Obtener el nombre base del archivo subido, SIN extensión. Asumimos siempre .xlsx
            if not original_filename_con_ext.lower().endswith(".xlsx"):
                advertencias_carga.append(
                    f"ADVERTENCIA: Archivo '{original_filename_con_ext}' no parece ser .xlsx. Se intentará procesar de todas formas.")
                # Si quisieras ser estricto y saltarlo: continue

            # Quita ".xlsx" de forma segura
            base_name_uploaded_file = original_filename_con_ext[:-5] if original_filename_con_ext.lower(
            ).endswith(".xlsx") else original_filename_con_ext.split('.')[0]

            df_sheets: Dict[str, pd.DataFrame] = {}
            try:  # Leer todas las hojas del Excel
                uploaded_file_obj.file.seek(0)
                file_content_bytes = uploaded_file_obj.file.read()
                df_sheets = pd.read_excel(io.BytesIO(
                    file_content_bytes), sheet_name=None)
            except Exception as e_read_excel:
                advertencias_carga.append(
                    f"ERROR: No se pudo leer el archivo Excel '{original_filename_con_ext}': {e_read_excel}")
                print(
                    f"ERROR: No se pudo leer el archivo Excel '{original_filename_con_ext}': {e_read_excel}")
                continue  # Saltar al siguiente archivo de datos

            # Iterar sobre las hojas leídas del archivo Excel actual
            for sheet_name_actual_excel, df_original in df_sheets.items():
                print(
                    f"  Inspeccionando hoja: '{sheet_name_actual_excel}' del archivo '{original_filename_con_ext}'")

                # Verificar si esta combinación específica (base_name_uploaded_file, sheet_name_actual_excel)
                # está definida en el índice (en la columna 'Sheet' para ese 'Archivo').
                # Usamos los nombres base (sin extensión) para `archivo_base_idx` que pobló los diccionarios.

                # Buscamos reglas de renombrado o eliminación para esta hoja específica.
                # El (base_name_uploaded_file, 'default') es un fallback si no hay reglas para la hoja específica.
                current_rename_dict = rename_map_details.get((base_name_uploaded_file, sheet_name_actual_excel),
                                                             # Si es None, será {}
                                                             rename_map_details.get((base_name_uploaded_file, 'default')))
                if current_rename_dict is None:
                    current_rename_dict = {}

                cols_originales_a_eliminar_hoja_actual = {
                    col_name for (ab_idx, hoja_idx, col_name) in drop_columns_set
                    if ab_idx == base_name_uploaded_file and (hoja_idx == sheet_name_actual_excel or hoja_idx == 'default')
                }

                # Para cumplir "de cada archivo usaremos únicamente una hoja para aplicarle la 'lógica' del indice":
                # Solo procesamos la hoja si tiene reglas de renombrado O de eliminación definidas
                # O si está explícitamente mencionada en el índice con acción 'KEEP' aunque no renombre/elimine nada.
                # Una forma simple: si no hay renombrados NI eliminaciones para esta hoja específica Y TAMPOCO para 'default',
                # y no está en el índice como 'KEEP' sin más, podríamos saltarla.
                # Por ahora, si el índice no la menciona explícitamente, el current_rename_dict será {} y cols_originales_a_eliminar_hoja_actual será {}.
                # Necesitamos una forma de saber si esta hoja es la "elegida" por el índice.

                # Revisamos el `indice_df` original para ver si esta hoja es la que se debe procesar:
                # (Comparando `base_name_uploaded_file` con la columna `Archivo` (sin extensión) del índice,
                # y `sheet_name_actual_excel` con la columna `Sheet` del índice)
                fila_indice_para_hoja = indice_df[
                    (indice_df[col_map['Archivo_Idx']].str.replace(r'\.xlsx?$', '', case=False, regex=True) == base_name_uploaded_file) &
                    (indice_df[col_map['Hoja_Idx']] == sheet_name_actual_excel)
                ]

                if fila_indice_para_hoja.empty:
                    print(
                        f"    Hoja '{sheet_name_actual_excel}' no está especificada en el índice para el archivo '{base_name_uploaded_file}'. Se omite esta hoja.")
                    continue  # Saltar al siguiente sheet_name_actual_excel

                # Si llegamos aquí, esta hoja SÍ está en el índice y es la que debemos procesar.
                print(
                    f"    Procesando hoja '{sheet_name_actual_excel}' según el índice.")
                df_cleaned = df_original.copy()

                # Aplicar renombrado (current_rename_dict ya está filtrado para esta hoja/default)
                df_cleaned.rename(columns=current_rename_dict, inplace=True)

                # Aplicar eliminación (cols_originales_a_eliminar_hoja_actual ya está filtrado)
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
                        f"      Columnas eliminadas: {len(columnas_finales_a_dropear_en_df_cleaned)}")

                # Nombre clave para el DataFrame procesado: "NombreBase_df"
                df_key_name = f"{base_name_uploaded_file}_df"

                if df_key_name in processed_dfs:
                    # Esto podría pasar si un archivo Excel tiene múltiples hojas Y TODAS están en el índice
                    # Y todas mapean al mismo base_name_uploaded_file.
                    # Dado que dijiste "de cada archivo usaremos únicamente una hoja", este caso no debería ocurrir
                    # si tu índice está bien definido (una sola hoja por 'Archivo Base' en el índice).
                    advertencias_carga.append(
                        f"ADVERTENCIA: Nombre de DataFrame '{df_key_name}' ya existe (generado por hoja anterior del mismo archivo o un archivo con nombre base similar). Se sobrescribirá con datos de hoja '{sheet_name_actual_excel}'.")
                processed_dfs[df_key_name] = df_cleaned
                print(
                    f"    DataFrame '{df_key_name}' (desde archivo '{original_filename_con_ext}', hoja '{sheet_name_actual_excel}') almacenado.")
                # Como solo procesamos UNA hoja por archivo (la especificada en el índice), podemos romper el bucle de hojas aquí.
                # Salir del bucle for sheet_name_actual_excel, ya procesamos la hoja correcta de este archivo.
                break

        except Exception as e:
            # ... (manejo de error como lo tenías) ...
            msg = f"ERROR al procesar el archivo de datos '{original_filename_con_ext}': {e}"
            advertencias_carga.append(msg)
            print(msg)
            import traceback
            traceback.print_exc()

    # ... (resto de la función, return) ...
    return processed_dfs, rename_map_details, drop_columns_set, advertencias_carga


def get_df_by_type(
    processed_dfs: Dict[str, pd.DataFrame],
    tipo_archivo_buscado: str,
    advertencias_list: List[str]
) -> Optional[pd.DataFrame]:
    if tipo_archivo_buscado in processed_dfs:
        print(f"DataFrame '{tipo_archivo_buscado}' encontrado.")
        return processed_dfs[tipo_archivo_buscado].copy()
    else:
        msg = f"ADVERTENCIA: No se encontró DataFrame '{tipo_archivo_buscado}'. Disponibles: {list(processed_dfs.keys())}"
        advertencias_list.append(msg)
        print(msg)
        return None


def generar_insights_pacientes(
    processed_dfs: Dict[str, pd.DataFrame],
    all_advertencias: List[str]
) -> Dict[str, pd.DataFrame]:
    resultados_dfs: Dict[str, pd.DataFrame] = {}
    df_tratamientos = get_df_by_type(
        processed_dfs, "Tratamientos", all_advertencias)
    df_pacientes = get_df_by_type(processed_dfs, "Pacientes", all_advertencias)
    df_citas = get_df_by_type(processed_dfs, "Citas", all_advertencias)

    if df_pacientes is None:
        all_advertencias.append(
            "ERROR CRÍTICO: DataFrame de Pacientes no disponible.")
        print("ERROR CRÍTICO: DataFrame de Pacientes no disponible.")
        return resultados_dfs

    print("Iniciando enriquecimiento y unión para insights...")
    try:
        df_analisis_final = df_pacientes.copy()
        if df_tratamientos is not None:
            if 'Id Paciente' in df_pacientes.columns and 'Id Paciente' in df_tratamientos.columns:
                df_analisis_final['Id Paciente'] = df_analisis_final['Id Paciente'].astype(
                    str)
                df_tratamientos['Id Paciente'] = df_tratamientos['Id Paciente'].astype(
                    str)
                df_analisis_final = pd.merge(
                    df_analisis_final, df_tratamientos, on='Id Paciente', how='left', suffixes=('_paciente', '_tratamiento'))
                print(
                    f"Merge Pacientes y Tratamientos. Filas: {len(df_analisis_final)}")
            else:
                all_advertencias.append(
                    "ADVERTENCIA: 'Id Paciente' no encontrado para merge Pacientes-Tratamientos.")
        else:
            all_advertencias.append(
                "INFO: DataFrame de Tratamientos no disponible.")

        cols_fecha_tratamiento = [
            'Fecha Creación_tratamiento', 'Fecha de Inicio', 'Fecha Terminado']
        for col in cols_fecha_tratamiento:
            if col in df_analisis_final.columns:
                try:
                    df_analisis_final[col] = pd.to_datetime(
                        df_analisis_final[col], errors='coerce')
                except Exception as e_conv:
                    all_advertencias.append(
                        f"Advertencia: Conversión fallida para '{col}': {e_conv}")

        if 'Fecha de nacimiento' in df_analisis_final.columns:
            try:
                df_analisis_final['Fecha de nacimiento'] = pd.to_datetime(
                    df_analisis_final['Fecha de nacimiento'], errors='coerce')
                if not df_analisis_final['Fecha de nacimiento'].isnull().all():
                    current_time_mex = pd.Timestamp(
                        'now', tz='America/Mexico_City')
                    if df_analisis_final['Fecha de nacimiento'].dt.tz is None:
                        df_analisis_final['Fecha de nacimiento'] = df_analisis_final['Fecha de nacimiento'].dt.tz_localize(
                            'America/Mexico_City', ambiguous='infer', nonexistent='NaT')
                    else:
                        df_analisis_final['Fecha de nacimiento'] = df_analisis_final['Fecha de nacimiento'].dt.tz_convert(
                            'America/Mexico_City')
                    time_difference = current_time_mex - \
                        df_analisis_final['Fecha de nacimiento']
                    df_analisis_final['Edad_float'] = time_difference / \
                        np.timedelta64(1, 'Y')
                    df_analisis_final['Edad'] = df_analisis_final['Edad_float'].astype(
                        'Int64')
                    print("Columna 'Edad' calculada.")
                else:
                    all_advertencias.append(
                        "Advertencia: 'Fecha de nacimiento' sin fechas válidas.")
                    df_analisis_final['Edad'] = pd.NA
            except Exception as e_edad:
                all_advertencias.append(
                    f"Advertencia: Error al calcular Edad: {e_edad}")
                df_analisis_final['Edad'] = pd.NA
        else:
            all_advertencias.append(
                "Advertencia: 'Fecha de nacimiento' no encontrada.")
            df_analisis_final['Edad'] = pd.NA

        col_sexo = 'Sexo_paciente' if 'Sexo_paciente' in df_analisis_final.columns else 'Sexo'
        col_sucursal = 'Sucursal_paciente' if 'Sucursal_paciente' in df_analisis_final.columns else 'Sucursal'
        col_estado_civil = 'Estado Civil_paciente' if 'Estado Civil_paciente' in df_analisis_final.columns else 'Estado Civil'
        profile_dimensions_pacientes = [
            'Edad', col_sexo, col_sucursal, col_estado_civil]

        if not df_analisis_final.empty:
            valid_profile_dimensions = [
                dim for dim in profile_dimensions_pacientes if dim in df_analisis_final.columns]
            if col_sexo not in valid_profile_dimensions:
                all_advertencias.append(
                    f"ADVERTENCIA: Columna '{col_sexo}' no disponible para perfiles de género.")
            else:
                if 'Edad' in valid_profile_dimensions:
                    resultados_dfs['perfil_pacientes_edad_genero'] = df_analisis_final.groupby(
                        ['Edad', col_sexo])['Id Paciente'].nunique().reset_index(name='Numero_Pacientes')
                else:
                    all_advertencias.append(
                        "Advertencia: 'Edad' no disponible para perfil Edad-Género.")
                if col_sucursal in valid_profile_dimensions:
                    resultados_dfs['perfil_pacientes_sucursal_genero'] = df_analisis_final.groupby(
                        [col_sucursal, col_sexo])['Id Paciente'].nunique().reset_index(name='Numero_Pacientes')
                else:
                    all_advertencias.append(
                        f"Advertencia: '{col_sucursal}' no disponible para perfil Sucursal-Género.")
                if col_estado_civil in valid_profile_dimensions:
                    resultados_dfs['perfil_pacientes_estado_civil_genero'] = df_analisis_final.groupby(
                        [col_estado_civil, col_sexo])['Id Paciente'].nunique().reset_index(name='Numero_Pacientes')
                else:
                    all_advertencias.append(
                        f"Advertencia: '{col_estado_civil}' no disponible para perfil EstadoCivil-Género.")
        else:
            all_advertencias.append(
                "ADVERTENCIA: df_analisis_final vacío, no se puede perfilar.")
        if not df_analisis_final.empty:
            resultados_dfs['df_analisis_final_completo'] = df_analisis_final
            print(
                f"DataFrame 'df_analisis_final_completo' generado con {len(df_analisis_final)} filas.")
    except Exception as e_insight:
        all_advertencias.append(f"ERROR CRÍTICO en insights: {e_insight}")
        print(f"ERROR CRÍTICO en insights: {e_insight}")
        import traceback
        traceback.print_exc()
    return resultados_dfs

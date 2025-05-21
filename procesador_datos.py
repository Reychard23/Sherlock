import pandas as pd
import numpy as np
import io
from typing import Dict, Any, List, Tuple, Set, Optional
# from datetime import date # No se usa directamente date, sino pd.Timestamp o pd.to_datetime

# === Función para cargar datos desde objetos UploadFile (o simulados) ===


def load_dataframes_from_uploads(
    data_files: List[Any],  # Lista de objetos tipo UploadFile (o simulados)
    index_file: Any,      # Objeto tipo UploadFile (o simulado) para indice.csv
) -> Tuple[Dict[str, pd.DataFrame], Dict[tuple[str, str], dict[str, str]], set[tuple[str, str, str]], List[str]]:
    processed_dfs: Dict[str, pd.DataFrame] = {}
    advertencias_carga: List[str] = []
    rename_map_details: Dict[tuple[str, str], dict[str, str]] = {}
    drop_columns_set: set[tuple[str, str, str]] = set()

    print(f"Intentando leer índice desde el archivo: {index_file.filename}")
    try:
        # Para leer de UploadFile (o nuestro simulador InMemoryUploadFile)
        # Necesitamos leer el contenido del .file (que es un stream)
        index_file.file.seek(0)  # Asegurar que el stream está al inicio
        content_indice_bytes = index_file.file.read()  # Lee bytes
        # Decodificar bytes a string si es necesario, o pd.read_csv puede manejar BytesIO directamente
        # Probar utf-8 primero
        indice_df = pd.read_csv(io.BytesIO(
            content_indice_bytes), encoding='utf-8')
        print("Índice cargado correctamente.")

        expected_cols_indice = ['Archivo Base', 'Hoja', 'Nombre Original Columna',
                                'Nuevo Nombre Columna', 'Eliminar?', 'Tipo de Archivo']
        if not all(col in indice_df.columns for col in expected_cols_indice):
            missing_cols = [
                col for col in expected_cols_indice if col not in indice_df.columns]
            msg = f"ADVERTENCIA CRÍTICA: Faltan columnas en el archivo índice: {missing_cols}."
            advertencias_carga.append(msg)
            print(msg)
            return {}, {}, set(), advertencias_carga

        for _, row in indice_df.iterrows():
            archivo_base = str(row['Archivo Base']).strip()
            hoja = str(row['Hoja']).strip() if pd.notna(
                row['Hoja']) else 'default'
            original_col = str(row['Nombre Original Columna']).strip()
            nuevo_col = str(row['Nuevo Nombre Columna']).strip()

            if pd.notna(row['Eliminar?']) and str(row['Eliminar?']).strip().upper() in ['SI', 'SÍ', 'YES', 'TRUE', '1']:
                drop_columns_set.add((archivo_base, hoja, original_col))

            if pd.notna(nuevo_col) and nuevo_col != original_col:
                if (archivo_base, hoja) not in rename_map_details:
                    rename_map_details[(archivo_base, hoja)] = {}
                rename_map_details[(archivo_base, hoja)
                                   ][original_col] = nuevo_col

        print(
            f"Rename map details construidos: {len(rename_map_details)} entradas.")
        print(
            f"Drop columns set construidos: {len(drop_columns_set)} entradas.")

    except Exception as e:
        msg = f"ERROR CRÍTICO al leer o procesar el archivo índice '{index_file.filename}': {e}"
        advertencias_carga.append(msg)
        print(msg)
        import traceback
        traceback.print_exc()
        return {}, {}, set(), advertencias_carga

    for uploaded_file_obj in data_files:
        original_filename = uploaded_file_obj.filename
        print(f"Procesando archivo de datos: {original_filename}")
        try:
            base_name_for_lookup = original_filename.split(
                '.')[0]  # Simple split para quitar extensión

            df_sheets: Dict[str, pd.DataFrame] = {}
            uploaded_file_obj.file.seek(0)  # Asegurar stream al inicio
            # Leer todo el contenido en bytes
            file_content_bytes = uploaded_file_obj.file.read()

            try:  # Intentar como Excel
                df_sheets = pd.read_excel(io.BytesIO(
                    file_content_bytes), sheet_name=None)
                print(
                    f"  Archivo '{original_filename}' leído como Excel. Hojas: {list(df_sheets.keys())}")
            except Exception as e_excel:  # Si no es Excel, intentar como CSV
                print(
                    f"  No se pudo leer '{original_filename}' como Excel ({e_excel}), intentando como CSV...")
                try:
                    # Para CSV, asumimos una sola "hoja" que llamaremos 'default'
                    df_temp = pd.read_csv(io.BytesIO(
                        file_content_bytes), sep=None, engine='python', encoding='utf-8-sig')
                    df_sheets['default'] = df_temp
                    print(
                        f"  Archivo '{original_filename}' leído como CSV (UTF-8).")
                except Exception as e_csv_utf8:
                    print(
                        f"  No se pudo leer '{original_filename}' como CSV UTF-8 ({e_csv_utf8}), intentando con Latin-1...")
                    try:
                        df_temp_latin1 = pd.read_csv(io.BytesIO(
                            file_content_bytes), sep=None, engine='python', encoding='latin1')
                        df_sheets['default'] = df_temp_latin1
                        print(
                            f"  Archivo '{original_filename}' leído como CSV (Latin-1).")
                    except Exception as e_csv_latin1:
                        msg = f"ADVERTENCIA: No se pudo leer '{original_filename}' ni como Excel ni como CSV (UTF-8/Latin-1): {e_csv_latin1}"
                        advertencias_carga.append(msg)
                        print(msg)
                        continue  # Saltar este archivo

            for sheet_name_excel, df_original in df_sheets.items():
                # Clave de búsqueda en el índice
                lookup_key_sheet_specific = (
                    base_name_for_lookup, sheet_name_excel)
                lookup_key_default_sheet = (base_name_for_lookup, 'default')
                current_rename_dict = rename_map_details.get(
                    lookup_key_sheet_specific, rename_map_details.get(lookup_key_default_sheet, {}))

                df_cleaned = df_original.copy()
                df_cleaned.rename(columns=current_rename_dict, inplace=True)

                # Lógica de eliminación (adaptada de tu local.py)
                # Identificar columnas a eliminar por su nombre ORIGINAL
                cols_originales_a_eliminar_para_este_archivo_hoja = {
                    col_name for (ab_idx, hoja_idx, col_name) in drop_columns_set
                    if ab_idx == base_name_for_lookup and (hoja_idx == sheet_name_excel or hoja_idx == 'default')
                }

                # Determinar qué columnas REALMENTE eliminar del df_cleaned
                # (considerando que algunas pudieron haber sido renombradas)
                columnas_finales_a_dropear_en_df_cleaned = []
                for original_col_to_drop in cols_originales_a_eliminar_para_este_archivo_hoja:
                    # Si la col original a dropear fue renombrada, su nuevo nombre es el que buscamos para dropear.
                    # Si no fue renombrada, su nombre original es el que buscamos.
                    nombre_actual_de_col_a_dropear = current_rename_dict.get(
                        original_col_to_drop, original_col_to_drop)
                    if nombre_actual_de_col_a_dropear in df_cleaned.columns:
                        columnas_finales_a_dropear_en_df_cleaned.append(
                            nombre_actual_de_col_a_dropear)

                if columnas_finales_a_dropear_en_df_cleaned:
                    df_cleaned.drop(columns=list(
                        set(columnas_finales_a_dropear_en_df_cleaned)), inplace=True, errors='ignore')
                    print(
                        f"  En '{original_filename}' (hoja: {sheet_name_excel}), columnas eliminadas: {len(columnas_finales_a_dropear_en_df_cleaned)}")

                # Nombre del DataFrame procesado (usando Tipo de Archivo del índice)
                tipo_archivo_candidates = indice_df[
                    (indice_df['Archivo Base'] == base_name_for_lookup) &
                    ((indice_df['Hoja'] == sheet_name_excel) | (indice_df['Hoja'].isna() & (
                        sheet_name_excel == 'default')) | (indice_df['Hoja'] == 'default'))
                ]['Tipo de Archivo'].unique()

                # Nombre por defecto
                df_key_name = f"{base_name_for_lookup}_{sheet_name_excel}"
                if len(tipo_archivo_candidates) > 0 and pd.notna(tipo_archivo_candidates[0]):
                    df_key_name = str(tipo_archivo_candidates[0]).strip()
                else:
                    advertencias_carga.append(
                        f"ADVERTENCIA: No se encontró 'Tipo de Archivo' para '{base_name_for_lookup}' (hoja: {sheet_name_excel}). Usando nombre: '{df_key_name}'.")

                if df_key_name in processed_dfs:
                    advertencias_carga.append(
                        f"ADVERTENCIA: Nombre de DataFrame duplicado '{df_key_name}' al procesar '{original_filename}'. Se sobrescribirá.")
                processed_dfs[df_key_name] = df_cleaned
                print(
                    f"  DataFrame '{df_key_name}' (desde hoja '{sheet_name_excel}') procesado y almacenado.")

        except Exception as e:
            msg = f"ERROR al procesar el archivo de datos '{original_filename}': {e}"
            advertencias_carga.append(msg)
            print(msg)
            import traceback
            traceback.print_exc()

    if not processed_dfs:
        advertencias_carga.append(
            "ADVERTENCIA CRÍTICA: No se procesó ningún DataFrame de datos.")
        print("ADVERTENCIA CRÍTICA: No se procesó ningún DataFrame de datos.")

    return processed_dfs, rename_map_details, drop_columns_set, advertencias_carga

# --- Funciones de apoyo ---


def get_df_by_type(
    processed_dfs: Dict[str, pd.DataFrame],
    tipo_archivo_buscado: str,
    advertencias_list: List[str]
) -> Optional[pd.DataFrame]:
    if tipo_archivo_buscado in processed_dfs:
        print(f"DataFrame '{tipo_archivo_buscado}' encontrado.")
        return processed_dfs[tipo_archivo_buscado].copy()
    else:
        msg = f"ADVERTENCIA: No se encontró el DataFrame con tipo '{tipo_archivo_buscado}'. Disponibles: {list(processed_dfs.keys())}"
        advertencias_list.append(msg)
        print(msg)
        return None

# === Lógica principal de análisis ===


def generar_insights_pacientes(
    processed_dfs: Dict[str, pd.DataFrame],
    all_advertencias: List[str]
) -> Dict[str, pd.DataFrame]:
    resultados_dfs: Dict[str, pd.DataFrame] = {}

    df_tratamientos = get_df_by_type(
        processed_dfs, "Tratamientos", all_advertencias)
    df_pacientes = get_df_by_type(processed_dfs, "Pacientes", all_advertencias)
    df_citas = get_df_by_type(processed_dfs, "Citas",
                              all_advertencias)  # Puede ser None

    if df_pacientes is None:  # Pacientes es el más fundamental
        all_advertencias.append(
            "ERROR CRÍTICO: DataFrame de Pacientes no disponible. No se puede continuar el análisis.")
        print("ERROR CRÍTICO: DataFrame de Pacientes no disponible.")
        return resultados_dfs

    print("Iniciando enriquecimiento y unión de DataFrames para insights...")

    try:
        # Base del análisis: df_pacientes
        df_analisis_final = df_pacientes.copy()

        # Merge con Tratamientos (si existe)
        if df_tratamientos is not None:
            if 'Id Paciente' in df_pacientes.columns and 'Id Paciente' in df_tratamientos.columns:
                # Asegurar que Id Paciente sea string para el merge
                df_analisis_final['Id Paciente'] = df_analisis_final['Id Paciente'].astype(
                    str)
                df_tratamientos['Id Paciente'] = df_tratamientos['Id Paciente'].astype(
                    str)
                df_analisis_final = pd.merge(
                    df_analisis_final, df_tratamientos, on='Id Paciente', how='left', suffixes=('_paciente', '_tratamiento'))
                print(
                    f"Merge Pacientes y Tratamientos realizado. Filas: {len(df_analisis_final)}")
            else:
                all_advertencias.append(
                    "ADVERTENCIA: 'Id Paciente' no encontrado en Pacientes o Tratamientos para el merge.")
        else:
            all_advertencias.append(
                "INFO: DataFrame de Tratamientos no disponible, se continuará sin él.")

        # (Opcional) Merge con Citas si es necesario para los perfiles.
        # Por ahora, los perfiles se basan en Pacientes y Tratamientos.

        # --- Conversiones de Tipo y Cálculo de Edad (con corrección para Pylance) ---
        # Nombres de columnas post-merge
        cols_fecha_tratamiento = [
            'Fecha Creación_tratamiento', 'Fecha de Inicio', 'Fecha Terminado']
        for col in cols_fecha_tratamiento:
            if col in df_analisis_final.columns:
                try:
                    df_analisis_final[col] = pd.to_datetime(
                        df_analisis_final[col], errors='coerce')
                except Exception as e_conv:
                    all_advertencias.append(
                        f"Advertencia: No se pudo convertir la columna '{col}' a datetime: {e_conv}")

        if 'Fecha de nacimiento' in df_analisis_final.columns:
            try:
                # 1. Convertir 'Fecha de nacimiento' a datetime, NaT para errores
                df_analisis_final['Fecha de nacimiento'] = pd.to_datetime(
                    df_analisis_final['Fecha de nacimiento'], errors='coerce')

                # Solo proceder si la columna no está completamente vacía de fechas válidas
                if not df_analisis_final['Fecha de nacimiento'].isnull().all():
                    # 2. Obtener la fecha y hora actual, consciente de la zona horaria (México)
                    current_time_mex = pd.Timestamp(
                        'now', tz='America/Mexico_City')

                    # 3. Asegurar que 'Fecha de nacimiento' sea consciente de la zona horaria.
                    if df_analisis_final['Fecha de nacimiento'].dt.tz is None:
                        # Si es naive, localizarla (asumiendo que las fechas son de México)
                        df_analisis_final['Fecha de nacimiento'] = df_analisis_final['Fecha de nacimiento'].dt.tz_localize(
                            'America/Mexico_City', ambiguous='infer', nonexistent='NaT')
                    else:
                        # Si ya tiene tz, convertirla a la zona de México
                        df_analisis_final['Fecha de nacimiento'] = df_analisis_final['Fecha de nacimiento'].dt.tz_convert(
                            'America/Mexico_City')

                    # 4. Calcular la diferencia. Ambas son ahora tz-aware en la misma zona.
                    # Esto resultará en una Series de Timedelta.
                    time_difference = current_time_mex - \
                        df_analisis_final['Fecha de nacimiento']

                    # 5. Convertir la diferencia a años (float)
                    df_analisis_final['Edad_float'] = time_difference / \
                        np.timedelta64(1, 'Y')

                    # 6. Convertir a entero nullable (Int64). NaNs/NaTs se vuelven pd.NA.
                    df_analisis_final['Edad'] = df_analisis_final['Edad_float'].astype(
                        'Int64')
                    print("Columna 'Edad' calculada.")
                else:
                    all_advertencias.append(
                        "Advertencia: La columna 'Fecha de nacimiento' no contiene fechas válidas para calcular Edad.")
                    # Asignar pd.NA si no se puede calcular
                    df_analisis_final['Edad'] = pd.NA
            except Exception as e_edad:
                all_advertencias.append(
                    f"Advertencia: Error al calcular la Edad: {e_edad}")
                # Asignar pd.NA en caso de error
                df_analisis_final['Edad'] = pd.NA
        else:
            all_advertencias.append(
                "Advertencia: 'Fecha de nacimiento' no encontrada para calcular Edad.")
            # Crear columna con pd.NA si no existe la base
            df_analisis_final['Edad'] = pd.NA

        # --- Lógica de Perfilado ---
        # Columnas que podrían venir de df_pacientes y necesitar sufijo si hay merge
        col_sexo = 'Sexo_paciente' if 'Sexo_paciente' in df_analisis_final.columns else 'Sexo'
        # Ajusta según tus nombres reales
        col_sucursal = 'Sucursal_paciente' if 'Sucursal_paciente' in df_analisis_final.columns else 'Sucursal'
        col_estado_civil = 'Estado Civil_paciente' if 'Estado Civil_paciente' in df_analisis_final.columns else 'Estado Civil'

        profile_dimensions_pacientes = [
            'Edad', col_sexo, col_sucursal, col_estado_civil]

        if not df_analisis_final.empty:
            valid_profile_dimensions = [
                dim for dim in profile_dimensions_pacientes if dim in df_analisis_final.columns]

            if col_sexo not in valid_profile_dimensions:
                all_advertencias.append(
                    f"ADVERTENCIA: Columna '{col_sexo}' no disponible. Perfiles por género no se pueden generar.")
            else:
                if 'Edad' in valid_profile_dimensions:
                    perfil_pacientes_edad_genero = df_analisis_final.groupby(
                        ['Edad', col_sexo])['Id Paciente'].nunique().reset_index(name='Numero_Pacientes')
                    resultados_dfs['perfil_pacientes_edad_genero'] = perfil_pacientes_edad_genero
                else:
                    all_advertencias.append(
                        "Advertencia: Columna 'Edad' no disponible para perfil Edad-Género.")

                if col_sucursal in valid_profile_dimensions:
                    perfil_pacientes_sucursal_genero = df_analisis_final.groupby(
                        [col_sucursal, col_sexo])['Id Paciente'].nunique().reset_index(name='Numero_Pacientes')
                    resultados_dfs['perfil_pacientes_sucursal_genero'] = perfil_pacientes_sucursal_genero
                else:
                    all_advertencias.append(
                        f"Advertencia: Columna '{col_sucursal}' no disponible para perfil Sucursal-Género.")

                if col_estado_civil in valid_profile_dimensions:
                    perfil_pacientes_estado_civil_genero = df_analisis_final.groupby(
                        [col_estado_civil, col_sexo])['Id Paciente'].nunique().reset_index(name='Numero_Pacientes')
                    resultados_dfs['perfil_pacientes_estado_civil_genero'] = perfil_pacientes_estado_civil_genero
                else:
                    all_advertencias.append(
                        f"Advertencia: Columna '{col_estado_civil}' no disponible para perfil EstadoCivil-Género.")
        else:
            all_advertencias.append(
                "ADVERTENCIA: df_analisis_final está vacío. No se puede realizar perfilado.")

        if not df_analisis_final.empty:
            # DataFrame principal con todo
            resultados_dfs['df_analisis_final_completo'] = df_analisis_final
            print(
                f"DataFrame 'df_analisis_final_completo' generado con {len(df_analisis_final)} filas.")

    except Exception as e_insight:
        all_advertencias.append(
            f"ERROR CRÍTICO durante la generación de insights: {e_insight}")
        print(f"ERROR CRÍTICO durante la generación de insights: {e_insight}")
        import traceback
        traceback.print_exc()

    return resultados_dfs

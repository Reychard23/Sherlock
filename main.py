from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import JSONResponse
from typing import List, Dict, Any
from typing import Dict, Any
import pandas as pd
import io
from datetime import date
import os
import requests
import json

app = FastAPI()
# Almacena los archivos raw (como los sube Make)
_saved_files: dict[str, bytes] = {}
# Almacena los DataFrames de Pandas procesados y limpios
_processed_dfs: dict[str, pd.DataFrame] = {}


def save_insight_to_airtable(question_key: str, answer_value: Any, units: str, dimensions: Dict[str, Any], advertencias: list) -> bool:
    """
    Guarda un insight en la tabla de Airtable.

    Args:
        question_key: La pregunta clave estandarizada del insight (string).
        answer_value: El valor calculado del insight (puede ser int, float, string, etc.).
        units: Las unidades del valor (ej: "pacientes", "%", "MXN").
        dimensions: Un diccionario con el contexto del insight (ej: {"Mes": "2024-03", "Sucursal": "Medellín"}).
        advertencias: Lista para añadir mensajes de advertencia o error.

    Returns:
        True si el guardado fue exitoso, False en caso contrario.
    """
    # ... (código para leer variables de entorno y construir URL - ya revisamos que esto está bien en tu código) ...
    airtable_api_key = os.environ.get("AIRTABLE_API_KEY")
    airtable_base_id = os.environ.get("AIRTABLE_BASE_ID")
    airtable_table_id = os.environ.get("AIRTABLE_TABLE_ID")
    airtable_url = f"https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_id}"

    if not airtable_api_key or not airtable_base_id or not airtable_table_id:
        print("Error de configuración de Airtable: Faltan una o más variables de entorno ('AIRTABLE_API_KEY', 'AIRTABLE_BASE_ID', 'AIRTABLE_TABLE_ID').")
        # Añadimos a la lista 'advertencias' que ahora es un argumento
        advertencias.append(
            "Error de configuración de Airtable: Faltan variables de entorno.")
        return False

    # Convertir el diccionario de dimensiones a string JSON
    try:
        dimensions_json_string = json.dumps(dimensions)
    except Exception as e:
        print(f"Error al serializar dimensiones a JSON string: {e}")
        # === CORRECCIÓN: 'advertencias' es accesible aquí porque se pasa como argumento ===
        # Añadimos el error de serialización a la lista 'advertencias'
        advertencias.append(
            f"Error al serializar dimensiones a JSON string: {e}. El insight se guardará sin dimensiones.")
        dimensions_json_string = "{}"  # Guardar un objeto JSON vacío si falla

    # ... (resto de la función para preparar data, headers, y hacer la solicitud requests.post - sin cambios) ...
    data = {  # Línea 60 en tu código actual
        "records": [
            {
                "fields": {
                    "Pregunta_Clave": question_key,
                    "Respuesta_Calculada": str(answer_value),
                    "Unidades": units,
                    "Dimensiones": dimensions_json_string,
                    "Fecha_Calculo": date.today().isoformat(),
                    "Origen_Datos": "Pandas Precalculado"
                }
            }
        ]
    }

    headers = {  # Línea 76 en tu código actual
        "Authorization": f"Bearer {airtable_api_key}",
        "Content-Type": "application/json"
    }

    try:  # Línea 81 en tu código actual
        response = requests.post(airtable_url, headers=headers, json=data)
        response.raise_for_status()
        print(
            f"Insight guardado exitosamente en Airtable para '{question_key}'.")
        return True

    except requests.exceptions.RequestException as e:  # Línea 89 en tu código actual
        print(
            f"Error HTTP al guardar insight en Airtable para '{question_key}': {e}")
        # === Añadimos el error de solicitud HTTP a advertencias ===
        advertencias.append(f"Error al guardar insight en Airtable: {e}")
        return False
    except Exception as e:  # Línea 96 en tu código actual
        print(
            f"Ocurrió un error inesperado en la función de guardar en Airtable: {e}")
        # === Añadimos el error inesperado a advertencias ===
        advertencias.append(
            f"Error inesperado al guardar insight en Airtable: {e}")
        return False


def calcular_pacientes_nuevos_atendidos(dataframes: Dict[str, pd.DataFrame]) -> int:
    """
    Calcula el número total de pacientes nuevos atendidos.
    """
    try:
        # ... (código de la función) ...
        # 1. Obtener los DataFrames necesarios
        df_citas_pacientes = dataframes.get("Citas_Pacientes")
        df_citas_motivo = dataframes.get("Citas_Motivo")

        if df_citas_pacientes is None or df_citas_motivo is None:
            print(
                "Error: No se encontraron los DataFrames de Citas_Pacientes o Citas_Motivo en _processed_dfs.")
            return 0

        # Asegurarse de que las columnas necesarias existan y tengan el tipo de dato correcto si es posible
        # Es buena práctica validar columnas, pero por ahora asumiremos que el índice las dejó bien
        # Si tienes problemas, podríamos añadir validaciones aquí.

        # 2. Filtrar citas duplicadas en df_citas_pacientes
        # Convertir la columna 'Cita duplicada' a tipo numérico o booleano si no lo está
        # Manejar posibles errores de conversión si los datos crudos no son limpios
        df_citas_pacientes['Cita duplicada'] = pd.to_numeric(
            df_citas_pacientes['Cita duplicada'], errors='coerce').fillna(0)
        # Usar .copy() para evitar SettingWithCopyWarning
        df_citas_filtradas = df_citas_pacientes[df_citas_pacientes['Cita duplicada'] != 1].copy(
        )

        # 3. Unir con df_citas_motivo para obtener la Fecha Cita
        # Nos aseguramos de que las columnas clave tengan el mismo tipo de dato para el merge
        df_citas_filtradas['ID_Cita'] = df_citas_filtradas['ID_Cita'].astype(
            str)
        df_citas_motivo['ID_Cita'] = df_citas_motivo['ID_Cita'].astype(str)

        # Realizar el merge (inner join para solo citas que existen en ambas tablas)
        # Seleccionar solo las columnas de Citas_Motivo que necesitamos (ID_Cita y Fecha Cita)
        df_citas_motivo_reduced = df_citas_motivo[[
            'ID_Cita', 'Fecha Cita']].copy()
        df_merged = pd.merge(
            df_citas_filtradas,
            df_citas_motivo_reduced,
            on='ID_Cita',
            how='inner'
        )

        # Convertir la columna 'Fecha Cita' a tipo datetime
        df_merged['Fecha Cita'] = pd.to_datetime(
            df_merged['Fecha Cita'], errors='coerce')

        # Asegurarnos de que Consecutivo_cita sea numérico y Cita_asistida sea booleano/numérico
        df_merged['Consecutivo_cita'] = pd.to_numeric(
            # Usar -1 o algún valor para nulos
            df_merged['Consecutivo_cita'], errors='coerce').fillna(-1)
        # Asumiendo que Cita_asistida es 0 o 1 o True/False
        df_merged['Cita_asistida'] = pd.to_numeric(
            df_merged['Cita_asistida'], errors='coerce').fillna(0).astype(int)

        # 4. Filtrar para obtener solo "Pacientes nuevos atendidos"
        # Aplicar la primera condición de tu lógica
        df_pacientes_nuevos_atendidos = df_merged[
            (df_merged['Consecutivo_cita'] == 1) &
            (df_merged['Cita_asistida'] == 1)
        ].copy()  # Usar .copy()

        # 5. Contar pacientes únicos
        # Contamos el número de IDs únicos en la columna 'ID_Paciente'
        numero_pacientes_nuevos_atendidos = df_pacientes_nuevos_atendidos['ID_Paciente'].nunique(
        )

        return numero_pacientes_nuevos_atendidos

    except Exception as e:
        print(f"Error calculando pacientes nuevos atendidos: {str(e)}")
        # Dependiendo de cómo quieras manejar errores, podrías lanzar la excepción o registrarla
        return 0  # Retornar 0 en caso de error


@app.get("/")
async def read_root():
    return {"message": "Hola, reychard"}


@app.post("/upload-excel/")
async def upload_excel(files: List[UploadFile] = File(...)):
    resultados = []
    # Limpiar DataFrames procesados anteriores al subir nuevos archivos
    _processed_dfs.clear()
    for file in files:
        try:
            data = await file.read()
            _saved_files[file.filename] = data  # Guardamos el archivo raw
            # Ya no necesitamos leerlo aquí solo para mostrar info,
            # se leerá y procesará en el siguiente endpoint.
            # df = pd.read_excel(io.BytesIO(data)) # Línea anterior, no necesaria aquí ahora

            resultados.append({
                "filename": file.filename,
                # Ya no mostramos columnas ni conteo aquí, eso se hace después del procesamiento
                # "columns": df.columns.tolist(),
                # "row_count": len(df)
                "status": "Archivo recibido y guardado para procesamiento"
            })
        except Exception as e:
            # Capturamos el error y lo devolvemos para ese archivo
            resultados.append({
                "filename": file.filename,
                "error": str(e)
            })
    return {"resultados": resultados}


@app.post("/process-excel/")
async def process_files_endpoint():
    """
    Endpoint para procesar los archivos Excel subidos,
    aplicando la lógica del índice para limpieza y renombrado.
    Almacena los DataFrames procesados en memoria.
    Maneja advertencias si faltan archivos esperados o sobran inesperados.
    """
    # Limpiar DataFrames procesados anteriores antes de empezar
    _processed_dfs.clear()
    resultados = []
    advertencias = []  # Lista para acumular advertencias

    # 1) Leer el índice desde memoria
    raw_index = _saved_files.get("indice.xlsx")
    if raw_index is None:
        # Este es un error fatal, el índice es indispensable
        return JSONResponse({"error": "No se subió indice.xlsx. El índice es necesario para procesar."}, status_code=400)
    try:
        indice_df = pd.read_excel(io.BytesIO(raw_index))
    except Exception as e:
        return JSONResponse({"error": f"Error al leer indice.xlsx: {str(e)}. Asegúrese de que sea un archivo Excel válido."}, status_code=400)

    # 2) Construir rename_dict y drop_set
    rename_dict: dict[tuple[str, str], dict[str, str]] = {}
    drop_set: set[tuple[str, str, str]] = set()

    required_cols = ["Archivo", "Sheet",
                     "Columna", "Nombre unificado", "Acción"]
    if not all(col in indice_df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in indice_df.columns]
        return JSONResponse({"error": f"Columnas requeridas faltantes en indice.xlsx: {missing}. Asegúrese de que el archivo índice tenga las columnas correctas."}, status_code=400)

    # Procesar filas del índice para construir diccionarios de mapeo y eliminación
    for idx, row in indice_df.iterrows():
        try:
            # Añadir verificación básica de que los valores no son NaN antes de strip
            if pd.isna(row["Archivo"]) or pd.isna(row["Sheet"]) or pd.isna(row["Columna"]) or pd.isna(row["Nombre unificado"]) or pd.isna(row["Acción"]):
                # +2 para contar encabezado y empezar en 1
                advertencias.append(
                    f"Fila {idx+2} en indice.xlsx contiene valores faltantes en columnas clave y será ignorada.")
                continue

            archivo = str(row["Archivo"]).strip()
            hoja = str(row["Sheet"]).strip()
            orig = str(row["Columna"]).strip()
            nuevo = str(row["Nombre unificado"]).strip()
            accion = str(row["Acción"]).strip().lower()

            # Validar que los campos importantes no estén vacíos después del strip
            if not archivo or not hoja or not orig or not accion:
                advertencias.append(
                    f"Fila {idx+2} en indice.xlsx tiene campos vacíos después de limpiar y será ignorada.")
                continue

            key = (archivo, hoja)
            if accion == "drop":
                drop_set.add((archivo, hoja, orig))
            elif accion == "keep":
                if key not in rename_dict:
                    rename_dict[key] = {}
                rename_dict[key][orig] = nuevo
            else:
                # Advertencia si la acción no es 'keep' ni 'drop'
                advertencias.append(
                    f"Fila {idx+2} en indice.xlsx tiene acción '{accion}' no reconocida para columna '{orig}' en archivo '{archivo}' hoja '{hoja}'. La fila será ignorada.")

        except Exception as e:
            # Capturar errores al procesar filas individuales del índice
            advertencias.append(
                f"Error procesando fila {idx+2} en indice.xlsx: {str(e)}. Fila: {row.to_dict()}.")

    # 3) Validar contra la lista esperada de archivos Y los archivos subidos
    # Derivar la lista de archivos esperados *del índice* donde hay al menos una acción 'keep'
    # Esto hace que la lista de 'expected' sea dinámica basada en el índice proporcionado
    expected = {archivo for (archivo, _) in rename_dict.keys()}

    # También consideramos los archivos mencionados en el índice con acciones 'drop'
    expected.update({archivo for (archivo, _, _) in drop_set})

    uploaded = set(_saved_files.keys()) - {"indice.xlsx"}

    faltan = expected - uploaded
    sobran = uploaded - expected
    # Archivos esperados según el índice pero que no se subieron
    # Estos no se pueden procesar, se añadirán a advertencias
    archivos_faltantes_no_procesables = list(faltan)

    # Archivos subidos que no estaban en el índice (ni para keep ni para drop)
    # Estos tampoco se procesarán
    archivos_sobrantes_no_procesables = list(sobran)

    # === MODIFICACIÓN: No retornar error 400 aquí, añadir advertencias ===
    if faltan:
        # Considerar si la falta de ciertos archivos clave (como Citas_Pacientes)
        # debería ser un error fatal. Por ahora, solo es advertencia.
        advertencias.append(
            f"ADVERTENCIA: No se subieron estos archivos esperados (listados en el índice): {sorted(faltan)}. No podrán ser procesados.")
    if sobran:
        advertencias.append(
            f"ADVERTENCIA: Se subieron archivos no listados en el índice: {sorted(sobran)}. No serán procesados.")

    # Validar que todos los archivos subidos Y esperados (que sí se subieron)
    # tengan al menos una entrada 'keep' en el índice para poder ser procesados.
    # Los archivos subidos que estaban en 'expected' pero no tienen 'keep'
    # tampoco pueden ser procesados, pero no es un error fatal si no los quieres.
    # Mantenemos esta validación pero la convertimos en advertencia si la lista 'expected'
    # se deriva del índice y cubre todos los archivos mencionados.
    # Si usamos una lista 'expected' fija (como antes), esta validación tiene más sentido.
    # Asumimos que la lista 'expected' se deriva ahora del índice para mayor flexibilidad.

    # 4) Procesar *solamente* los (archivo, hoja) definidos en rename_dict *que sí fueron subidos*
    # Iterar sobre las combinaciones archivo/hoja definidas en el índice con acción 'keep'
    archivos_procesados_con_exito = set()  # Para llevar un control

    for (archivo, hoja), mapeo in rename_dict.items():
        # Solo intentar procesar si el archivo esperado fue realmente subido
        if archivo not in uploaded:
            # Ya se añadió una advertencia general si faltan archivos esperados
            continue

        raw = _saved_files.get(archivo)
        # Esta validación adicional no debería ser necesaria si el filtro anterior funciona, pero es segura
        if raw is None:
            advertencias.append(
                f"ADVERTENCIA: Archivo '{archivo}' listado en el índice para procesar (hoja '{hoja}') fue inesperadamente no encontrado en memoria. No se procesará esta combinación.")
            continue

        try:
            # Leer solo la hoja especificada
            df = pd.read_excel(io.BytesIO(raw), sheet_name=hoja)

            # Drop de columnas (solo si existen en el DF)
            to_drop = [col for (a, h, col) in drop_set if a ==
                       archivo and h == hoja]
            cols_to_actually_drop = [
                col for col in to_drop if col in df.columns]
            if cols_to_actually_drop:
                df = df.drop(columns=cols_to_actually_drop)
            # else:
                # Opcional: advertencia si hay columnas en el índice para dropear pero no existen en el archivo
                # for col in to_drop:
                #      if col not in df.columns:
                #           advertencias.append(f"Columna '{col}' listada para eliminar en indice.xlsx para '{archivo}' hoja '{hoja}' no encontrada en el archivo.")

            # Rename de columnas keep (solo si existen en el DF)
            actual_mapeo = {orig_col: new_col for orig_col,
                            new_col in mapeo.items() if orig_col in df.columns}
            if actual_mapeo:
                df.rename(columns=actual_mapeo, inplace=True)
            # else:
                # Opcional: advertencia si hay columnas en el índice para keep/rename pero no existen en el archivo
                # for col in mapeo.keys():
                #      if col not in df.columns:
                #           advertencias.append(f"Columna '{col}' listada para mantener/renombrar en indice.xlsx para '{archivo}' hoja '{hoja}' no encontrada en el archivo.")

            # ===>>> GUARDAR EL DATAFRAME PROCESADO <<<===
            # Usamos el nombre del archivo (sin la extensión) como clave
            base_filename = archivo.replace(".xlsx", "").replace(".xslx", "")
            # Si un archivo tiene múltiples hojas relevantes según el índice,
            # necesitarás una clave única que incluya el nombre de la hoja,
            # por ejemplo: f"{base_filename}_{hoja}"
            # Si cada archivo tiene solo UNA hoja relevante:
            _processed_dfs[base_filename] = df

            resultados.append({
                "archivo":   archivo,
                "hoja":      hoja,
                "status":   "Procesado exitosamente",
                "columns":   df.columns.tolist(),
                "row_count": len(df)
            })
            archivos_procesados_con_exito.add(archivo)

        except Exception as e:
            # Capturar errores durante el procesamiento de un archivo/hoja específico
            advertencias.append({
                "archivo": archivo,
                "hoja":    hoja,
                "error":   f"Error durante el procesamiento con Pandas: {str(e)}"
            })

    # 5) Reportar archivos esperados que no pudieron ser procesados (si aplica)
    # Estos son los archivos listados en 'expected' que no están en 'uploaded'
    for archivo_faltante in archivos_faltantes_no_procesables:
        # Ya se agregó una advertencia general arriba
        pass  # No hacemos nada específico aquí, ya se reportó

    # Reportar archivos subidos que no fueron listados en el índice (si aplica)
    for archivo_sobrante in archivos_sobrantes_no_procesables:
        # Ya se agregó una advertencia general arriba
        pass  # No hacemos nada específico aquí, ya se reportó

    # 6) Devolver resultados y advertencias
    response_content: Dict[str, Any] = {"resultados_procesamiento": resultados}
    if advertencias:
        response_content["advertencias_o_errores"] = advertencias

    # Considerar un código de estado 200 OK incluso si hay advertencias,
    # ya que el proceso no se detuvo por completo.
    # Si la falta de archivos críticos debe dar error, tendrías que
    # añadir validación específica para esos nombres de archivo aquí.
    return JSONResponse(response_content, status_code=200)


@app.post("/calculate-insights/")
async def calculate_insights_endpoint():
    """
    Endpoint para calcular los insights predefinidos y guardarlos en Airtable.
    Requiere que los archivos hayan sido subidos y procesados previamente.
    """
    # === CORRECCIÓN: Inicializar la lista advertencias aquí ===
    advertencias = []

    # === Asegurar que json está importado al inicio del archivo main.py ===
    # (Verifica que 'import json' esté en las primeras líneas)

    if not _processed_dfs:
        # Si no hay datos para procesar, no intentamos calcular ni guardar
        return JSONResponse({"error": "No hay datos procesados disponibles. Por favor, suba y procese los archivos primero."}, status_code=400)

    # === Calcular el primer insight ===
    pacientes_nuevos_atendidos_count = calcular_pacientes_nuevos_atendidos(
        _processed_dfs)

    question_key = "Cantidad total de pacientes nuevos atendidos"
    answer_value = pacientes_nuevos_atendidos_count
    units = "pacientes"
    dimensions = {}

    # === Guardar el insight en Airtable ===
    # === CORRECCIÓN: Llamar a la función de guardado pasando la lista 'advertencias' ===
    guardado_exitoso = save_insight_to_airtable(
        question_key, answer_value, units, dimensions, advertencias)  # <--- Pasamos la lista

    # === Puedes añadir aquí más cálculos de insights y guardarlos ===
    # Por ejemplo, si quisieras calcular por sucursal:
    # insights_por_sucursal = calcular_pacientes_nuevos_atendidos_por_sucursal(_processed_dfs)
    # for suc, count in insights_por_sucursal.items():
    #    save_insight_to_airtable("Cantidad de pacientes nuevos atendidos por sucursal", count, "pacientes", {"Sucursal": suc}, advertencias)

    # === Devolver el resultado y las advertencias ===
    response_content = {
        "status": "Insights calculados",
        "insight_pacientes_nuevos_atendidos": answer_value,
        "guardado_en_airtable_exitoso": guardado_exitoso
    }

    # === CORRECCIÓN: Incluir la lista de advertencias en la respuesta final ===
    if advertencias:
        response_content["advertencias_o_errores"] = advertencias

    return JSONResponse(response_content, status_code=200)

    # Puedes añadir lógica aquí para ver si se guardó correctamente y reportarlo en la respuesta

    # === Puedes añadir aquí más cálculos de insights y guardarlos ===

    # Devolver el resultado y si se guardó o no (opcional)
    response_content = {
        "status": "Insights calculados",
        "insight_pacientes_nuevos_atendidos": answer_value,
        "guardado_en_airtable_exitoso": guardado_exitoso
    }

    # Si hubo advertencias durante el cálculo o guardado (si modificas las funciones para retornarlas)
    # response_content["advertencias"] = advertencias # Esto requiere modificar las funciones para acumular y retornar advertencias

    return JSONResponse(response_content, status_code=200)


@app.post("/slack")
async def slack_command(request: Request):
    # ... (código de este endpoint para Slack) ...
    form_data = await request.form()
    print("Payload recibido:", form_data)
    user_question = form_data.get("text")

    # Lógica del árbol de decisiones empezará aquí
    # 1. Buscar en Airtable...
    # 2. Si no está, evaluar si se puede calcular con _processed_dfs
    #    if _processed_dfs:
    #        # Intentar calcular dinámicamente o solicitar permiso
    #        pass # Lógica futura

    return JSONResponse({"text": f"Hola, yo soy Sherlock. Recibí tu pregunta: '{user_question}'. Aún estoy aprendiendo a analizar los datos para responderte."}, status_code=200)

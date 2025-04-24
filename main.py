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
    Endpoint para calcular los insights predefinidos y devolverlos.
    La lógica de guardar en Airtable se maneja en Make.
    Requiere que los archivos hayan sido subidos y procesados previamente.
    """

    if not _processed_dfs:
        return JSONResponse({"error": "No hay datos procesados disponibles. Por favor, suba y procese los archivos primero."}, status_code=400)

    # === Calcular el primer insight ===
    pacientes_nuevos_atendidos_count = calcular_pacientes_nuevos_atendidos(
        _processed_dfs)

    # Definir los datos del insight para devolver
    question_key = "Cantidad total de pacientes nuevos atendidos"
    answer_value = pacientes_nuevos_atendidos_count
    units = "pacientes"
    dimensions = {}  # Podríamos devolver esto si es útil para Make

    # === Puedes añadir aquí más cálculos de insights y devolverlos ===
    # insights_por_sucursal = calcular_pacientes_nuevos_atendidos_por_sucursal(_processed_dfs)
    # etc.

    # === Devolver el resultado ===
    # Simplificamos la respuesta JSON para devolver los datos calculados
    return JSONResponse({
        "status": "Insights calculados exitosamente",
        "insight_pacientes_nuevos_atendidos": {
            "question_key": question_key,
            "answer_value": answer_value,
            "units": units,
            # Devolvemos las dimensiones (vacías por ahora) por si Make las necesita
            "dimensions": dimensions
        }
        # Añadir aquí otros insights calculados si los hubiera
    }, status_code=200)


@app.post("/slack")
async def slack_command(request: Request):
    """
    Endpoint activado por el comando /sherlock en Slack.
    Recibe la pregunta del usuario y la envía a Make para procesar el árbol de decisiones.
    """
    form_data = await request.form()
    print("Payload recibido:", form_data)

    # Slack envía la pregunta en el campo 'text' del form_data
    user_question = form_data.get("text")
    if not user_question:
        return JSONResponse({"text": "No recibí tu pregunta. Por favor, intenta de nuevo."}, status_code=200)

    # --- Llamar a un Webhook en Make para procesar la pregunta ---
    # Necesitarás la URL del Webhook de Make para el escenario del árbol de decisiones.
    # Configuraremos esto en el siguiente paso en Make.
    # Leer la URL del webhook de Make desde variables de entorno
    make_webhook_url = os.environ.get("MAKE_SHERLOCK_WEBHOOK_URL")

    if not make_webhook_url:
        print(
            "Error: La variable de entorno MAKE_SHERLOCK_WEBHOOK_URL no está configurada.")
        # Error interno si falta la configuración
        return JSONResponse({"text": "Sherlock no está configurado correctamente para procesar tu pregunta. Contacta a soporte."}, status_code=500)

    try:
        # Enviar la pregunta del usuario a Make
        # Puedes enviar más datos si Make los necesita, como el user_id, channel_id, etc.
        payload_to_make = {
            "user_question": user_question,
            "user_id": form_data.get("user_id"),  # ID del usuario en Slack
            "channel_id": form_data.get("channel_id")  # ID del canal en Slack
            # Otros campos relevantes de form_data de Slack
        }

        # Realizar la solicitud POST al webhook de Make
        response_from_make = requests.post(
            make_webhook_url, json=payload_to_make)

        # Verificar si la llamada al webhook fue exitosa (códigos 2xx)
        response_from_make.raise_for_status()

        # Opcional: Leer la respuesta de Make si el escenario devuelve algo
        # make_response_data = response_from_make.json() # Si Make devuelve un JSON
        # print("Respuesta de Make:", make_response_data)

        # Sherlock responderá después a través de Make.
        # Por ahora, solo confirmamos que la pregunta fue recibida.
        return JSONResponse({"text": f"Pregunta recibida: '{user_question}'. Sherlock está procesando la respuesta..."}, status_code=200)

    except requests.exceptions.RequestException as e:
        print(f"Error al llamar al webhook de Make: {e}")
        return JSONResponse({"text": "Ocurrió un error al enviar tu pregunta a Sherlock para procesamiento."}, status_code=500)
    except Exception as e:
        print(f"Ocurrió un error inesperado en el endpoint /slack: {e}")
        return JSONResponse({"text": "Ocurrió un error interno al procesar tu pregunta."}, status_code=500)

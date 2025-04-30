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

# === Nuevas importaciones para la base de datos ===
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
# ===============================================

app = FastAPI()

# Almacena los archivos raw (como los sube Make) - Esto lo mantendremos por ahora para el procesamiento
_saved_files: dict[str, bytes] = {}

# === Esto ya NO lo usaremos para almacenar DataFrames procesados ===
# _processed_dfs: dict[str, pd.DataFrame] = {}
# ==================================================================

# === Configuración de la conexión a la base de datos ===
# Leer la URL de conexión desde las variables de entorno (.env local y Render)
DATABASE_URL = os.environ.get("DATABASE_URL")

# Validar si la URL de la base de datos está configurada
if not DATABASE_URL:
    # Podríamos querer lanzar un error o simplemente imprimir un mensaje.
    # Para el despliegue, es mejor lanzar un error que impida que el servicio inicie sin DB.
    # Para desarrollo local, a veces es útil permitir que inicie pero falle si intenta usar la DB.
    # Por ahora, imprimiremos y asumiremos que para operaciones que requieren DB, fallará.
    print("ADVERTENCIA: La variable de entorno DATABASE_URL no está configurada. Las operaciones de base de datos fallarán.")
    # Si quieres que el servicio NO inicie sin la DB, descomenta la siguiente línea:
    # raise Exception("DATABASE_URL no configurada")

# Crear el engine de SQLAlchemy
# El pool_recycle es útil en entornos como Render para manejar conexiones inactivas
# Puedes ajustar pool_size y max_overflow si tienes muchos usuarios concurrentes
try:
    engine = create_engine(DATABASE_URL, pool_recycle=3600)
    print("Conexión a la base de datos configurada.")
except Exception as e:
    print(f"Error al crear el engine de la base de datos: {e}")
    engine = None  # Asegúrate de que engine sea None si falla la creación


# ======================================================


# --- Función para calcular insights (la modificaremos después) ---
def calcular_pacientes_nuevos_atendidos() -> int:  # Ya no necesita dataframes como parámetro
    """
    Calcula el número total de pacientes nuevos atendidos
    leyendo los datos desde la base de datos.
    """
    if engine is None:
        print("Error: No se puede calcular insights, la conexión a la base de datos no está configurada.")
        return 0

    try:
        # === MODIFICACIÓN: Leer datos desde la base de datos ===
        # Usamos una conexión con with para asegurar que se cierre correctamente
        with engine.connect() as connection:
            # Ejemplo de cómo leer una tabla. El nombre 'citas_pacientes' depende
            # de cómo la guardes en process_files_endpoint (Pandas la pone en minúsculas)
            # Tendremos que confirmar los nombres exactos de las tablas después de la primera ejecución.
            # df_citas_pacientes = pd.read_sql("SELECT * FROM citas_pacientes", connection)
            # df_citas_motivo = pd.read_sql("SELECT * FROM citas_motivo", connection)

            # Por ahora, esta función no funcionará completamente porque la lógica
            # de lectura de DB y los nombres de tabla aún no están definidos.
            # La dejaremos así y la completaremos en un paso posterior.
            print("calculating_pacientes_nuevos_atendidos placeholder - reading from DB logic not implemented yet.")
            # --- Lógica de cálculo original (ahora comentada/removida ya que lee de DB) ---
            # if df_citas_pacientes is None or df_citas_motivo is None:
            #     print("Error: No se encontraron los DataFrames de Citas_Pacientes o Citas_Motivo en _processed_dfs.")
            #     return 0
            # ... (resto de la lógica de cálculo que usaba los dataframes) ...
            # return numero_pacientes_nuevos_atendidos # Resultado del cálculo
            # --------------------------------------------------------------
            return 0  # Retornamos 0 temporalmente

    except SQLAlchemyError as e:
        print(
            f"Error de base de datos al calcular pacientes nuevos atendidos: {str(e)}")
        return 0
    except Exception as e:
        print(f"Error general calculando pacientes nuevos atendidos: {str(e)}")
        return 0

# =============================================================


@app.get("/")
async def read_root():
    # Mensaje actualizado
    return {"message": "Hola, reychard - Base de datos conectada!"}


@app.post("/upload-excel/")
async def upload_excel(files: List[UploadFile] = File(...)):
    resultados = []
    # Limpiar DataFrames procesados anteriores ya NO es necesario aquí
    # _processed_dfs.clear() # Línea removida
    for file in files:
        try:
            data = await file.read()
            _saved_files[file.filename] = data  # Guardamos el archivo raw

            resultados.append({
                "filename": file.filename,
                # Mensaje actualizado
                "status": "Archivo recibido y guardado en memoria para procesamiento"
            })
        except Exception as e:
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
    *** Guarda los DataFrames procesados en la base de datos de Supabase. ***
    Maneja advertencias si faltan archivos esperados o sobran inesperados.
    """
    # === Esto ya NO es necesario ===
    # _processed_dfs.clear() # Línea removida
    # ==============================

    # Validar si el engine de la base de datos se creó correctamente
    if engine is None:
        return JSONResponse({"error": "No se pudo conectar a la base de datos para procesar los archivos."}, status_code=500)

    resultados = []
    advertencias = []   # Lista para acumular advertencias

    # 1) Leer el índice desde memoria
    raw_index = _saved_files.get("indice.xlsx")
    if raw_index is None:
        return JSONResponse({"error": "No se subió indice.xlsx. El índice es necesario para procesar."}, status_code=400)
    try:
        indice_df = pd.read_excel(io.BytesIO(raw_index))
        print("indice.xlsx leído exitosamente.")
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
            if pd.isna(row["Archivo"]) or pd.isna(row["Sheet"]) or pd.isna(row["Columna"]) or pd.isna(row["Nombre unificado"]) or pd.isna(row["Acción"]):
                advertencias.append(
                    f"Fila {idx+2} en indice.xlsx contiene valores faltantes en columnas clave y será ignorada.")
                continue

            archivo = str(row["Archivo"]).strip()
            hoja = str(row["Sheet"]).strip()
            orig = str(row["Columna"]).strip()
            nuevo = str(row["Nombre unificado"]).strip()
            accion = str(row["Acción"]).strip().lower()

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
                advertencias.append(
                    f"Fila {idx+2} en indice.xlsx tiene acción '{accion}' no reconocida para columna '{orig}' en archivo '{archivo}' hoja '{hoja}'. La fila será ignorada.")

        except Exception as e:
            advertencias.append(
                f"Error procesando fila {idx+2} en indice.xlsx: {str(e)}. Fila: {row.to_dict()}.")

    # 3) Validar contra la lista esperada de archivos Y los archivos subidos
    expected = {archivo for (archivo, _) in rename_dict.keys()}
    expected.update({archivo for (archivo, _, _) in drop_set})
    uploaded = set(_saved_files.keys()) - {"indice.xlsx"}

    faltan = expected - uploaded
    sobran = uploaded - expected

    archivos_faltantes_no_procesables = list(faltan)
    archivos_sobrantes_no_procesables = list(sobran)

    if faltan:
        advertencias.append(
            f"ADVERTENCIA: No se subieron estos archivos esperados (listados en el índice): {sorted(faltan)}. No podrán ser procesados.")
    if sobran:
        advertencias.append(
            f"ADVERTENCIA: Se subieron archivos no listados en el índice: {sorted(sobran)}. No serán procesados.")

    # 4) Procesar *solamente* los (archivo, hoja) definidos en rename_dict *que sí fueron subidos*
    archivos_procesados_con_exito = set()

    for (archivo, hoja), mapeo in rename_dict.items():
        if archivo not in uploaded:
            continue

        raw = _saved_files.get(archivo)
        if raw is None:
            advertencias.append(
                f"ADVERTENCIA: Archivo '{archivo}' listado en el índice para procesar (hoja '{hoja}') fue inesperadamente no encontrado en memoria. No se procesará esta combinación.")
            continue

        try:
            # Leer solo la hoja especificada
            df = pd.read_excel(io.BytesIO(raw), sheet_name=hoja)
            print(
                f"Archivo '{archivo}', Hoja '{hoja}' leído exitosamente con Pandas.")

            # Drop de columnas (solo si existen en el DF)
            to_drop = [col for (a, h, col) in drop_set if a ==
                       archivo and h == hoja]
            cols_to_actually_drop = [
                col for col in to_drop if col in df.columns]
            if cols_to_actually_drop:
                df = df.drop(columns=cols_to_actually_drop)
                print(
                    f"Columnas eliminadas para '{archivo}' '{hoja}': {cols_to_actually_drop}")

            # Rename de columnas keep (solo si existen en el DF)
            actual_mapeo = {orig_col: new_col for orig_col,
                            new_col in mapeo.items() if orig_col in df.columns}
            if actual_mapeo:
                df.rename(columns=actual_mapeo, inplace=True)
                print(
                    f"Columnas renombradas para '{archivo}' '{hoja}': {actual_mapeo}")

            # ===>>> GUARDAR EL DATAFRAME PROCESADO EN SUPABASE <<<===
            # Generamos un nombre de tabla a partir del nombre del archivo base
            # Puedes definir tu propia convención de nombres si prefieres
            # Quitamos la extensión y caracteres especiales si es necesario.
            # Pandas .to_sql convertirá el nombre a minúsculas por defecto.
            table_name = archivo.replace(".xlsx", "").replace(
                ".xslx", "").lower().replace(" ", "_")

            try:
                # Usamos el engine para conectar y guardar el DataFrame
                # if_exists='replace' borrará la tabla si existe y la creará de nuevo.
                # CUIDADO: Esto significa que CADA vez que proceses, se borrarán los datos anteriores
                # de esa tabla y se reemplazarán con los nuevos del archivo subido.
                # Para actualizar datos (ej. si Alirio sube la actualización diaria),
                # necesitaríamos otra lógica (ej. borrar datos por fecha/sucursal y luego insertar,
                # o usar upsert si la base de datos lo soporta y tienes una clave primaria).
                # Por ahora, 'replace' es el más simple para empezar.
                # index=False evita que el índice de Pandas se guarde como columna.
                print(
                    f"Intentando guardar DataFrame procesado para '{archivo}' '{hoja}' en la tabla '{table_name}' en la base de datos...")
                df.to_sql(table_name, con=engine,
                          if_exists='replace', index=False)
                print(
                    f"DataFrame procesado guardado exitosamente en la tabla '{table_name}'.")

                # === Esto ya NO es necesario ===
                # _processed_dfs[base_filename] = df # Línea removida
                # ==============================

                resultados.append({
                    "archivo":    archivo,
                    "hoja":     hoja,
                    # Mensaje actualizado
                    "status":   f"Procesado y guardado en tabla '{table_name}' exitosamente",
                    "columns":   df.columns.tolist(),
                    "row_count": len(df),
                    "saved_to_table": table_name  # Nuevo campo en el resultado
                })
                archivos_procesados_con_exito.add(archivo)

            except SQLAlchemyError as db_err:
                # Error específico de la base de datos al guardar
                print(
                    f"Error de base de datos al guardar '{archivo}' '{hoja}' en '{table_name}': {db_err}")
                advertencias.append({
                    "archivo": archivo,
                    "hoja":    hoja,
                    "error":   f"Error al guardar en la base de datos: {str(db_err)}"
                })
            except Exception as e:
                # Capturar otros errores durante el procesamiento o guardado
                print(
                    f"Error general procesando o guardando '{archivo}' '{hoja}': {e}")
                advertencias.append({
                    "archivo": archivo,
                    "hoja":    hoja,
                    "error":   f"Error durante el procesamiento o guardado: {str(e)}"
                })

        except Exception as e:
            # Capturar errores durante la lectura inicial del Excel
            print(
                f"Error al leer el archivo Excel '{archivo}' hoja '{hoja}': {e}")
            advertencias.append({
                "archivo": archivo,
                "hoja":    hoja,
                "error":   f"Error al leer el archivo Excel: {str(e)}"
            })

    # 5) Reportar archivos esperados que no pudieron ser procesados (si aplica)
    for archivo_faltante in archivos_faltantes_no_procesables:
        pass  # Ya se añadió una advertencia general arriba

    # Reportar archivos subidos que no fueron listados en el índice (si aplica)
    for archivo_sobrante in archivos_sobrantes_no_procesables:
        pass  # Ya se añadió una advertencia general arriba

    # 6) Devolver resultados y advertencias
    response_content: Dict[str, Any] = {"resultados_procesamiento": resultados}
    if advertencias:
        response_content["advertencias_o_errores"] = advertencias

    return JSONResponse(response_content, status_code=200)


@app.post("/calculate-insights/")
async def calculate_insights_endpoint():
    """
    Endpoint para calcular los insights predefinidos y devolverlos.
    La lógica de guardar en Airtable se maneja en Make.
    *** Ahora lee los datos desde la base de datos. ***
    """

    # Ya no verificamos _processed_dfs, verificamos la conexión a la DB
    if engine is None:
        return JSONResponse({"error": "No se pudo conectar a la base de datos para calcular insights."}, status_code=500)

    try:
        # === Calcular el primer insight (que ahora lee de la DB) ===
        # NOTA: La función calcular_pacientes_nuevos_atendidos actual es un placeholder
        # y necesita ser modificada para leer de la base de datos y aceptar filtros si aplica.
        pacientes_nuevos_atendidos_count = calcular_pacientes_nuevos_atendidos()

        # ... (resto de la lógica para devolver el insight) ...
        insight_id = "insight_pacientes_nuevos_atendidos_total"
        question_key = "Cantidad total de pacientes nuevos atendidos"
        # Usamos el resultado del cálculo (actualmente 0)
        answer_value = pacientes_nuevos_atendidos_count
        units = "pacientes"
        dimensions = {}  # Podríamos devolver esto si es útil para Make

        return JSONResponse({
            "status": "Insights calculados exitosamente",
            "calculated_insight": {
                "insight_id": insight_id,
                "question_key": question_key,
                "answer_value": answer_value,
                "units": units,
                "dimensions": dimensions
            }
        }, status_code=200)

    except Exception as e:
        print(f"Error general en el endpoint /calculate-insights: {e}")
        return JSONResponse({"error": f"Ocurrió un error al calcular insights: {str(e)}"}, status_code=500)


@app.post("/slack")
async def slack_command(request: Request):
    """
    Endpoint activado por el comando /sherlock en Slack.
    Recibe la pregunta del usuario y la envía a Make para procesar el árbol de decisiones.
    """
    form_data = await request.form()
    print("Payload recibido:", form_data)

    user_question = form_data.get("text")
    if not user_question:
        # Mensaje mejorado
        return JSONResponse({"text": "Lo siento, no recibí tu pregunta. Por favor, intenta de nuevo."}, status_code=200)

    make_webhook_url = os.environ.get("MAKE_SHERLOCK_WEBHOOK_URL")

    if not make_webhook_url:
        print(
            "Error: La variable de entorno MAKE_SHERLOCK_WEBHOOK_URL no está configurada.")
        return JSONResponse({"text": "Sherlock no está configurado correctamente para procesar tu pregunta. Contacta a soporte o a Rich."}, status_code=500)

    try:
        payload_to_make = {
            "user_question": user_question,
            "user_id": form_data.get("user_id"),
            "channel_id": form_data.get("channel_id")
        }

        response_from_make = requests.post(
            make_webhook_url, json=payload_to_make)

        response_from_make.raise_for_status()

        return JSONResponse({"text": f"Pregunta recibida: '{user_question}'. Sherlock está procesando. Un momento por favor..."}, status_code=200)

    except requests.exceptions.RequestException as e:
        print(f"Error al llamar al webhook de Make: {e}")
        return JSONResponse({"text": "Ocurrió un error al enviar tu pregunta a Sherlock para procesamiento. Por favor contacta a Soporte."}, status_code=500)
    except Exception as e:
        print(f"Ocurrió un error inesperado en el endpoint /slack: {e}")
        return JSONResponse({"text": "Ocurrió un error interno al procesar tu pregunta. Por favor contacta a soporte."}, status_code=500)

import pandas as pd
import io
import os
import requests
import json
import psycopg2.extras
import zipfile  # Importar la librería zipfile
from fastapi import FastAPI, Request, File, UploadFile, Form  # Importar Form
from fastapi.responses import JSONResponse
from typing import List, Dict, Any
from typing import Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


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


@app.post("/process-all-files/")
# Espera un solo archivo tipo File
async def process_all_files_endpoint(zip_file: UploadFile = File(...)):
    """
    Endpoint para recibir un archivo ZIP con todos los archivos Excel dentro,
    descomprimirlo en memoria y procesar los archivos, incluyendo el índice.
    Espera UN solo archivo UploadFile que es el ZIP.
    """
    # Validar si el engine de la base de datos se creó correctamente
    if engine is None:
        _saved_files.clear()
        return JSONResponse({"error": "No se pudo conectar a la base de datos para procesar los archivos."}, status_code=500)

    # Limpiamos saved_files al inicio para asegurarnos de que solo tenemos los archivos del ZIP
    _saved_files.clear()

    resultados = []
    advertencias = []
    raw_index = None

    print(f"Recibido archivo: {zip_file.filename}")

    # === Descomprimir el archivo ZIP en memoria ===
    try:
        zip_content = await zip_file.read()  # Leer el contenido binario del ZIP
        # Usamos io.BytesIO para tratar el contenido binario como un archivo en memoria
        with zipfile.ZipFile(io.BytesIO(zip_content), 'r') as zip_ref:
            print(
                f"Archivo ZIP '{zip_file.filename}' abierto. Contiene {len(zip_ref.namelist())} archivos.")
            # Iterar sobre cada archivo dentro del ZIP
            for filename_in_zip in zip_ref.namelist():
                # Procesar solo archivos Excel
                if filename_in_zip.endswith(('.xlsx', '.xls')):
                    try:
                        with zip_ref.open(filename_in_zip) as excel_file_in_zip:
                            # Leemos el contenido de cada archivo Excel dentro del ZIP
                            data = excel_file_in_zip.read()
                            # Guardamos el contenido en nuestra variable en memoria
                            _saved_files[filename_in_zip] = data
                            print(
                                f"Archivo '{filename_in_zip}' extraído del ZIP.")
                            # Identificamos el archivo indice.xlsx (ignorando case)
                            if filename_in_zip.lower() == "indice.xlsx":
                                raw_index = data

                    except Exception as e:
                        advertencias.append({
                            "filename_in_zip": filename_in_zip,
                            "error": f"Error al extraer o leer archivo del ZIP: {str(e)}"
                        })

    except zipfile.BadZipFile:
        _saved_files.clear()
        return JSONResponse({"error": "El archivo subido no es un archivo ZIP válido."}, status_code=400)
    except Exception as e:
        _saved_files.clear()
        return JSONResponse({"error": f"Error al procesar el archivo ZIP: {str(e)}"}, status_code=500)
    # === Fin de la descompresión del ZIP ===

    # Verificamos si el indice.xlsx fue encontrado dentro del ZIP
    if raw_index is None:
        _saved_files.clear()
        return JSONResponse({"error": "No se encontró indice.xlsx dentro del archivo ZIP subido."}, status_code=400)

    # Ahora _saved_files contiene los contenidos de los archivos Excel extraídos del ZIP.
    # La lógica restante es la misma que antes.

    # 1) Leer el índice desde memoria (ya tenemos raw_index del ZIP)
    try:
        # raw_index ya fue leído del ZIP
        indice_df = pd.read_excel(io.BytesIO(raw_index))
        print("indice.xlsx leído exitosamente desde el ZIP.")
    except Exception as e:
        _saved_files.clear()
        return JSONResponse({"error": f"Error al leer indice.xlsx desde el ZIP: {str(e)}. Asegúrese de que sea un archivo Excel válido dentro del ZIP."}, status_code=400)

    # 2) Construir rename_dict y drop_set (copiar lógica)
    rename_dict: dict[tuple[str, str], dict[str, str]] = {}
    drop_set: set[tuple[str, str, str]] = set()

    required_cols = ["Archivo", "Sheet",
                     "Columna", "Nombre unificado", "Acción"]
    if not all(col in indice_df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in indice_df.columns]
        _saved_files.clear()
        return JSONResponse({"error": f"Columnas requeridas faltantes en indice.xlsx: {missing}. Asegúrese de que el archivo índice dentro del ZIP tenga las columnas correctas."}, status_code=400)

    for idx, row in indice_df.iterrows():
        try:
            if pd.isna(row["Archivo"]) or pd.isna(row["Sheet"]) or pd.isna(row["Columna"]) or pd.isna(row["Nombre unificado"]) or pd.isna(row["Acción"]):
                advertencias.append(
                    f"Fila {idx+2} en indice.xlsx (dentro del ZIP) contiene valores faltantes en columnas clave y será ignorada.")
                continue

            archivo = str(row["Archivo"]).strip()
            hoja = str(row["Sheet"]).strip()
            orig = str(row["Columna"]).strip()
            nuevo = str(row["Nombre unificado"]).strip()
            accion = str(row["Acción"]).strip().lower()

            if not archivo or not hoja or not orig or not accion:
                advertencias.append(
                    f"Fila {idx+2} en indice.xlsx (dentro del ZIP) tiene campos vacíos después de limpiar y será ignorada.")
                continue

            key = (archivo, hoja)
            if accion == "drop":
                drop_set.add((archivo, hoja, orig))
            elif accion == "keep":
                if key not in rename_dict:
                    rename_dict[key] = {}
                if not nuevo:
                    advertencias.append(
                        f"Fila {idx+2} en indice.xlsx (dentro del ZIP) tiene acción 'keep' para columna '{orig}' pero el 'Nombre unificado' está vacío. Esta columna no se renombrará.")
                    rename_dict[key][orig] = orig
                else:
                    rename_dict[key][orig] = nuevo
            else:
                advertencias.append(
                    f"Fila {idx+2} en indice.xlsx (dentro del ZIP) tiene acción '{accion}' no reconocida para columna '{orig}' en archivo '{archivo}' hoja '{hoja}'. La fila será ignorada.")
        except Exception as e:
            advertencias.append(
                f"Error procesando fila {idx+2} en indice.xlsx (dentro del ZIP): {str(e)}. Fila: {row.to_dict()}.")

    # 3) Validar contra la lista esperada de archivos Y los archivos recibidos del ZIP
    expected = {archivo for (archivo, _) in rename_dict.keys()}
    expected.update({archivo for (archivo, _, _) in drop_set})
    # 'uploaded' son ahora los nombres de archivo que extrajimos del ZIP, menos el indice
    uploaded = set(_saved_files.keys()) - {"indice.xlsx"}

    faltan = expected - uploaded
    sobran = uploaded - expected

    archivos_faltantes_no_procesables = list(faltan)
    archivos_sobrantes_no_procesables = list(sobran)

    if faltan:
        advertencias.append(
            f"ADVERTENCIA: No se encontraron estos archivos esperados (listados en el índice) dentro del ZIP: {sorted(faltan)}. No podrán ser procesados.")
    if sobran:
        advertencias.append(
            f"ADVERTENCIA: Se encontraron archivos dentro del ZIP no listados en el índice: {sorted(sobran)}. No serán procesados.")

    # 4) Procesar *solamente* los (archivo, hoja) definidos en rename_dict *que sí fueron extraídos del ZIP*
    # === INICIO DE LA SELECCIÓN DEL SUBSET Y LOGICA DE execute_values (Copiar lógica) ===
    archivos_procesados_con_exito = set()

    rename_keys_to_process = sorted(list(rename_dict.keys()))

    # Filtramos esta lista para incluir solo las claves donde el archivo correspondiente fue realmente extraído del ZIP
    files_and_sheets_to_actually_attempt = [
        key for key in rename_keys_to_process if key[0] in uploaded
    ]

    # >>>>>>>>>> AQUÍ SE SELECCIONAN LOS PRIMEROS 4 ARCHIVOS/HOJAS (para la prueba) <<<<<<<<<<
    subset_to_process = files_and_sheets_to_actually_attempt[:4]
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    print(
        f"MODO DE PRUEBA: Procesando solo {len(subset_to_process)} combinaciones de archivo/hoja de un total de {len(files_and_sheets_to_actually_attempt)} posibles (filtrado por archivos en ZIP).")

    for (archivo, hoja) in subset_to_process:

        # Verifica que el archivo raw exista en memoria (debería existir ya que filtramos por 'uploaded')
        raw = _saved_files.get(archivo)
        if raw is None:
            advertencias.append(
                f"ADVERTENCIA: Archivo '{archivo}' listado para procesar (hoja '{hoja}') fue inesperadamente no encontrado en memoria (extraído del ZIP). No se procesará esta combinación.")
            continue

        try:
            df = pd.read_excel(io.BytesIO(raw), sheet_name=hoja)
            print(
                f"Archivo '{archivo}', Hoja '{hoja}' leído exitosamente con Pandas desde memoria.")

            to_drop = [col for (a, h, col) in drop_set if a ==
                       archivo and h == hoja]
            cols_to_actually_drop = [
                col for col in to_drop if col in df.columns]
            if cols_to_actually_drop:
                df = df.drop(columns=cols_to_actually_drop)
                print(
                    f"Columnas eliminadas para '{archivo}' '{hoja}': {cols_to_actually_drop}")

            mapeo = rename_dict.get((archivo, hoja), {})
            actual_mapeo = {orig_col: new_col for orig_col,
                            new_col in mapeo.items() if orig_col in df.columns}

            if actual_mapeo:
                df.rename(columns=actual_mapeo, inplace=True)
                print(
                    f"Columnas renombradas para '{archivo}' '{hoja}': {actual_mapeo}")

            table_name = archivo.replace(".xlsx", "").replace(
                ".xslx", "").lower().replace(" ", "_")
            table_name = "".join(
                c for c in table_name if c.isalnum() or c == '_' or c == '-').strip('_-')

            # --- Lógica de guardado con execute_values ---
            try:
                print(
                    f"Intentando guardar DataFrame procesado para '{archivo}' '{hoja}' en la tabla '{table_name}' en la base de datos usando execute_values...")

                try:
                    if not df.empty:
                        df.head(0).to_sql(table_name, con=engine,
                                          if_exists='append', index=False)
                        print(
                            f"Verificando/Creando estructura de tabla '{table_name}'...")
                    else:
                        print(
                            f"DataFrame para '{archivo}' '{hoja}' está vacío. No se verificará/creará la tabla con head(0).")

                except SQLAlchemyError as create_table_err:
                    print(
                        f"Posible error al verificar/crear tabla '{table_name}' (puede ser que ya existía): {create_table_err}")
                    pass

                data_to_insert = df.values.tolist()

                if not data_to_insert:
                    print(
                        f"DataFrame para '{archivo}' '{hoja}' está vacío, no hay datos para insertar.")
                    resultados.append({
                        "archivo":    archivo,
                        "hoja":     hoja,
                        "status":   f"Procesado y guardado en tabla '{table_name}' (DataFrame vacío)",
                        "columns":   df.columns.tolist(),
                        "row_count": 0,
                        "saved_to_table": table_name
                    })
                    archivos_procesados_con_exito.add(archivo)
                    continue

                columns = df.columns.tolist()
                quoted_columns = [f'"{col}"' for col in columns]
                columns_sql_string = ', '.join(quoted_columns)
                insert_sql = f"INSERT INTO {table_name} ({columns_sql_string}) VALUES %s"

                with engine.connect() as connection:
                    raw_connection = connection.connection
                    with raw_connection.cursor() as cursor:
                        psycopg2.extras.execute_values(
                            cursor,
                            insert_sql,
                            data_to_insert,
                            page_size=1000
                        )
                    raw_connection.commit()

                print(
                    f"DataFrame procesado guardado exitosamente en la tabla '{table_name}' usando execute_values.")

                resultados.append({
                    "archivo":    archivo,
                    "hoja":     hoja,
                    "status":   f"Procesado y guardado en tabla '{table_name}' exitosamente (execute_values)",
                    "columns":   columns,
                    "row_count": len(data_to_insert),
                    "saved_to_table": table_name
                })
                archivos_procesados_con_exito.add(archivo)

            except SQLAlchemyError as db_err:
                print(
                    f"Error de base de datos al guardar '{archivo}' '{hoja}' en '{table_name}' (execute_values): {db_err}")
                advertencias.append({
                    "archivo": archivo,
                    "hoja":    hoja,
                    "error":   f"Error al guardar en la base de datos (execute_values): {str(db_err)}"
                })
            except Exception as e:
                print(f"Error general guardando '{archivo}' '{hoja}': {e}")
                advertencias.append({
                    "archivo": archivo,
                    "hoja":    hoja,
                    "error":   f"Error durante el guardado (execute_values): {str(e)}"
                })

        except Exception as e:
            print(
                f"Error al leer el archivo Excel o procesar con Pandas '{archivo}' hoja '{hoja}': {e}")
            advertencias.append({
                "archivo": archivo,
                "hoja":    hoja,
                "error":   f"Error al leer el archivo Excel o procesar con Pandas: {str(e)}"
            })

    # 5) Reportar archivos esperados que no pudieron ser procesados (si aplica)
    pass

    # 6) Limpiar los archivos raw de la memoria una vez procesados
    # Aunque ahora leemos del ZIP, mantenemos esto para limpiar la variable global si se usó
    _saved_files.clear()
    print("Archivos raw limpiados de la memoria después del procesamiento.")

    # 7) Devolver resultados y advertencias
    response_content: Dict[str, Any] = {"resultados_procesamiento": resultados}
    if advertencias:
        response_content["advertencias_o_errores"] = advertencias
        return JSONResponse(response_content, status_code=500)
    else:
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

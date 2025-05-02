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
import psycopg2.extras
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


@app.post("/process-excel/")
async def process_files_endpoint():
    """
    Endpoint para procesar los archivos Excel subidos,
    aplicando la lógica del índice para limpieza y renombrado.
    *** Guarda los DataFrames procesados en la base de datos de Supabase
        usando execute_values para mejor rendimiento. ***
    Maneja advertencias si faltan archivos esperados o sobran inesperados.

    *** MODIFICACIÓN TEMPORAL: Procesará solo los primeros 4 archivos para prueba de rendimiento. ***
    """
    # Validar si el engine de la base de datos se creó correctamente
    if engine is None:
        # Limpiamos saved_files si no podemos procesar
        _saved_files.clear()
        return JSONResponse({"error": "No se pudo conectar a la base de datos para procesar los archivos."}, status_code=500)

    resultados = []
    advertencias = []   # Lista para acumular advertencias

    # 1) Leer el índice desde memoria
    raw_index = _saved_files.get("indice.xlsx")
    if raw_index is None:
        # Limpiar saved_files si el índice no se subió
        _saved_files.clear()
        return JSONResponse({"error": "No se subió indice.xlsx. El índice es necesario para procesar."}, status_code=400)
    try:
        indice_df = pd.read_excel(io.BytesIO(raw_index))
        print("indice.xlsx leído exitosamente.")
    except Exception as e:
        # Limpiar saved_files si el índice no se puede leer
        _saved_files.clear()
        return JSONResponse({"error": f"Error al leer indice.xlsx: {str(e)}. Asegúrese de que sea un archivo Excel válido."}, status_code=400)

    # 2) Construir rename_dict y drop_set
    rename_dict: dict[tuple[str, str], dict[str, str]] = {}
    drop_set: set[tuple[str, str, str]] = set()

    required_cols = ["Archivo", "Sheet",
                     "Columna", "Nombre unificado", "Acción"]
    if not all(col in indice_df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in indice_df.columns]
        # Limpiar saved_files si el índice tiene columnas faltantes
        _saved_files.clear()
        return JSONResponse({"error": f"Columnas requeridas faltantes en indice.xlsx: {missing}. Asegúrese de que el archivo índice tenga las columnas correctas."}, status_code=400)

    # Procesar filas del índice para construir diccionarios de mapeo y eliminación
    for idx, row in indice_df.iterrows():
        try:
            # Asegurarse de que los valores no sean NaN antes de intentar strip
            if pd.isna(row["Archivo"]) or pd.isna(row["Sheet"]) or pd.isna(row["Columna"]) or pd.isna(row["Nombre unificado"]) or pd.isna(row["Acción"]):
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
                # Si el nombre unificado está vacío para una acción 'keep', es un problema
                if not nuevo:
                    advertencias.append(
                        f"Fila {idx+2} en indice.xlsx tiene acción 'keep' para columna '{orig}' pero el 'Nombre unificado' está vacío. Esta columna no se renombrará.")
                    # Opcional: puedes decidir si esto debería ser un error fatal o solo advertencia
                    # continue # Si quieres saltar esta fila
                    # Si decides no saltar, usará el nombre original o fallará si intentas usar 'nuevo'
                    # Opcional: usa el nombre original como unificado
                    rename_dict[key][orig] = orig
                else:
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
    # === INICIO DE LA MODIFICACIÓN TEMPORAL PARA PROCESAR SUBSET Y USAR execute_values ===
    archivos_procesados_con_exito = set()

    # Obtenemos una lista de las claves (archivo, hoja) definidas en rename_dict (las que tienen acción 'keep')
    # Ordenamos para procesar siempre los mismos archivos en el modo subset
    rename_keys_to_process = sorted(list(rename_dict.keys()))

    # Filtramos esta lista para incluir solo las claves donde el archivo correspondiente fue realmente subido
    files_and_sheets_to_actually_attempt = [
        key for key in rename_keys_to_process if key[0] in uploaded
    ]

    # >>>>>>>>>> AQUÍ SE SELECCIONAN LOS PRIMEROS 4 ARCHIVOS/HOJAS <<<<<<<<<<
    # Selecciona solo los primeros 4 elementos de la lista de archivos/hojas que se pueden intentar procesar
    subset_to_process = files_and_sheets_to_actually_attempt[:4]
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    print(
        f"MODO DE PRUEBA: Procesando solo {len(subset_to_process)} combinaciones de archivo/hoja de un total de {len(files_and_sheets_to_actually_attempt)} posibles (filtrado por archivos subidos).")
    # === FIN DE LA SELECCIÓN DEL SUBSET ===

    # === Iterar sobre el subconjunto seleccionado para procesar ===
    # Este bucle solo se ejecutará para las primeras 4 combinaciones de archivo/hoja que seleccionamos arriba
    for (archivo, hoja) in subset_to_process:

        # Las acciones de drop y rename se aplican solo si este par (archivo, hoja) está en rename_dict,
        # lo cual ya garantizamos al iterar sobre subset_to_process que se deriva de rename_dict.

        # Verifica que el archivo raw exista en memoria (debería existir ya que filtramos por 'uploaded')
        raw = _saved_files.get(archivo)
        if raw is None:
            advertencias.append(
                f"ADVERTENCIA: Archivo '{archivo}' listado para procesar (hoja '{hoja}') fue inesperadamente no encontrado en memoria durante la iteración. No se procesará esta combinación.")
            continue

        try:
            # Leer solo la hoja especificada del archivo raw
            df = pd.read_excel(io.BytesIO(raw), sheet_name=hoja)
            print(
                f"Archivo '{archivo}', Hoja '{hoja}' leído exitosamente con Pandas.")

            # === Aplicar DROP y RENAME basados en el índice ===
            # Obtener las columnas a eliminar para este archivo y hoja
            to_drop = [col for (a, h, col) in drop_set if a ==
                       archivo and h == hoja]
            # Filtrar las columnas a eliminar para asegurar que existan en el DataFrame actual
            cols_to_actually_drop = [
                col for col in to_drop if col in df.columns]
            if cols_to_actually_drop:
                df = df.drop(columns=cols_to_actually_drop)
                print(
                    f"Columnas eliminadas para '{archivo}' '{hoja}': {cols_to_actually_drop}")

            # Obtener el mapeo de nombres para renombrar para este archivo y hoja
            mapeo = rename_dict.get((archivo, hoja), {})
            # Filtrar el mapeo para incluir solo columnas que existan en el DataFrame actual
            actual_mapeo = {orig_col: new_col for orig_col,
                            new_col in mapeo.items() if orig_col in df.columns}

            if actual_mapeo:
                df.rename(columns=actual_mapeo, inplace=True)
                print(
                    f"Columnas renombradas para '{archivo}' '{hoja}': {actual_mapeo}")
            # =============================================

            # ===>>> GUARDAR EL DATAFRAME PROCESADO EN SUPABASE USANDO execute_values <<<===
            # Generamos un nombre de tabla a partir del nombre del archivo base (sin extensión y en minúsculas)
            table_name = archivo.replace(".xlsx", "").replace(
                ".xslx", "").lower().replace(" ", "_")
            # Podemos añadir más limpieza si es necesario para caracteres no SQL
            table_name = "".join(
                c for c in table_name if c.isalnum() or c == '_' or c == '-').strip('_-')

            # --- Lógica de guardado con execute_values ---
            try:
                print(
                    f"Intentando guardar DataFrame procesado para '{archivo}' '{hoja}' en la tabla '{table_name}' en la base de datos usando execute_values...")

                # Para usar execute_values, la tabla debe existir.
                # Podemos intentar crearla si no existe.
                # Esta es una forma simple de crear la tabla basándose en el DataFrame:
                try:
                    # Intentar crear la tabla si no existe, usando df.to_sql con if_exists='append' y pasando 0 filas
                    # Esto crea la tabla con el esquema adecuado si no está.
                    # No inserta datos porque el DataFrame está vacío para esta llamada.
                    # Esto puede ser ineficiente si la tabla siempre existe, considera optimizar.
                    if not df.empty:  # Solo intentamos si el DF tiene datos, sino la creacion con append no funciona bien
                        df.head(0).to_sql(table_name, con=engine,
                                          if_exists='append', index=False)
                        print(
                            f"Verificando/Creando estructura de tabla '{table_name}'...")
                    else:
                        # Si el DF está vacío, solo intentaremos la inserción y esperamos que la tabla ya exista.
                        print(
                            f"DataFrame para '{archivo}' '{hoja}' está vacío. No se verificará/creará la tabla con head(0).")

                except SQLAlchemyError as create_table_err:
                    # Ignoramos errores si la tabla ya existe (como 'relation "..." already exists')
                    # O puedes ser más específico en el manejo de errores
                    print(
                        f"Posible error al verificar/crear tabla '{table_name}' (puede ser que ya existía): {create_table_err}")
                    pass  # Continuamos intentando insertar

                # Convertir el DataFrame a una lista de tuplas.
                # Esto se hace eficientemente con .values.tolist()
                data_to_insert = df.values.tolist()

                # Si el DataFrame está vacío, no hay nada que insertar
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
                    # No hay datos para insertar, saltamos la parte de execute_values y confirmación
                    continue  # Pasar a la siguiente iteración del bucle for

                # Obtener los nombres de las columnas del DataFrame procesado para la sentencia SQL
                columns = df.columns.tolist()

                # Construir la sentencia SQL de inserción.
                # Usamos comillas dobles alrededor de los nombres de columna por si acaso
                # usamos nombres con espacios o caracteres especiales que SQL requiera escapar.
                # El `VALUES %s` es el formato esperado por execute_values para la lista de tuplas.
                # Formateamos los nombres de columna para la sentencia SQL, encerrándolos en comillas dobles
                # Esto ayuda si los nombres de columna tienen espacios o caracteres especiales
                quoted_columns = [f'"{col}"' for col in columns]
                # Unimos los nombres de columna formateados con ', '
                columns_sql_string = ', '.join(quoted_columns)
                # Construimos la sentencia INSERT final usando la cadena de columnas formateada
                insert_sql = f"INSERT INTO {table_name} ({columns_sql_string}) VALUES %s"

                # Obtener una conexión cruda de psycopg2 desde el pool de SQLAlchemy y usar un cursor.
                # Usamos 'with' para asegurar que la conexión y el cursor se manejen correctamente.
                with engine.connect() as connection:
                    # connection.connection gives the raw psycopg2 connection
                    raw_connection = connection.connection
                    with raw_connection.cursor() as cursor:
                        # Usar execute_values para la inserción masiva
                        # page_size determina cuántas filas se envían al servidor en cada lote.
                        # Un tamaño de página adecuado puede mejorar el rendimiento. 1000 es un valor común.
                        psycopg2.extras.execute_values(
                            cursor,
                            insert_sql,
                            data_to_insert,
                            page_size=1000
                        )
                    # Confirmar la transacción para guardar los datos en la base de datos
                    raw_connection.commit()

                print(
                    f"DataFrame procesado guardado exitosamente en la tabla '{table_name}' usando execute_values.")

                resultados.append({
                    "archivo":    archivo,
                    "hoja":     hoja,
                    # Mensaje actualizado
                    "status":   f"Procesado y guardado en tabla '{table_name}' exitosamente (execute_values)",
                    "columns":   columns,
                    "row_count": len(data_to_insert),
                    "saved_to_table": table_name
                })
                archivos_procesados_con_exito.add(archivo)

                # --- Fin de la lógica de guardado con execute_values ---

            except SQLAlchemyError as db_err:
                # Capturar errores específicos de la base de datos (SQLAlchemy/psycopg2)
                print(
                    f"Error de base de datos al guardar '{archivo}' '{hoja}' en '{table_name}' (execute_values): {db_err}")
                advertencias.append({
                    "archivo": archivo,
                    "hoja":    hoja,
                    "error":   f"Error al guardar en la base de datos (execute_values): {str(db_err)}"
                })
            except Exception as e:
                # Capturar cualquier otro error inesperado durante el guardado
                print(f"Error general guardando '{archivo}' '{hoja}': {e}")
                advertencias.append({
                    "archivo": archivo,
                    "hoja":    hoja,
                    "error":   f"Error durante el guardado (execute_values): {str(e)}"
                })

        except Exception as e:
            # Capturar errores durante la lectura inicial del Excel o procesamiento con Pandas
            print(
                f"Error al leer el archivo Excel o procesar con Pandas '{archivo}' hoja '{hoja}': {e}")
            advertencias.append({
                "archivo": archivo,
                "hoja":    hoja,
                "error":   f"Error al leer el archivo Excel o procesar con Pandas: {str(e)}"
            })

    # 5) Reportar archivos esperados que no pudieron ser procesados (si aplica)
    # Estas advertencias ya se añadieron antes de empezar a iterar.
    pass  # No hacemos nada específico aquí.

    # 6) Limpiar los archivos raw de la memoria una vez procesados
    _saved_files.clear()
    print("Archivos raw limpiados de la memoria después del procesamiento.")

    # 7) Devolver resultados y advertencias
    response_content: Dict[str, Any] = {"resultados_procesamiento": resultados}
    if advertencias:
        response_content["advertencias_o_errores"] = advertencias
        # Podemos decidir si un error de DB debe ser un 500 o un 200 con advertencia.
        # Por ahora, si hubo errores, devolvemos 500 para que Make lo detecte más claramente.
        # Si solo hubo advertencias (ej. archivo no subido), devolvemos 200.
        # Si la lista de resultados_procesamiento está vacía Y hay advertencias/errores, probablemente sea un 500.
        # Si hay resultados Y advertencias/errores, puede ser un 200.
        # Para simplificar por ahora: si hay CUALQUIER advertencia/error en la lista, devolvemos 500.
        # Si quieres ser más granular, puedes verificar los tipos de errores.
        # Cambiado a 500 si hay advertencias/errores
        return JSONResponse(response_content, status_code=500)
    else:
        # Si no hay advertencias, todo salió según lo planeado para los archivos procesados.
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

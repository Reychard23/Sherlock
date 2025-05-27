
from fastapi import FastAPI, Request, File, UploadFile, HTTPException, Form, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Tuple, Literal
import pandas as pd
import numpy as np
import os
import io
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

import procesador_datos

app = FastAPI()

_saved_files: dict[str, bytes] = {}
_indice_file_content: bytes | None = None
_indice_file_name: str | None = None

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = None

if not DATABASE_URL:
    print("ADVERTENCIA: La variable de entorno DATABASE_URL no está configurada.")
else:
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            print("Conexión a Supabase (PostgreSQL) establecida exitosamente.")
    except Exception as e:
        print(f"Error al crear el engine de SQLAlchemy o al conectar: {e}")
        engine = None


def save_df_to_supabase(
    df: pd.DataFrame,
    table_name: str,
    db_engine: Any,
    if_exists: Literal['fail', 'replace', 'append'] = 'replace'
):
    if db_engine is None:
        print(
            f"ERROR: No hay conexión a la base de datos. No se puede guardar la tabla '{table_name}'.")
        raise ConnectionError("Conexión a la base de datos no establecida.")
    if df.empty:
        print(
            f"ADVERTENCIA: El DataFrame para la tabla '{table_name}' está vacío. No se guardará.")
        return
    try:
        print(
            f"Intentando guardar DataFrame en la tabla '{table_name}' en Supabase (modo: {if_exists})...")
        df.to_sql(table_name, db_engine, if_exists=if_exists,
                  index=False, chunksize=1000, method='multi')
        print(
            f"DataFrame guardado exitosamente en la tabla '{table_name}'. Filas: {len(df)}")
    except Exception as e:
        print(f"Error al guardar en la tabla '{table_name}': {e}")
        raise


@app.post("/upload_single_file/")
async def upload_single_file(file: UploadFile = File(...), filename: str = Form(...)):
    global _indice_file_content, _indice_file_name
    try:
        content = await file.read()
        # CAMBIO AQUÍ: Buscar "indice.xlsx" en lugar de "indice.csv"
        if filename.lower() == "indice.xlsx":
            _indice_file_content = content
            _indice_file_name = filename
            print(
                f"Archivo índice '{filename}' (Excel) almacenado en memoria.")
        else:
            _saved_files[filename] = content
            print(
                f"Archivo de datos '{filename}' almacenado en memoria. Total archivos de datos: {len(_saved_files)}")
        return {"filename": filename, "message": "Archivo recibido y almacenado temporalmente en memoria."}
    except Exception as e:
        print(f"Error al recibir el archivo '{filename}': {e}")
        raise HTTPException(
            status_code=500, detail=f"Error al procesar el archivo {filename}: {str(e)}")


class InMemoryUploadFile:
    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self.file = io.BytesIO(content)
        self._content = content

    async def read(self) -> bytes:
        self.file.seek(0)
        return self.file.read()

    def seek(self, offset: int, whence: int = 0) -> int:
        return self.file.seek(offset, whence)

    def tell(self) -> int:
        return self.file.tell()

    async def close(self):
        self.file.close()


async def do_the_heavy_processing_and_saving(
    # Pasaremos lo que necesite, como copias de las variables globales o los datos directamente
    # Para evitar problemas con el estado global si múltiples requests llegan muy rápido (aunque tu flujo de Make es secuencial)
    # es mejor pasar los datos necesarios.
    # Por ahora, para simplificar, asumiremos que puede acceder a las globales,
    # pero idealmente se le pasarían copias.
):
    # Necesita acceso a estas
    global _saved_files, _indice_file_content, _indice_file_name, engine

    print("--- Log Sherlock (BG Task): INICIO TAREA DE FONDO para procesar y guardar ---")
    all_server_warnings_bg = []  # Usar una lista local para la tarea de fondo

    # --- Copiamos la lógica de tu /trigger_processing_and_save/ original aquí ---
    # PERO con cuidado con las variables globales si hay concurrencia.
    # Para tu flujo actual de Make (una ejecución a la vez), acceder a las globales debería estar bien,
    # pero hay que limpiarlas al final o al inicio de una nueva subida.

    # Hacemos una copia de los datos para que la tarea de fondo trabaje con un "snapshot"
    # y la limpieza de las globales no afecte a una tarea en curso si algo sale muy mal.
    current_saved_files = _saved_files.copy()
    current_indice_content = _indice_file_content
    current_indice_name = _indice_file_name

    # Limpiamos las globales INMEDIATAMENTE después de copiarlas,
    # para que el endpoint /upload_single_file/ esté listo para un nuevo lote.
    _saved_files.clear()
    _indice_file_content = None
    _indice_file_name = None
    print("--- Log Sherlock (BG Task): Variables globales de archivos limpiadas para el próximo lote.")

    if engine is None:
        print("--- Log Sherlock (BG Task): ERROR - DB Engine no disponible.")
        # Aquí no podemos hacer HTTPException, la tarea de fondo no devuelve HTTP.
        # Podrías loggear a un sistema externo o una tabla de estado si necesitas monitorear fallos de tareas de fondo.
        return

    if not current_indice_content or not current_indice_name:
        print("--- Log Sherlock (BG Task): ERROR - Archivo índice no disponible para la tarea.")
        return

    if not current_saved_files:
        print("--- Log Sherlock (BG Task): ERROR - No hay archivos de datos para la tarea.")
        return

    try:
        print(
            f"--- Log Sherlock (BG Task): Iniciando procesamiento. Archivo índice: '{current_indice_name}'. Archivos de datos: {len(current_saved_files)}")

        data_files_simulated: List[InMemoryUploadFile] = [InMemoryUploadFile(
            fname, fcontent) for fname, fcontent in current_saved_files.items()]
        indice_file_simulated = InMemoryUploadFile(
            filename=current_indice_name, content=current_indice_content)

        print("--- Log Sherlock (BG Task): Llamando a load_dataframes_from_uploads...")
        processed_dfs_map, _, _, carga_warnings = procesador_datos.load_dataframes_from_uploads(
            data_files=data_files_simulated,
            index_file=indice_file_simulated
        )
        all_server_warnings_bg.extend(carga_warnings)
        print("--- Log Sherlock (BG Task): Retorno de load_dataframes_from_uploads.")

        if not processed_dfs_map:
            print(
                f"--- Log Sherlock (BG Task): ERROR - No se pudieron cargar DataFrames. Advertencias: {carga_warnings}")
            return

        print("--- Log Sherlock (BG Task): DataFrames cargados.")
        print("--- Log Sherlock (BG Task): Llamando a generar_insights_pacientes...")
        final_dataframes_to_save = procesador_datos.generar_insights_pacientes(
            processed_dfs_map, all_server_warnings_bg)
        print("--- Log Sherlock (BG Task): Retorno de generar_insights_pacientes.")

        if not final_dataframes_to_save:
            print(
                f"--- Log Sherlock (BG Task): ERROR - No se pudieron generar insights. Advertencias: {all_server_warnings_bg}")
            return

        print(
            f"--- Log Sherlock (BG Task): Insights generados. DF listos para guardar: {list(final_dataframes_to_save.keys())}")

        for table_name_key, df_to_save in final_dataframes_to_save.items():
            nombre_tabla_en_db = table_name_key.lower()
            if df_to_save is not None and not df_to_save.empty:
                try:
                    print(
                        f"--- Log Sherlock (BG Task): Guardando tabla: {nombre_tabla_en_db}, Filas: {len(df_to_save)}")
                    save_df_to_supabase(
                        df_to_save, nombre_tabla_en_db, engine, if_exists='replace')
                except Exception as e_save:
                    error_msg = f"--- Log Sherlock (BG Task): ERROR al guardar tabla '{nombre_tabla_en_db}': {str(e_save)}"
                    print(error_msg)
                    all_server_warnings_bg.append(error_msg)
            else:
                # ... (log de df vacío)
                pass

        print("--- Log Sherlock (BG Task): PROCESO DE GUARDADO EN SUPABASE COMPLETADO ---")
        if all_server_warnings_bg:
            print("--- Log Sherlock (BG Task): Advertencias durante la tarea de fondo:")
            for warn in all_server_warnings_bg:
                print(warn)

    except Exception as e:
        print(
            f"--- Log Sherlock (BG Task): ERROR CRÍTICO en tarea de fondo: {str(e)} ---")
        import traceback
        traceback.print_exc()
    finally:
        print("--- Log Sherlock (BG Task): FIN TAREA DE FONDO ---")


@app.post("/trigger_processing_and_save/")
# <--- AÑADE background_tasks
async def trigger_processing_and_save(background_tasks: BackgroundTasks):
    # Solo para verificar que existan antes de encolar
    global _saved_files, _indice_file_content, _indice_file_name

    print("--- Log Sherlock: RECIBIDA LLAMADA a /trigger_processing_and_save/ ---")

    if engine is None:
        # No limpiar globales aquí, la tarea de fondo no se ejecutará.
        raise HTTPException(
            status_code=500, detail="Error de configuración: Conexión a DB no disponible.")

    if not _indice_file_content or not _indice_file_name:
        # No limpiar globales aquí
        raise HTTPException(
            status_code=400, detail="Archivo índice ('indice.xlsx') no encontrado. Sube archivos primero.")

    if not _saved_files:
        # No limpiar globales aquí
        raise HTTPException(
            status_code=400, detail="No hay archivos de datos en memoria. Sube archivos primero.")

    # Añade la función de trabajo pesado a las tareas de fondo
    # La tarea comenzará DESPUÉS de que esta función de endpoint devuelva la respuesta.
    background_tasks.add_task(do_the_heavy_processing_and_saving)

    print("--- Log Sherlock: Tarea de procesamiento encolada en segundo plano. Devolviendo 202 a Make. ---")
    return JSONResponse(
        # 202 Accepted: La solicitud ha sido aceptada para procesamiento, pero el procesamiento no ha terminado.
        status_code=202,
        content={
            "message": "Solicitud de procesamiento recibida. El trabajo se está realizando en segundo plano."}
    )

# ... (tu endpoint /ask_sherlock/ se mantiene igual) ...

# (Opcional) Endpoint de "reset_memory" para depuración


@app.get("/admin/reset_memory/")
async def reset_memory_admin():
    global _saved_files, _indice_file_content, _indice_file_name
    _saved_files.clear()
    _indice_file_content = None
    _indice_file_name = None
    msg = "--- Log Sherlock: Memoria temporal de archivos limpiada manualmente por admin ---"
    print(msg)
    return {"message": msg}


@app.post("/ask_sherlock/")
async def ask_sherlock(request: Request):
    form_data = await request.form()
    user_question = form_data.get("text")
    if not user_question:
        return JSONResponse({"text": "Lo siento, no recibí tu pregunta. Por favor, intenta de nuevo."}, status_code=200)
    make_webhook_url = os.environ.get("MAKE_SHERLOCK_WEBHOOK_URL")
    if not make_webhook_url:
        print("Error: MAKE_SHERLOCK_WEBHOOK_URL no configurada.")
        return JSONResponse({"text": "Sherlock no está configurado."}, status_code=500)
    try:
        payload_to_make = {"user_question": user_question, "user_id": form_data.get(
            "user_id"), "channel_id": form_data.get("channel_id")}
        response_from_make = requests.post(
            make_webhook_url, json=payload_to_make)
        response_from_make.raise_for_status()
        return JSONResponse({"text": f"Pregunta recibida: '{user_question}'. Sherlock procesando..."}, status_code=200)
    except requests.exceptions.RequestException as e:
        print(f"Error al llamar al webhook de Make: {e}")
        return JSONResponse({"text": "Error al enviar tu pregunta a Sherlock."}, status_code=500)
    except Exception as e:
        print(f"Error inesperado en ask_sherlock: {e}")
        return JSONResponse({"text": "Ocurrió un error inesperado con Sherlock."}, status_code=500)

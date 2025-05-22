from fastapi import FastAPI, Request, File, UploadFile, HTTPException, Form
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


@app.post("/trigger_processing_and_save/")
async def trigger_processing_and_save():
    global _saved_files, _indice_file_content, _indice_file_name, engine
    all_server_warnings = []

    if engine is None:
        _saved_files.clear()
        _indice_file_content = None
        _indice_file_name = None
        print("Error crítico: El engine de la base de datos no está inicializado. Limpiando archivos en memoria.")
        raise HTTPException(
            status_code=500, detail="Error de configuración del servidor: Conexión a la base de datos no disponible.")

    # CAMBIO AQUÍ: Mensaje de error actualizado para "indice.xlsx"
    if not _indice_file_content or not _indice_file_name:
        _saved_files.clear()
        _indice_file_content = None
        _indice_file_name = None
        raise HTTPException(
            status_code=400, detail="Archivo índice ('indice.xlsx') no encontrado en memoria. Asegúrate de que se haya subido.")

    if not _saved_files:
        _saved_files.clear()
        _indice_file_content = None
        _indice_file_name = None
        raise HTTPException(
            status_code=400, detail="No hay archivos de datos en memoria para procesar.")

    try:
        print(
            f"Iniciando procesamiento. Archivo índice: '{_indice_file_name}'. Archivos de datos en memoria: {len(_saved_files)}")
        data_files_simulated: List[InMemoryUploadFile] = [InMemoryUploadFile(
            fname, fcontent) for fname, fcontent in _saved_files.items()]
        indice_file_simulated = InMemoryUploadFile(
            filename=_indice_file_name, content=_indice_file_content)

        processed_dfs_map, _, _, carga_warnings = procesador_datos.load_dataframes_from_uploads(
            data_files=data_files_simulated,  # type: ignore
            index_file=indice_file_simulated  # type: ignore
        )
        all_server_warnings.extend(carga_warnings)
        if not processed_dfs_map:
            _saved_files.clear()
            _indice_file_content = None
            _indice_file_name = None
            raise HTTPException(
                status_code=400, detail=f"No se pudieron cargar los DataFrames desde memoria. Advertencias: {carga_warnings}")

        print("DataFrames cargados y limpiados preliminarmente desde memoria.")
        final_dataframes_to_save = procesador_datos.generar_insights_pacientes(
            processed_dfs_map, all_server_warnings)
        if not final_dataframes_to_save:
            _saved_files.clear()
            _indice_file_content = None
            _indice_file_name = None
            raise HTTPException(
                status_code=400, detail=f"No se pudieron generar los DataFrames de insights. Advertencias: {all_server_warnings}")

        print(
            f"Insights generados. DataFrames listos para guardar: {list(final_dataframes_to_save.keys())}")
        saved_tables_summary = {}
        failed_tables_summary = {}
        for table_name_key, df_to_save in final_dataframes_to_save.items():
            nombre_tabla_en_db = table_name_key.lower()
            if df_to_save is not None and not df_to_save.empty:
                try:
                    save_df_to_supabase(
                        df_to_save, nombre_tabla_en_db, engine, if_exists='replace')
                    saved_tables_summary[
                        nombre_tabla_en_db] = f"Guardado exitoso ({len(df_to_save)} filas)"
                except Exception as e_save:
                    error_msg = f"Error al guardar tabla '{nombre_tabla_en_db}': {str(e_save)}"
                    print(error_msg)
                    all_server_warnings.append(error_msg)
                    failed_tables_summary[nombre_tabla_en_db] = str(e_save)
            else:
                msg = f"DataFrame para '{nombre_tabla_en_db}' está vacío o es None, no se guardará."
                print(msg)
                all_server_warnings.append(msg)

        if not saved_tables_summary and final_dataframes_to_save and any(not df.empty for df in final_dataframes_to_save.values()):
            raise HTTPException(
                status_code=500, detail=f"Se generaron DataFrames pero ninguno pudo ser guardado en Supabase. Errores: {failed_tables_summary}. Advertencias: {all_server_warnings}")

        print("Proceso de guardado en Supabase completado.")
        _saved_files.clear()
        _indice_file_content = None
        _indice_file_name = None
        print("Almacenamiento temporal en memoria limpiado.")
        return JSONResponse(content={
            "message": "Archivos en memoria procesados y datos guardados (o intentado guardar) en Supabase.",
            "tables_successfully_processed_and_saved": list(saved_tables_summary.keys()),
            "tables_with_save_errors": failed_tables_summary,
            "processed_dataframes_generated": list(final_dataframes_to_save.keys()),
            "server_warnings_and_errors": all_server_warnings
        }, status_code=200)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        _saved_files.clear()
        _indice_file_content = None
        _indice_file_name = None
        print(
            f"Error crítico durante el procesamiento de archivos en memoria: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Error interno del servidor al procesar archivos en memoria: {str(e)}")


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

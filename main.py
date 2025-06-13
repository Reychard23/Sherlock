
import json
from fastapi import FastAPI, Request, File, UploadFile, HTTPException, Form, BackgroundTasks, Body
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Tuple, Literal
import pandas as pd
import numpy as np
import os
import io
import requests
from sqlalchemy import create_engine, text
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

# --- Función para guardar DataFrame en Supabase ---


def save_df_to_supabase(
    df: pd.DataFrame, table_name: str, db_engine: Any,
    if_exists: Literal['fail', 'replace', 'append'] = 'replace'
):
    if db_engine is None:
        raise ConnectionError("Conexión a la base de datos no establecida.")
    if df.empty:
        print(
            f"ADVERTENCIA: DataFrame para tabla '{table_name}' vacío. No se guardará.")
        return
    try:
        print(
            f"--- Log Sherlock (BG Task - save_df): Guardando tabla: {table_name}, Filas: {len(df)}")
        df.to_sql(table_name, db_engine, if_exists=if_exists,
                  index=False, chunksize=1000, method='multi')
        print(
            f"--- Log Sherlock (BG Task - save_df): Tabla '{table_name}' guardada exitosamente.")
    except Exception as e:
        print(
            f"--- Log Sherlock (BG Task - save_df): ERROR al guardar tabla '{table_name}': {e}")
        raise

# --- Endpoint 1: Recibir Archivos Individualmente ---


@app.post("/upload_single_file/")
async def upload_single_file(file: UploadFile = File(...), filename: str = Form(...)):
    global _indice_file_content, _indice_file_name
    try:
        content = await file.read()
        if filename.lower() == "indice.xlsx":
            _indice_file_content = content
            _indice_file_name = filename
            print(f"Archivo índice '{filename}' almacenado en memoria.")
        else:
            _saved_files[filename] = content
            print(
                f"Archivo de datos '{filename}' almacenado. Total datos: {len(_saved_files)}")
        return {"filename": filename, "message": "Archivo almacenado temporalmente."}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al procesar archivo {filename}: {str(e)}")

# --- Clase Helper para Simular UploadFile ---


class InMemoryUploadFile:
    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self.file = io.BytesIO(content)

    async def read(self) -> bytes: self.file.seek(0); return self.file.read()
    def seek(self, offset: int,
             whence: int = 0) -> int: return self.file.seek(offset, whence)

    def tell(self) -> int: return self.file.tell()
    async def close(self): self.file.close()

# --- Función de Tarea de Fondo ---


async def do_the_heavy_processing_and_saving():
    global _saved_files, _indice_file_content, _indice_file_name, engine
    print("--- Log Sherlock (BG Task): INICIO TAREA DE FONDO ---")

    current_saved_files = _saved_files.copy()
    current_indice_content = _indice_file_content
    current_indice_name = _indice_file_name

    _saved_files.clear()
    _indice_file_content = None
    _indice_file_name = None
    print("--- Log Sherlock (BG Task): Variables globales de archivos limpiadas para próximo lote.")

    if engine is None or not current_saved_files:
        print("--- Log Sherlock (BG Task): ERROR - Faltan datos esenciales (engine o archivos) para iniciar la tarea.")
        return

    try:
        if not current_indice_name or not current_indice_content:
            print("--- Log Sherlock (BG Task): ERROR CRÍTICO - No se encontró el nombre o el contenido del archivo 'indice.xlsx'. Abortando tarea.")
            return

        data_files_sim = [InMemoryUploadFile(
            fn, fc) for fn, fc in current_saved_files.items()]

        indice_file_sim = InMemoryUploadFile(
            filename=current_indice_name, content=current_indice_content)

        print("--- Log Sherlock (BG Task): Llamando a load_dataframes_from_uploads...")
        processed_dfs, _, _, carga_warnings = procesador_datos.load_dataframes_from_uploads(
            data_files=data_files_sim, index_file=indice_file_sim)
        if not processed_dfs:
            print(
                f"--- Log Sherlock (BG Task): ERROR - No se cargaron DataFrames. Advertencias: {carga_warnings}")
            return

        print("--- Log Sherlock (BG Task): Llamando a generar_insights_pacientes...")
        final_dataframes_to_save = procesador_datos.generar_insights_pacientes(
            processed_dfs, carga_warnings)
        if not final_dataframes_to_save:
            print(
                f"--- Log Sherlock (BG Task): ERROR - No se generaron DataFrames finales para guardar. Advertencias: {carga_warnings}")
            return

        print(
            f"--- Log Sherlock (BG Task): DataFrames para guardar en Supabase: {list(final_dataframes_to_save.keys())}")
        for table_name, df_to_save in final_dataframes_to_save.items():
            save_df_to_supabase(df_to_save, table_name,
                                engine, if_exists='replace')

        print("--- Log Sherlock (BG Task): PROCESO COMPLETO DE GUARDADO EN SUPABASE TERMINADO ---")
        if carga_warnings:
            print("--- Log Sherlock (BG Task): Resumen de Advertencias ---")
            for warn in carga_warnings:
                print(warn)

    except Exception as e:
        print(
            f"--- Log Sherlock (BG Task): ERROR CRÍTICO en tarea de fondo: {str(e)} ---")
        import traceback
        traceback.print_exc()
    finally:
        print("--- Log Sherlock (BG Task): FIN TAREA DE FONDO ---")


# --- Endpoint 2: Disparador de la Tarea de Fondo ---
@app.post("/trigger_processing_and_save/")
async def trigger_processing_and_save(background_tasks: BackgroundTasks):
    print("--- Log Sherlock: RECIBIDA LLAMADA a /trigger_processing_and_save/ ---")
    if engine is None:
        raise HTTPException(
            status_code=500, detail="Error: Conexión a DB no disponible.")
    if not _indice_file_content:
        raise HTTPException(
            status_code=400, detail="Error: Falta 'indice.xlsx'.")
    if not _saved_files:
        raise HTTPException(
            status_code=400, detail="Error: Faltan archivos de datos.")

    background_tasks.add_task(do_the_heavy_processing_and_saving)
    print("--- Log Sherlock: Tarea de procesamiento encolada. Devolviendo 202 a Make. ---")
    return JSONResponse(status_code=202, content={"message": "Solicitud de procesamiento recibida. El trabajo se realiza en segundo plano."})

# --- Endpoint 3: Ejecutor de SQL para tu "Playground" Manual ---


@app.post("/execute_sql_query/")
async def execute_sql(sql_query: str = Body(..., embed=True)):
    if engine is None:
        raise HTTPException(
            status_code=500, detail="Conexión a DB no disponible.")
    print(
        f"--- Log Sherlock (SQL): Recibida query SQL para ejecutar: {sql_query[:500]}...")
    try:
        with engine.connect() as connection:
            result = connection.execute(text(sql_query))
            if result.returns_rows:

                df_result = pd.DataFrame(
                    result.fetchall(), columns=list(result.keys()))
                json_result = df_result.to_json(
                    orient='records', date_format='iso')
                print(
                    f"--- Log Sherlock (SQL): Query ejecutada. Filas devueltas: {len(df_result)}")

                return JSONResponse(content=json.loads(json_result))
            else:
                msg = f"Query ejecutada (sin filas devueltas). Filas afectadas (aprox): {result.rowcount}"
                print(f"--- Log Sherlock (SQL): {msg}")
                return {"status": "success", "message": msg}
    except SQLAlchemyError as e_sql:
        print(f"--- Log Sherlock (SQL): ERROR SQLAlchemy: {e_sql}")
        raise HTTPException(
            status_code=400, detail=f"Error de base de datos al ejecutar SQL: {str(e_sql)}")
    except Exception as e:
        print(f"--- Log Sherlock (SQL): ERROR Inesperado: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail="Error inesperado en el servidor al ejecutar SQL.")

# --- Endpoint 4: Para que Render no se queje ---


@app.get("/")
async def root():
    return {"message": "Sherlock API V2 está activo y escuchando."}

# --- Endpoint 5: Para limpiar memoria durante pruebas ---


@app.get("/admin/reset_memory/")
async def reset_memory_admin():
    global _saved_files, _indice_file_content, _indice_file_name
    _saved_files.clear()
    _indice_file_content = None
    _indice_file_name = None
    msg = "--- Log Sherlock: Memoria temporal de archivos limpiada manualmente por admin ---"
    print(msg)
    return {"message": msg}

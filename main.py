# main.py

import pandas as pd
import io
import os
import requests
import json
import psycopg2
import psycopg2.extras
import zipfile
from fastapi import FastAPI, Request, File, UploadFile, Form
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Tuple
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv  # Importar load_dotenv

# Importar la función de procesamiento de datos
from data_processing import run_data_processing

# Cargar variables de entorno desde .env
# Esto es crucial para el desarrollo local y para que Render las cargue automáticamente.
load_dotenv()

app = FastAPI()

# Esto ya no es estrictamente necesario si los archivos se procesan y luego se eliminan.
# Lo mantengo por si acaso tienes alguna otra necesidad de guardar archivos raw.
_saved_files: dict[str, bytes] = {}

# === Configuración de la conexión a la base de datos ===
# Leer la URL de conexión desde las variables de entorno (.env local y Render)
# Ejemplo de formato DATABASE_URL para Supabase:
# postgresql://postgres:[YOUR_PASSWORD]@db.[YOUR_PROJECT_REF].supabase.co:5432/postgres
DATABASE_URL = os.environ.get("DATABASE_URL")

# Validar si la URL de la base de datos está configurada
if not DATABASE_URL:
    print("ADVERTENCIA: La variable de entorno DATABASE_URL no está configurada. Las operaciones de base de datos no funcionarán.")
    # En un entorno de producción, podrías querer lanzar una excepción aquí
    # o hacer que la app no inicie si la DB es crítica.
    # raise ValueError("DATABASE_URL no está configurada. No se puede iniciar la aplicación.")

# Función para obtener un motor de SQLAlchemy para la conexión a la base de datos


def get_db_engine():
    if not DATABASE_URL:
        # Aquí se lanza una excepción si la URL no está configurada,
        # lo que evitará errores posteriores si se intenta usar el motor.
        raise ValueError(
            "DATABASE_URL no está configurada. No se puede conectar a la base de datos.")
    try:
        # SQLAlchemy usa un formato diferente para psycopg2, a veces necesita el prefijo.
        # Si DATABASE_URL ya viene con 'postgresql://', no es necesario cambiarlo.
        # create_engine puede manejarlo.
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        print(f"Error al crear el motor de base de datos: {e}")
        raise  # Re-lanzar la excepción para que se maneje aguas arriba

# Nueva función para guardar un DataFrame en Supabase


def save_dataframe_to_supabase(df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> bool:
    """
    Guarda un DataFrame de Pandas en una tabla de Supabase (PostgreSQL).

    Args:
        df: El DataFrame de Pandas a guardar.
        table_name: El nombre de la tabla en Supabase.
        if_exists: Comportamiento si la tabla ya existe ('fail', 'replace', 'append').
                   'replace' eliminará la tabla y la creará de nuevo.
                   'append' añadirá nuevas filas.

    Returns:
        True si la operación fue exitosa, False en caso contrario.
    """
    if df.empty:
        print(
            f"ADVERTENCIA: DataFrame '{table_name}' está vacío. No se guardará en Supabase.")
        # Consideramos que no guardar un DF vacío es un éxito si no hay datos.
        return True

    if not DATABASE_URL:
        print(
            f"ERROR: DATABASE_URL no configurada para guardar '{table_name}'.")
        return False  # No podemos guardar sin la URL de la DB

    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            print(
                f"Intentando guardar DataFrame '{table_name}' en Supabase con if_exists='{if_exists}'...")
            # to_sql es una función poderosa de Pandas para escribir DataFrames a bases de datos SQL
            # index=False: No escribir el índice del DataFrame como una columna en la tabla.
            # chunksize=1000: Insertar en lotes de 1000 filas, mejor para DataFrames grandes.
            df.to_sql(table_name, con=connection,
                      if_exists=if_exists, index=False, chunksize=1000)
            print(
                f"DataFrame '{table_name}' guardado exitosamente en Supabase.")
            return True
    except SQLAlchemyError as e:
        print(
            f"ERROR SQLAlchemy al guardar DataFrame '{table_name}' en Supabase: {e}")
        return False
    except Exception as e:
        print(
            f"ERROR inesperado al guardar DataFrame '{table_name}' en Supabase: {e}")
        return False

# Endpoint de prueba para verificar que el servicio está funcionando


@app.get("/")
async def read_root():
    return {"message": "Bienvenido al Asistente Sherlock de Dentalink"}

# Endpoint para recibir y procesar los archivos de datos


@app.post("/upload-data/")
async def upload_data(
    # Recibe múltiples archivos Excel
    excel_files: List[UploadFile] = File(...),
    indice_file: UploadFile = File(...)      # Recibe el archivo índice
):
    all_advertencias: List[str] = []
    processed_dfs: Dict[str, pd.DataFrame] = {}
    index_file_path = "temp_indice.csv"  # Nombre temporal para el archivo índice
    temp_data_folder = "temp_excel_data"  # Carpeta temporal para los archivos Excel

    try:
        # Asegurarse de que el directorio temporal exista
        os.makedirs(temp_data_folder, exist_ok=True)

        # 1. Guardar el archivo índice temporalmente
        # El contenido del archivo se lee directamente del stream de subida
        with open(index_file_path, "wb") as buffer:
            buffer.write(indice_file.file.read())
        print(
            f"Archivo índice '{indice_file.filename}' guardado temporalmente en: {index_file_path}")

        # 2. Guardar los archivos Excel temporalmente
        excel_filepaths = []
        for excel_file in excel_files:
            file_path = os.path.join(temp_data_folder, excel_file.filename)
            with open(file_path, "wb") as buffer:
                buffer.write(excel_file.file.read())
            excel_filepaths.append(file_path)
            print(
                f"Archivo Excel '{excel_file.filename}' guardado temporalmente en: {file_path}")

        # 3. Ejecutar la lógica de procesamiento de datos
        # Se pasa la ruta de la carpeta donde están los excels y la ruta del índice.
        # run_data_processing devolverá un diccionario de DataFrames procesados
        # y una lista de advertencias.
        processed_dfs, run_warnings = run_data_processing(
            temp_data_folder, index_file_path)
        # Añadir las advertencias al listado general
        all_advertencias.extend(run_warnings)

        # 4. === LÓGICA PARA GUARDAR EN SUPABASE ===
        supabase_save_success = True

        # Guarda el DataFrame de análisis final
        df_analisis_final = processed_dfs.get('df_analisis_final')
        if df_analisis_final is not None and not df_analisis_final.empty:
            # La tabla 'analisis_atenciones' contendrá el DataFrame consolidado
            # 'replace' es ideal para actualizaciones diarias donde los datos del día anterior no son necesarios.
            if not save_dataframe_to_supabase(df_analisis_final, 'analisis_atenciones', if_exists='replace'):
                supabase_save_success = False
                all_advertencias.append(
                    "ERROR: Falló el guardado de 'analisis_atenciones' en Supabase.")
        else:
            all_advertencias.append(
                "ADVERTENCIA: 'df_analisis_final' está vacío o no se encontró. No se guardará en Supabase.")

        # Guarda los DataFrames de perfil de pacientes nuevos
        # Iteramos sobre todos los DataFrames que empiezan con 'Perfil_Pacientes_Nuevos_Atendidos_Por_'
        for df_name, df in processed_dfs.items():
            if df_name.startswith('Perfil_Pacientes_Nuevos_Atendidos_Por_'):
                # Generamos un nombre de tabla limpio y en minúsculas para Supabase.
                # Reemplazamos espacios, tildes y el prefijo largo.
                table_name = df_name.lower().replace(
                    'perfil_pacientes_nuevos_atendidos_por_', 'perfil_nuevos_')
                table_name = table_name.replace(' ', '_').replace('ó', 'o').replace(
                    'á', 'a').replace('é', 'e').replace('í', 'i').replace('ú', 'u')

                if not save_dataframe_to_supabase(df, table_name, if_exists='replace'):
                    supabase_save_success = False
                    all_advertencias.append(
                        f"ERROR: Falló el guardado de '{df_name}' en Supabase.")
        # =======================================

    except Exception as e:
        # Captura cualquier excepción que ocurra durante el proceso
        error_message = f"ERROR INTERNO DEL SERVIDOR en /upload-data/: {e}"
        all_advertencias.append(error_message)
        print(error_message)  # Imprimir el error en los logs de Render
        # Asegurar que se limpian los archivos temporales incluso si hay un error
        supabase_save_success = False  # Marcar como fallo general

    finally:
        # Este bloque 'finally' se ejecuta siempre, haya o no un error.
        # Es crucial para limpiar los archivos temporales.
        print("Iniciando limpieza de archivos temporales...")
        if os.path.exists(index_file_path):
            try:
                os.remove(index_file_path)
                print(f"Archivo temporal eliminado: {index_file_path}")
            except OSError as e:
                print(
                    f"Error al eliminar archivo temporal {index_file_path}: {e}")
                all_advertencias.append(
                    f"ADVERTENCIA: No se pudo eliminar archivo temporal {index_file_path}.")

        if os.path.exists(temp_data_folder):
            try:
                # Eliminar todos los archivos dentro de la carpeta temporal
                for f_name in os.listdir(temp_data_folder):
                    f_path = os.path.join(temp_data_folder, f_name)
                    if os.path.isfile(f_path):  # Asegurarse de que es un archivo
                        os.remove(f_path)
                        print(f"Archivo temporal eliminado: {f_path}")
                # Después de eliminar los archivos, eliminar la carpeta
                os.rmdir(temp_data_folder)
                print(f"Carpeta temporal eliminada: {temp_data_folder}")
            except OSError as e:
                print(
                    f"Error al eliminar carpeta temporal {temp_data_folder}: {e}")
                all_advertencias.append(
                    f"ADVERTENCIA: No se pudo eliminar carpeta temporal {temp_data_folder}.")

        # Preparar la respuesta JSON final
        # Si hubo algún fallo en el guardado de Supabase o un error general,
        # el status code de la respuesta será 500 (Internal Server Error).
        # De lo contrario, será 200 (OK).
        response_status_code = 200 if supabase_save_success else 500
        response_message = "Archivos procesados exitosamente."
        if not supabase_save_success:
            response_message += " Sin embargo, hubo errores en el procesamiento o al guardar DataFrames en Supabase."

        return JSONResponse({
            "message": response_message,
            # Nombres de los DFs que se intentaron procesar
            "processed_dataframes": list(processed_dfs.keys()),
            "warnings": all_advertencias
        }, status_code=response_status_code)


# Endpoint para el webhook de Make (sin cambios relevantes para este paso)
@app.post("/ask-sherlock/")
async def ask_sherlock(request: Request):
    try:
        form_data = await request.json()
        user_question = form_data.get("user_question")
    except json.JSONDecodeError:
        return JSONResponse({"text": "Solicitud JSON inválida."}, status_code=400)

    if not user_question:
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

        # Lanza un error para códigos de respuesta HTTP 4xx/5xx
        response_from_make.raise_for_status()

        return JSONResponse({"text": f"Pregunta recibida: '{user_question}'. Sherlock está procesando. Un momento por favor..."}, status_code=200)

    except requests.exceptions.RequestException as e:
        print(f"Error al llamar al webhook de Make: {e}")
        return JSONResponse({"text": "Ocurrió un error al enviar tu pregunta a Sherlock para procesamiento. Por favor contacta a Soporte."}, status_code=500)
    except Exception as e:
        print(f"Un error inesperado ocurrió en /ask-sherlock: {e}")
        return JSONResponse({"text": "Ocurrió un error inesperado al procesar tu pregunta. Por favor contacta a Soporte."}, status_code=500)

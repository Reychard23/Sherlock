from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import JSONResponse
from typing import List, Dict, Any
from typing import Dict, Any
import pandas as pd
import io
from datetime import date

app = FastAPI()
# Almacena los archivos raw (como los sube Make)
_saved_files: dict[str, bytes] = {}
# Almacena los DataFrames de Pandas procesados y limpios
_processed_dfs: dict[str, pd.DataFrame] = {}


def calcular_pacientes_nuevos_atendidos(dataframes: Dict[str, pd.DataFrame]) -> int:
    """
    Calcula el número total de pacientes nuevos atendidos.
    ... (el código completo de la función que te pasé antes) ...
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
    # 1) Leer el índice desde memoria
    raw_index = _saved_files.get("indice.xlsx")
    if raw_index is None:
        return JSONResponse({"error": "No se subió indice.xlsx"}, status_code=400)
    try:
        indice_df = pd.read_excel(io.BytesIO(raw_index))
    except Exception as e:
        return JSONResponse({"error": f"Error al leer indice.xlsx: {str(e)}"}, status_code=400)

    # 2) Construir rename_dict y drop_set
    rename_dict: dict[tuple[str, str], dict[str, str]] = {}
    drop_set: set[tuple[str, str, str]] = set()

    # Validar que las columnas esperadas estén en el índice
    required_cols = ["Archivo", "Sheet",
                     "Columna", "Nombre unificado", "Acción"]
    if not all(col in indice_df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in indice_df.columns]
        return JSONResponse({"error": f"Columnas requeridas faltantes en indice.xlsx: {missing}"}, status_code=400)

    for _, row in indice_df.iterrows():
        # Agregar manejo de errores en la lectura de filas del índice si alguna celda está vacía inesperadamente
        try:
            archivo = str(row["Archivo"]).strip()
            hoja = str(row["Sheet"]).strip()
            orig = str(row["Columna"]).strip()
            nuevo = str(row["Nombre unificado"]).strip()
            accion = str(row["Acción"]).strip().lower()

            key = (archivo, hoja)
            if accion == "drop":
                drop_set.add((archivo, hoja, orig))
            elif accion == "keep":  # Asegurarnos de que solo 'keep' renombra
                # Inicializar el diccionario interior si no existe
                if key not in rename_dict:
                    rename_dict[key] = {}
                rename_dict[key][orig] = nuevo
            # Ignorar otras acciones no reconocidas en el índice

        except Exception as e:
            return JSONResponse({"error": f"Error procesando fila en indice.xlsx: {row.to_dict()} - {str(e)}"}, status_code=400)

    # 3) Validar contra la lista esperada de archivos (Puedes mantener esta lista o hacerla dinámica desde el índice si prefieres)
    # expected = {archivo for (archivo, _) in rename_dict.keys()} # Opción: derivar esperados del índice
    expected = {  # Lista hardcodeada que tenías, es válida si es fija
        "Pacientes_Nuevos.xlsx",
        "Acciones.xlsx",
        "Citas_Motivo.xlsx",
        "Citas_Pacientes.xlsx",
        "Presupuesto por Accion.xlsx",
        "Respuesta_Encuestas.xlsx",
        "Tratamiento Generado Mex.xlsx",
        "Tabla Gastos Aliadas Mexico.xlsx",
        "Movimiento.xlsx",
        "Sucursal.xlsx",
        "Tabla_Procedimientos.xlsx",
        "Tipos de pacientes.xlsx",
    }
    uploaded = set(_saved_files.keys()) - {"indice.xlsx"}
    # Ajustar indexed para que solo use archivos presentes en la lista 'expected' si usas la lista fija
    indexed = {archivo for (archivo, _)
               in rename_dict.keys() if archivo in expected}

    faltan = expected - uploaded
    sobran = uploaded - expected
    # no_index = (uploaded & expected) - indexed # Lógica anterior, puede ser confusa
    # Nueva lógica: archivos subidos que están en la lista expected pero no tienen entradas 'keep' en el índice
    no_index = {archivo for archivo in (
        uploaded & expected) if archivo not in indexed}

    if faltan:
        return JSONResponse(
            {"error": f"No se subieron estos archivos esperados: {sorted(faltan)}"},
            status_code=400
        )
    if sobran:
        # Advertencia en lugar de error fatal si suben archivos extra? Depende de si quieres ser estricto.
        # Por ahora, lo mantenemos como error.
        return JSONResponse(
            {"error": f"Se subieron archivos no contemplados: {sorted(sobran)}"},
            status_code=400
        )
    if no_index:
        # Asegurarnos de que todos los archivos esperados y subidos tengan al menos una entrada 'keep' en el índice
        return JSONResponse(
            {"error": f"Faltan entradas 'keep' en el índice para estos archivos subidos: {sorted(no_index)}. Asegúrate de que cada archivo esperado tenga al menos una columna marcada como 'Keep'."},
            status_code=400
        )

    # Limpiar DataFrames procesados anteriores antes de empezar
    _processed_dfs.clear()
    resultados = []
    errores_procesamiento = []

    # 4) Procesar cada (archivo, hoja) según rename_dict y drop_set
    # Iterar sobre las combinaciones archivo/hoja definidas en el índice
    for (archivo, hoja), mapeo in rename_dict.items():
        raw = _saved_files.get(archivo)
        # Esta validación ya se hizo arriba, pero no está de más
        if raw is None:
            # Este caso no debería ocurrir si las validaciones previas fueron correctas,
            # pero lo dejamos como salvaguarda.
            errores_procesamiento.append({
                "archivo": archivo,
                "hoja":     hoja,
                "error":   "Archivo no subido (error de lógica interna o validación previa fallida)"
            })
            continue

        try:
            # Leer solo la hoja especificada
            df = pd.read_excel(io.BytesIO(raw), sheet_name=hoja)

            # Drop de columnas
            to_drop = [col for (a, h, col) in drop_set if a ==
                       archivo and h == hoja]
            # Solo intentar dropear si hay columnas para dropear Y si existen en el DF
            cols_to_actually_drop = [
                col for col in to_drop if col in df.columns]
            if cols_to_actually_drop:
                # Quitamos errors="ignore" para ser más estrictos si la columna a dropear del índice no existe en el archivo/hoja
                df = df.drop(columns=cols_to_actually_drop)

            # Rename de columnas keep
            # Crear un mapeo que solo contenga columnas que realmente existen en el DF
            actual_mapeo = {orig_col: new_col for orig_col,
                            new_col in mapeo.items() if orig_col in df.columns}
            if actual_mapeo:
                df.rename(columns=actual_mapeo, inplace=True)

            # ===>>> PASO CLAVE: GUARDAR EL DATAFRAME PROCESADO <<<===
            # Usamos el nombre del archivo (sin la extensión .xlsx) como clave, o una combinación si es necesario
            # Si un archivo tiene varias hojas que se procesan, podríamos usar una clave como "nombre_archivo_nombre_hoja"
            # Por ahora, si cada archivo tiene solo una hoja relevante (según tu índice), el nombre del archivo es suficiente.
            # Si un archivo tuviera múltiples hojas relevantes, tendrías que ajustar esto para evitar sobrescribir.
            # Asumiendo 1 hoja relevante por archivo por ahora, usamos el nombre base:
            base_filename = archivo.replace(".xlsx", "")
            # Si el índice indica que un archivo tiene múltiples hojas relevantes, necesitarás una clave única.
            # Ejemplo: _processed_dfs[f"{base_filename}_{hoja}"] = df
            # Si estás seguro que cada archivo subido solo tiene UNA hoja relevante según tu índice:
            _processed_dfs[base_filename] = df

            resultados.append({
                "archivo":   archivo,
                "hoja":      hoja,
                "status":   "Procesado exitosamente",
                "columns":   df.columns.tolist(),  # Mostrar columnas después de drop/rename
                "row_count": len(df)
            })
        except Exception as e:
            # Capturar errores específicos de pandas si es posible, o dejar Exception general
            errores_procesamiento.append({
                "archivo": archivo,
                "hoja":    hoja,
                "error":   f"Error durante el procesamiento con Pandas: {str(e)}"
            })

     # 5) Devolver resultados (incluyendo errores de procesamiento si hubo)
    response_content: Dict[str, Any] = {"resultados": resultados}
    if errores_procesamiento:
        response_content["advertencias_o_errores_procesamiento"] = errores_procesamiento
        # Podrías cambiar el código de estado si consideras que un error de procesamiento es fatal
        # return JSONResponse(response_content, status_code=500)

    # En este punto, los DataFrames limpios están en _processed_dfs
    # Ejemplo de cómo ver qué DFs están disponibles (solo para depuración si quieres)
    # print("DataFrames procesados disponibles:", _processed_dfs.keys())

    return JSONResponse(response_content, status_code=200)


@app.post("/calculate-insights/")
async def calculate_insights_endpoint():
    """
    Endpoint para calcular los insights predefinidos.
    Requiere que los archivos hayan sido subidos y procesados previamente.
    """
    # Verificar si hay datos procesados disponibles en la variable global
    if not _processed_dfs:
        return JSONResponse({"error": "No hay datos procesados disponibles. Por favor, suba y procese los archivos primero."}, status_code=400)

    # === Calcular el primer insight llamando a la función ===
    # Le pasamos el diccionario _processed_dfs a la función para que acceda a los DataFrames
    pacientes_nuevos_atendidos = calcular_pacientes_nuevos_atendidos(
        _processed_dfs)

    # Aquí llamarías a otras funciones para calcular otros insights de manera similar:
    # otro_insight = calcular_otro_insight(_processed_dfs)
    # ...

    # Por ahora, solo devolvemos el resultado del primer insight
    # En el futuro, aquí guardarías esto en Airtable y prepararías la respuesta para Slack
    return JSONResponse({
        "status": "Insights calculados",
        "insight_pacientes_nuevos_atendidos": pacientes_nuevos_atendidos
        # Agregar otros insights aquí en la respuesta
    })


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

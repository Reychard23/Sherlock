from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import JSONResponse
from typing import List
import pandas as pd
import io

app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "Hola, reychard"}

@app.post("/upload-excel/")
async def upload_excel(files: List[UploadFile] = File(...)):
    resultados = []
    for file in files:
        try:
            data = await file.read()  
            df = pd.read_excel(io.BytesIO(data))
            resultados.append({
                "filename": file.filename,
                "columns": df.columns.tolist(),
                "row_count": len(df)
            })
        except Exception as e:
            # Capturamos el error y lo devolvemos para ese archivo
            resultados.append({
                "filename": file.filename,
                "error": str(e)
            })
    return {"resultados": resultados}

    try:
        indice_df = pd.read_excel("indice.xlsx")
    except Exception as e:
        return JSONResponse({"error": f"No se pudo leer el índice: {str(e)}"}, status_code=500)
    
    # Crear diccionario de mapeo: {archivo: {nombre_original: nombre_unificado, ...}, ...}
    mapping_dict = {}
    for _, row in indice_df.iterrows():
        archivo = row["Archivo"].strip()  # Asegúrate de que coincide con el nombre del archivo
        columna = row["Columna"].strip()
        descripcion = row["Descripción"].strip()
        if archivo not in mapping_dict:
            mapping_dict[archivo] = {}
        mapping_dict[archivo][columna] = descripcion
        for file in files:
        try:
            data = await file.read()
            # Leer el Excel en un DataFrame
            df = pd.read_excel(io.BytesIO(data))
            
            # Paso 2: Eliminar columnas innecesarias
            # Por ejemplo, si hay columnas redundantes en este DataFrame, podrías especificarlas:
            columnas_a_eliminar = ["Nombre", "Apellidos", "Fecha de nacimiento"]
            df = df.drop(columns=columnas_a_eliminar, errors='ignore')
            
            # Paso 3: Renombrar las columnas usando el mapeo, si existe para este archivo
            mapeo = mapping_dict.get(file.filename, {})
            if mapeo:
                df.rename(columns=mapeo, inplace=True)


@app.post("/slack")
async def slack_command(request: Request):
    form_data = await request.form()
    print("Payload recibido:", form_data)
    return JSONResponse({"text": "Hola, yo soy Sherlock, y pronto estaré disponible para analizar tus datos"}, status_code=200)


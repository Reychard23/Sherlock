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
    # Lee el archivo índice para el mapeo (suponiendo que "índice.xlsx" esté en un lugar accesible)
    indice_df = pd.read_excel("indice.xlsx")
    mapping_dict = {}
    for _, row in indice_df.iterrows():
        archivo = row["Archivo"].strip()
        columna = row["Columna"].strip()
        descripcion = row["Descripción"].strip()
        if archivo not in mapping_dict:
            mapping_dict[archivo] = {}
        mapping_dict[archivo][columna] = descripcion

    for file in files:
        try:
            data = await file.read()  
            df = pd.read_excel(io.BytesIO(data))
            
            # Si existe un mapeo para este archivo, renombrar las columnas
            mapeo = mapping_dict.get(file.filename, {})
            if mapeo:
                df.rename(columns=mapeo, inplace=True)
            
            # Realiza el análisis o cruza datos según sea necesario
            # Aquí puedes aplicar otros filtros o merges si tienes más DataFrames
            resumen = {
                "filename": file.filename,
                "row_count": len(df),
                "columns": df.columns.tolist(),
                # Puedes incluir resúmenes estadísticos o conteos de valores perdidos, etc.
            }
            resultados.append(resumen)
        except Exception as e:
            resultados.append({
                "filename": file.filename,
                "error": str(e)
            })
    return {"resultados": resultados}

@app.post("/slack")
async def slack_command(request: Request):
    form_data = await request.form()
    print("Payload recibido:", form_data)
    return JSONResponse(
        {"text": "Hola, esta es la respuesta de Sherlock"},
        status_code=200
    )
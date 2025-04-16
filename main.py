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
    # 1. Cargo el índice y construyo mapping_dict
    indice_df    = pd.read_excel("indice.xlsx")
    mapping_dict = {}
    for _, row in indice_df.iterrows():
        archivo     = row["Archivo"].strip()
        columna     = row["Columna"].strip()
        descripcion = row["Descripción"].strip()
        mapping_dict.setdefault(archivo, {})[columna] = descripcion

    resultados = []
    for file in files:
        try:
            # —————— Leer contenido y crear DataFrame (igual que antes)
            data = await file.read()  
            df   = pd.read_excel(io.BytesIO(data))

            # —————— Renombrado según mapeo (nuevo)
            mapeo = mapping_dict.get(file.filename, {})
            if mapeo:
                df.rename(columns=mapeo, inplace=True)

            # —————— Aquí vuelve tu append original
            resultados.append({
                "filename": file.filename,
                "columns":  df.columns.tolist(),
                "row_count": len(df)
            })
        except Exception as e:
            # Capturamos el error y lo devolvemos para ese archivo
            resultados.append({
                "filename": file.filename,
                "error":    str(e)
            })

    # —————— Devuelvo todos los resultados al final
    return {"resultados": resultados}

@app.post("/slack")
async def slack_command(request: Request):
    form_data = await request.form()
    print("Payload recibido:", form_data)
    return JSONResponse({"text": "Hola, yo soy Sherlock, y pronto estaré disponible para analizar tus datos"}, status_code=200)

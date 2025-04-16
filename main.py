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
            # Leer cada Excel y devolver nombre, columnas y número de filas
            data = await file.read()
            df = pd.read_excel(io.BytesIO(data))
            resultados.append({
                "filename":  file.filename,
                "columns":   df.columns.tolist(),
                "row_count": len(df)
            })
        except Exception as e:
            resultados.append({
                "filename": file.filename,
                "error":    str(e)
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
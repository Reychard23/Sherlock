from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import JSONResponse
import pandas as pd
import io


app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "Hola, reychard"}

from typing import List
@app.post("/upload-excel/")
async def upload_excel(files: List[UploadFile] = File(...)):
    # Procesar cada archivo
    resultados = []
    for file in files:
        # Procesa cada archivo y agrega resultados a la lista
        data = await file.read()  
        df = pd.read_excel(io.BytesIO(data))
        resultados.append({
            "filename": file.filename,
            "columns": df.columns.tolist(),
            "row_count": len(df)
        })
    return {"resultados": resultados}
@app.post("/slack")
async def slack_command(request: Request):
    # Obtener el payload del comando (lo puedes imprimir para depurar)
    form_data = await request.form()
    print("Payload recibido:", form_data)  # Esto se verá en los logs de Render
    # Responder con un mensaje simple en formato que Slack entienda
    return JSONResponse({"text": "Hola, yo soy Sherlock, y pronto estaré disponible para analizar tus datos"}, status_code=200)

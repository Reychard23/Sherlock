from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import JSONResponse
from typing import List
import pandas as pd
import io

app = FastAPI()

# Fuera de cualquier función, en el scope del módulo:
mapping_dict = {}      # Aquí guardaremos el mapeo una vez
mapping_loaded = False # Indicador de que ya cargamos el índice

@app.get("/")
async def read_root():
    return {"message": "Hola, reychard"}

@app.post("/upload-excel/")
async def upload_excel(files: List[UploadFile] = File(...)):
    # 1) Busco y leo indice.xlsx dentro de files
    index_file = next((f for f in files if f.filename.lower() == "indice.xlsx"), None)
    if not index_file:
        return JSONResponse(
            {"error": "No se encontró indice.xlsx entre los archivos subidos."},
            status_code=400
        )
    raw_index = await index_file.read()
    df_index  = pd.read_excel(io.BytesIO(raw_index))

    # 2) Construyo mapping_dict a partir del DataFrame del índice
    mapping_dict = { }
    for _, row in df_index.iterrows():
        arch   = row["Archivo"].strip()
        orig   = row["Columna"].strip()
        uni    = row["Descripción"].strip()
        mapping_dict.setdefault(arch, {})[orig] = uni

    # 3) Procesar todos los demás archivos
    resultados = []
    for f in files:
        if f is index_file:
            continue
        data = await f.read()
        df   = pd.read_excel(io.BytesIO(data))
        mapeo = mapping_dict.get(f.filename, {})
        if mapeo:
            df.rename(columns=mapeo, inplace=True)
        resultados.append({
            "filename":  f.filename,
            "columns":   df.columns.tolist(),
            "row_count": len(df)
        })

    return {"resultados": resultados}

@app.post("/slack")
async def slack_command(request: Request):
    form_data = await request.form()
    print("Payload recibido:", form_data)
    return JSONResponse({"text": "Hola, yo soy Sherlock, y pronto estaré disponible para analizar tus datos"}, status_code=200)

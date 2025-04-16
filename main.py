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
    # 1. Extraigo y leo índice.xlsx desde los UploadFile
    index_file = next((f for f in files if f.filename.lower() == "indice.xlsx"), None)
    if not index_file:
        return JSONResponse(
            {"error": "No se encontró indice.xlsx entre los archivos subidos."},
            status_code=400
        )
    raw_index = await index_file.read()
    indice_df  = pd.read_excel(io.BytesIO(raw_index))

    # 2. Construyo el mapping_dict
    mapping_dict = {}
    for _, row in indice_df.iterrows():
        arch   = row["Archivo"].strip()
        orig   = row["Columna"].strip()
        uni    = row["Descripción"].strip()
        mapping_dict.setdefault(arch, {})[orig] = uni

    # 3. Filtro los archivos de datos (todo excepto indice.xlsx)
    data_files = [f for f in files if f is not index_file]

    resultados = []
    for file in data_files:
        try:
            raw = await file.read()
            df  = pd.read_excel(io.BytesIO(raw))

            # 4. Aplico el renombrado si hay mapeo
            mapeo = mapping_dict.get(file.filename, {})
            if mapeo:
                df.rename(columns=mapeo, inplace=True)

            resultados.append({
                "filename": file.filename,
                "columns":  df.columns.tolist(),
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
    return JSONResponse({"text": "Hola, yo soy Sherlock, y pronto estaré disponible para analizar tus datos"}, status_code=200)

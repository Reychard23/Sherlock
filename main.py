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
    global mapping_dict, mapping_loaded

    # ——— 1. Si aún no cargamos el índice, búscalo y constrúyelo ———
    if not mapping_loaded:
        index_file = next((f for f in files if f.filename.lower() == "indice.xlsx"), None)
        if not index_file:
            return JSONResponse(
                {"error": "Primero debes subir indice.xlsx en un request aparte."},
                status_code=400
            )
        raw_index = await index_file.read()
        df_index  = pd.read_excel(io.BytesIO(raw_index))
        # Construimos el dict una sola vez
        for _, row in df_index.iterrows():
            arch   = row["Archivo"].strip()
            orig   = row["Columna"].strip()
            uni    = row["Descripción"].strip()
            mapping_dict.setdefault(arch, {})[orig] = uni
        mapping_loaded = True  # Marcamos que ya lo cargamos

    # ——— 2. Procesamos solo los archivos de datos (excluimos indice.xlsx) ———
    data_files = [f for f in files if f.filename.lower() != "indice.xlsx"]

    resultados = []
    for file in data_files:
        try:
            raw = await file.read()
            df  = pd.read_excel(io.BytesIO(raw))

            # ——— 3. Renombrado según el mapping ya cargado ———
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

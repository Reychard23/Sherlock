from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
from typing import List
import pandas as pd
import io

app = FastAPI()

# Almacenamiento temporal en memoria
_saved_files: dict[str, bytes] = {}

@app.get("/")
async def read_root():
    return {"message": "Hola, reychard"}

@app.post("/upload-excel/")
async def upload_file_endpoint(file: UploadFile = File(...)):
    """Recibe un solo archivo y lo almacena en _saved_files."""
    content = await file.read()
    _saved_files[file.filename] = content
    return {"status": "ok", "filename": file.filename}

@app.post("/process-excel/")
async def process_files_endpoint():
    """Procesa TODOS los archivos ya subidos."""
    # 1) Leer el índice
    raw_index = _saved_files.get("indice.xlsx")
    if raw_index is None:
        return JSONResponse({"error": "No se subió indice.xlsx"}, status_code=400)
    indice_df = pd.read_excel(io.BytesIO(raw_index))

    # 2) Construir mapping_dict
    mapping_dict = {}
    for _, row in indice_df.iterrows():
        arch = row["Archivo"].strip()
        orig = row["Columna"].strip()
        uni  = row["Descripción"].strip()
        mapping_dict.setdefault(arch, {})[orig] = uni

    resultados = []
    # 3) Procesar cada archivo salvo el índice
    for filename, raw in _saved_files.items():
        if filename.lower() == "indice.xlsx":
            continue
        try:
            df = pd.read_excel(io.BytesIO(raw))
            # Renombrar según mapping
            mapeo = mapping_dict.get(filename, {})
            if mapeo:
                df.rename(columns=mapeo, inplace=True)
            resultados.append({
                "filename":  filename,
                "columns":   df.columns.tolist(),
                "row_count": len(df)
            })
        except Exception as e:
            resultados.append({
                "filename": filename,
                "error":    str(e)
            })

    return {"resultados": resultados}
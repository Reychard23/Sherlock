from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import JSONResponse
from typing import List
import pandas as pd
import io

app = FastAPI()
_saved_files: dict[str, bytes] = {}

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
@app.post("/process-excel/")
async def process_files_endpoint():
    """
    Procesa TODOS los archivos previamente subidos con /upload-excel/.
    Busca primero 'indice.xlsx', arma el mapping y luego renombra y analiza el resto.
    """
    # 1) Leer el índice desde la memoria
    raw_index = _saved_files.get("indice.xlsx")
    if raw_index is None:
        return JSONResponse({"error": "No se subió indice.xlsx"}, status_code=400)
    indice_df = pd.read_excel(io.BytesIO(raw_index))

    # 2) Construir el diccionario de mapeo
    mapping_dict: dict[str, dict[str, str]] = {}
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
            # 4) Renombrar según el mapping
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

@app.post("/slack")
async def slack_command(request: Request):
    form_data = await request.form()
    print("Payload recibido:", form_data)
    return JSONResponse({"text": "Hola, yo soy Sherlock, y pronto estaré disponible para analizar tus datos"}, status_code=200)
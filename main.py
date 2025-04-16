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
    # 1. Busco dentro de los archivos subidos el índice
    index_file = None
    for f in files:
        if f.filename.lower() == "indice.xlsx":
            index_file = f
            break
    if not index_file:
        return JSONResponse({"error": "No se encontró archivo indice.xlsx"}, status_code=400)

    # 2. Leo el índice desde la memoria
    data_index = await index_file.read()
    indice_df  = pd.read_excel(io.BytesIO(data_index))

    # 3. Quito el índice de la lista para no procesarlo como dato
    data_files = [f for f in files if f is not index_file]

    # 4. Construyo el diccionario de mapeo
    mapping_dict = {}
    for _, row in indice_df.iterrows():
        archivo     = row["Archivo"].strip()
        columna     = row["Columna"].strip()
        descripcion = row["Descripción"].strip()
        mapping_dict.setdefault(archivo, {})[columna] = descripcion

    resultados = []
    # 5. Ahora itero solo sobre los archivos de datos
    for file in data_files:
        try:
            data = await file.read()
            df   = pd.read_excel(io.BytesIO(data))

            # renombrado según mapeo
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

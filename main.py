from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import JSONResponse
import pandas as pd
import io


app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "Hola, reychard"}

@app.post("/upload-excel/")
async def upload_excel(file: UploadFile = File(...)):
    valid_types = [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel"
    ]
    if file.content_type not in valid_types:
        return {"error": "El archivo no es un Excel válido. Asegúrate de subir un archivo .xlsx o .xls."}

    # Leer el archivo con Pandas
    data = await file.read()  # Leer el contenido del archivo
    df = pd.read_excel(io.BytesIO(data))
    
    # Retornar algunas estadísticas básicas
    return {
        "filename": file.filename,
        "columns": df.columns.tolist(),
        "row_count": len(df)
    }
@app.post("/slack")
async def slack_command(request: Request):
    # Obtener el payload del comando (lo puedes imprimir para depurar)
    form_data = await request.form()
    print("Payload recibido:", form_data)  # Esto se verá en los logs de Render
    # Responder con un mensaje simple en formato que Slack entienda
    return JSONResponse({"text": "Hola, yo soy Sherlock, y pronto estaré disponible para analizar tus datos"}, status_code=200)

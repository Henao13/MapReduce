from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import os
import boto3
from urllib.parse import urlparse

app = FastAPI(title="MapReduce Envigado")

DIRECTORIO_BASE = os.path.dirname(os.path.abspath(__file__))
DIRECTORIO_SALIDA = os.path.join(DIRECTORIO_BASE, "output_emr")
ARCHIVO_RESULTADOS = os.path.join(DIRECTORIO_SALIDA, 'resultados.txt')
RUTA_S3_OUTPUT = 's3://proyecto-3-top-telematica/output'

def descargar_desde_s3():
    if not os.path.exists(DIRECTORIO_SALIDA):
        os.makedirs(DIRECTORIO_SALIDA)

    try:
        # Parseamos la URL del bucket
        parsed_url = urlparse(RUTA_S3_OUTPUT)
        bucket_name = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')

        s3 = boto3.client('s3')
        
        # Listamos TODOS los objetos en la carpeta de salida
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            return

        # Abrimos el archivo local en modo binario para escribir (WB)
        with open(ARCHIVO_RESULTADOS, 'wb') as outfile:
            for obj in response['Contents']:
                key = obj['Key']
                # Solo descargamos los archivos que son resultados (empiezan por part-)
                if "part-" in key:
                    print(f"Descargando segmento: {key}")
                    data = s3.get_object(Bucket=bucket_name, Key=key)
                    outfile.write(data['Body'].read())
        
    except Exception:
        # Si falla, creamos uno vacio para no romper la API
        if not os.path.exists(ARCHIVO_RESULTADOS):
            with open(ARCHIVO_RESULTADOS, 'w') as f: f.write("")

def leer_datos():
    descargar_desde_s3()
    
    data = {
        "estado": "OK",
        "estadisticas": {
            "dias": {}, "gravedad": {}, "clase_accidente": {}, "causa": {}, "barrios": {}
        }
    }

    if not os.path.exists(ARCHIVO_RESULTADOS) or os.path.getsize(ARCHIVO_RESULTADOS) == 0:
        return {"estado": "ERROR", "mensaje": "Archivo vacio o no encontrado en S3"}

    try:
        with open(ARCHIVO_RESULTADOS, 'r', encoding='utf-8') as f:
            for linea in f:
                try:
                    parts = linea.strip().replace('"', '').split('\t')
                    if len(parts) < 2: continue
                    
                    k, v = parts[0], int(parts[1])
                    
                    if k.startswith("DIA_"): data["estadisticas"]["dias"][k[4:]] = v
                    elif k.startswith("GRAVEDAD_"): data["estadisticas"]["gravedad"][k[9:]] = v
                    elif k.startswith("CLASE_"): data["estadisticas"]["clase_accidente"][k[6:]] = v
                    elif k.startswith("CAUSA_"): data["estadisticas"]["causa"][k[6:]] = v
                    elif k.startswith("BARRIO_"): data["estadisticas"]["barrios"][k[7:]] = v
                except:
                    continue
    except Exception as e:
        return {"estado": "ERROR", "mensaje": str(e)}
        
    return data

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
        <head>
            <title>API EMR</title>
            <style>
                body { font-family: sans-serif; text-align: center; padding-top: 50px; background-color: #f4f6f7; }
                .card { background: white; padding: 20px; border-radius: 10px; display: inline-block; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
                h1 { color: #2c3e50; }
                a { text-decoration: none; color: #3498db; font-weight: bold; margin: 0 10px; }
            </style>
        </head>
        <body>
            <div class="card">
                <h1>API Corriendo en AWS EMR</h1>
                <p>Procesamiento finalizado</p>
                <hr>
                <p>
                    <a href="/api/datos">Ver JSON</a>
                    <a href="/docs">Documentacion Swagger</a>
                </p>
            </div>
        </body>
    </html>
    """

@app.get("/api/datos")
def get_datos():
    return JSONResponse(content=leer_datos())

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=5000)
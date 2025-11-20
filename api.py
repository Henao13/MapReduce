from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import subprocess
import os
import sys

app = FastAPI(
    title="MapReduce Accidentes Envigado",
    description="API para visualizar los resultados del procesamiento batch.",
    version="1.0.0"
)

DIRECTORIO_BASE = os.path.dirname(os.path.abspath(__file__))
DIRECTORIO_SALIDA_LOCAL = os.path.join(DIRECTORIO_BASE, "output")
ARCHIVO_RESULTADOS = os.path.join(DIRECTORIO_SALIDA_LOCAL, 'resultados.txt')
RUTA_OUTPUT_HDFS = '/user/estudiante/proyecto3/output'

def intentar_descarga_hdfs():
    print("No se encontro archivo local. Intentando descargar de HDFS...")
    
    ruta_part_hdfs = f"{RUTA_OUTPUT_HDFS}/part-00000"
    cmd = f'hdfs dfs -get -f {ruta_part_hdfs} "{ARCHIVO_RESULTADOS}"'
    
    try:
        subprocess.run(cmd, shell=True, check=True)
        print("Descarga de HDFS exitosa.")
    except subprocess.CalledProcessError:
        print("Error: Fallo la descarga de HDFS.")
    except FileNotFoundError:
        print("Error: Comando hdfs no encontrado.")

def leer_datos():
    data = {
        "estado": "OK",
        "modo_ejecucion": "LOCAL",
        "estadisticas": {"dias": {}, "gravedad": {}, "barrios": {}}
    }

    if not os.path.exists(ARCHIVO_RESULTADOS):
        intentar_descarga_hdfs()

    if not os.path.exists(ARCHIVO_RESULTADOS):
        return {
            "estado": "ERROR", 
            "mensaje": "No se encuentra output/resultados.txt. Ejecuta primero el MapReduce."
        }

    try:
        with open(ARCHIVO_RESULTADOS, 'r', encoding='utf-8') as f:
            for linea in f:
                try:
                    parts = linea.strip().replace('"', '').split('\t')
                    if len(parts) < 2: continue
                    
                    clave = parts[0]
                    valor = int(parts[1])
                    
                    if clave.startswith("DIA_"):
                        data["estadisticas"]["dias"][clave.replace("DIA_", "")] = valor
                    elif clave.startswith("GRAVEDAD_"):
                        data["estadisticas"]["gravedad"][clave.replace("GRAVEDAD_", "")] = valor
                    elif clave.startswith("BARRIO_"):
                        data["estadisticas"]["barrios"][clave.replace("BARRIO_", "")] = valor
                except:
                    continue
    except Exception as e:
        return {"estado": "ERROR", "mensaje": str(e)}
        
    return data

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
        <head><title>Proyecto MapReduce</title></head>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
            <h1>API Accidentalidad Envigado</h1>
            <p>Sistema funcionando correctamente.</p>
            <div style="margin-top: 20px;">
                <a href="/api/datos" style="margin-right: 20px; font-size: 18px;">Ver JSON</a>
                <a href="/docs" style="font-size: 18px;">Documentacion (Swagger)</a>
            </div>
        </body>
    </html>
    """

@app.get("/api/datos")
def get_datos():
    return JSONResponse(content=leer_datos())

if __name__ == '__main__':
    print("Iniciando API (FastAPI)")
    print(f"Buscando datos en: {ARCHIVO_RESULTADOS}")

    if os.path.exists(ARCHIVO_RESULTADOS):
        print("Archivo local encontrado. Modo OFFLINE activado.")

    uvicorn.run(app, host='0.0.0.0', port=5000)
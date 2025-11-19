from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import subprocess
import os
import sys

# --- CONFIGURACIÓN ---
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
    """
    Solo se ejecuta si NO encuentra el archivo local.
    Intenta conectar al clúster para bajar los datos.
    """
    print("No se encontró archivo local. Intentando descargar de HDFS (Modo Clúster)...")
    
    ruta_part_hdfs = f"{RUTA_OUTPUT_HDFS}/part-00000"
    # Usamos 'hdfs.cmd' en Windows o 'hdfs' en Linux, intentamos genérico
    cmd = f'hdfs dfs -get -f {ruta_part_hdfs} "{ARCHIVO_RESULTADOS}"'
    
    try:
        # check=True lanzará error si el comando falla (ej. si no tienes Hadoop instalado)
        subprocess.run(cmd, shell=True, check=True)
        print("Descarga de HDFS exitosa.")
    except subprocess.CalledProcessError:
        print("ERROR: Falló la descarga de HDFS (¿Quizás estás en modo local sin clúster?)")
    except FileNotFoundError:
        print("ERROR: No se encontró el comando 'hdfs'. Asumiendo modo local estricto.")

def leer_datos():
    """Lee el archivo de texto generado por MapReduce y lo convierte a JSON"""
    data = {
        "estado": "OK",
        "modo_ejecucion": "LOCAL",
        "estadisticas": {"dias": {}, "gravedad": {}, "barrios": {}}
    }

    # 1. Si no existe el archivo, intentamos bajarlo (por si cambias a cluster en el futuro)
    if not os.path.exists(ARCHIVO_RESULTADOS):
        intentar_descarga_hdfs()

    # 2. Si sigue sin existir después del intento, retornamos error
    if not os.path.exists(ARCHIVO_RESULTADOS):
        return {
            "estado": "ERROR",
            "mensaje": "No se encuentra 'output/resultados.txt'. Ejecuta primero el MapReduce."
        }

    # 3. Procesamiento del archivo
    try:
        with open(ARCHIVO_RESULTADOS, 'r', encoding='utf-8') as f:
            for linea in f:
                try:
                    # mrjob genera: "CLAVE" VALOR
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

# --- ENDPOINTS ---

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
        <head><title>Proyecto MapReduce</title></head>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
            <h1>API Accidentalidad Envigado</h1>
            <p style="color: green;">Sistema funcionando correctamente.</p>
            <div style="margin-top: 20px;">
                <a href="/api/datos" style="margin-right: 20px; font-size: 18px;">Ver JSON</a>
                <a href="/docs" style="font-size: 18px;">Documentación (Swagger)</a>
            </div>
        </body>
    </html>
    """

@app.get("/api/datos")
def get_datos():
    return JSONResponse(content=leer_datos())

# --- EJECUCIÓN ---
if __name__ == '__main__':
    print(f"\n--- INICIANDO API (FastAPI) ---")
    print(f"Buscando datos en: {ARCHIVO_RESULTADOS}")

    # Si el archivo ya existe (creado por run.ps1), avisamos que usaremos ese
    if os.path.exists(ARCHIVO_RESULTADOS):
        print("Archivo local encontrado. Modo OFFLINE activado (No se usará HDFS).")

    # Corremos en puerto 5000
    uvicorn.run(app, host='0.0.0.0', port=5000)
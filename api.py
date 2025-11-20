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
        "estadisticas": {
            "dias": {}, 
            "gravedad": {}, 
            "clase_accidente": {},
            "causa": {},
            "barrios": {}
        }
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
                    if len(parts) < 2:
                        continue

                    clave = parts[0]
                    valor = int(parts[1])

                    if clave.startswith("DIA_"):
                        data["estadisticas"]["dias"][clave.replace("DIA_", "")] = valor
                    elif clave.startswith("GRAVEDAD_"):
                        data["estadisticas"]["gravedad"][clave.replace("GRAVEDAD_", "")] = valor
                    elif clave.startswith("CLASE_"):
                        data["estadisticas"]["clase_accidente"][clave.replace("CLASE_", "")] = valor
                    elif clave.startswith("CAUSA_"):
                        data["estadisticas"]["causa"][clave.replace("CAUSA_", "")] = valor
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
        <head>
            <title>Proyecto MapReduce</title>
            <style>
                body { 
                    font-family: Arial, sans-serif; 
                    margin: 20px; 
                    background-color: #f5f5f5; 
                }
                .header { 
                    text-align: center; 
                    background-color: #2c3e50; 
                    color: white; 
                    padding: 20px; 
                    border-radius: 10px; 
                    margin-bottom: 30px; 
                }
                .container { 
                    max-width: 1200px; 
                    margin: 0 auto; 
                }
                .section { 
                    background-color: white; 
                    margin-bottom: 30px; 
                    padding: 20px; 
                    border-radius: 10px; 
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1); 
                }
                .section h2 { 
                    color: #2c3e50; 
                    margin-bottom: 15px; 
                    border-bottom: 2px solid #3498db; 
                    padding-bottom: 5px; 
                }
                table { 
                    width: 100%; 
                    border-collapse: collapse; 
                    margin-top: 10px; 
                }
                th, td { 
                    padding: 12px; 
                    text-align: left; 
                    border-bottom: 1px solid #ddd; 
                }
                th { 
                    background-color: #3498db; 
                    color: white; 
                    font-weight: bold; 
                }
                tr:nth-child(even) { 
                    background-color: #f9f9f9; 
                }
                tr:hover { 
                    background-color: #f5f5f5; 
                }
                .status-ok { 
                    color: #27ae60; 
                    font-weight: bold; 
                }
                .links { 
                    text-align: center; 
                    margin-top: 20px; 
                }
                .links a { 
                    margin: 0 10px; 
                    padding: 10px 20px; 
                    background-color: #3498db; 
                    color: white; 
                    text-decoration: none; 
                    border-radius: 5px; 
                }
                .links a:hover { 
                    background-color: #2980b9; 
                }
                .loading { 
                    text-align: center; 
                    padding: 20px; 
                    font-style: italic; 
                    color: #7f8c8d; 
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>API Accidentalidad Envigado</h1>
                    <p>Sistema de Análisis MapReduce</p>
                </div>

                <div id="data-container">
                    <div class="loading">Cargando datos...</div>
                </div>

                <div class="links">
                    <a href="/api/datos">Ver JSON</a>
                    <a href="/docs">Documentación (Swagger)</a>
                </div>
            </div>

            <script>
                async function cargarDatos() {
                    try {
                        const response = await fetch('/api/datos');
                        const data = await response.json();

                        const container = document.getElementById('data-container');

                        if (data.estado === 'ERROR') {
                            container.innerHTML = `
                                <div class="section">
                                    <h2 style="color: #e74c3c;">Error</h2>
                                    <p style="color: #e74c3c;">${data.mensaje}</p>
                                </div>
                            `;
                            return;
                        }

                        container.innerHTML = `
                            <div class="section">
                                <h2>Estado del Sistema</h2>
                                <p><strong>Estado:</strong> <span class="status-ok">${data.estado}</span></p>
                                <p><strong>Modo de Ejecución:</strong> ${data.modo_ejecucion}</p>
                            </div>

                            <div class="section">
                                <h2>Accidentes por Día de la Semana</h2>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Día</th>
                                            <th>Cantidad de Accidentes</th>
                                            <th>Porcentaje</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${generarFilasDias(data.estadisticas.dias)}
                                    </tbody>
                                </table>
                            </div>

                            <div class="section">
                                <h2>Accidentes por Gravedad</h2>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Tipo de Gravedad</th>
                                            <th>Cantidad de Accidentes</th>
                                            <th>Porcentaje</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${generarFilasGravedad(data.estadisticas.gravedad)}
                                    </tbody>
                                </table>
                            </div>

                            <div class="section">
                                <h2>Accidentes por Clase</h2>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Clase de Accidente</th>
                                            <th>Cantidad de Accidentes</th>
                                            <th>Porcentaje</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${generarFilasClase(data.estadisticas.clase_accidente)}
                                    </tbody>
                                </table>
                            </div>

                            <div class="section">
                                <h2>Accidentes por Causa (Top 20)</h2>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Causa del Accidente</th>
                                            <th>Cantidad de Accidentes</th>
                                            <th>Porcentaje</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${generarFilasCausa(data.estadisticas.causa)}
                                    </tbody>
                                </table>
                            </div>

                            <div class="section">
                                <h2>Accidentes por Barrio (Top 20)</h2>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Barrio</th>
                                            <th>Cantidad de Accidentes</th>
                                            <th>Porcentaje</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${generarFilasBarrios(data.estadisticas.barrios)}
                                    </tbody>
                                </table>
                            </div>
                        `;
                    } catch (error) {
                        document.getElementById('data-container').innerHTML = `
                            <div class="section">
                                <h2 style="color: #e74c3c;">Error de Conexión</h2>
                                <p style="color: #e74c3c;">No se pudieron cargar los datos: ${error.message}</p>
                            </div>
                        `;
                    }
                }

                function generarFilasDias(dias) {
                    const total = Object.values(dias).reduce((sum, val) => sum + val, 0);
                    const ordenDias = ['LUNES', 'MARTES', 'MIÉRCOLES', 'JUEVES', 'VIERNES', 'SÁBADO', 'DOMINGO'];

                    return ordenDias.map(dia => {
                        const cantidad = dias[dia] || 0;
                        const porcentaje = ((cantidad / total) * 100).toFixed(1);
                        return `
                            <tr>
                                <td>${dia}</td>
                                <td>${cantidad.toLocaleString()}</td>
                                <td>${porcentaje}%</td>
                            </tr>
                        `;
                    }).join('');
                }

                function generarFilasGravedad(gravedad) {
                    const total = Object.values(gravedad).reduce((sum, val) => sum + val, 0);

                    return Object.entries(gravedad)
                        .sort(([,a], [,b]) => b - a)
                        .map(([tipo, cantidad]) => {
                            const porcentaje = ((cantidad / total) * 100).toFixed(1);
                            return `
                                <tr>
                                    <td>${tipo}</td>
                                    <td>${cantidad.toLocaleString()}</td>
                                    <td>${porcentaje}%</td>
                                </tr>
                            `;
                        }).join('');
                }

                function generarFilasBarrios(barrios) {
                    const total = Object.values(barrios).reduce((sum, val) => sum + val, 0);

                    return Object.entries(barrios)
                        .sort(([,a], [,b]) => b - a)
                        .slice(0, 20)
                        .map(([barrio, cantidad]) => {
                            const porcentaje = ((cantidad / total) * 100).toFixed(1);
                            return `
                                <tr>
                                    <td>${barrio}</td>
                                    <td>${cantidad.toLocaleString()}</td>
                                    <td>${porcentaje}%</td>
                                </tr>
                            `;
                        }).join('');
                }

                function generarFilasClase(clases) {
                    const total = Object.values(clases).reduce((sum, val) => sum + val, 0);

                    return Object.entries(clases)
                        .sort(([,a], [,b]) => b - a)
                        .map(([clase, cantidad]) => {
                            const porcentaje = ((cantidad / total) * 100).toFixed(1);
                            return `
                                <tr>
                                    <td>${clase}</td>
                                    <td>${cantidad.toLocaleString()}</td>
                                    <td>${porcentaje}%</td>
                                </tr>
                            `;
                        }).join('');
                }

                function generarFilasCausa(causas) {
                    const total = Object.values(causas).reduce((sum, val) => sum + val, 0);

                    return Object.entries(causas)
                        .sort(([,a], [,b]) => b - a)
                        .slice(0, 20)
                        .map(([causa, cantidad]) => {
                            const porcentaje = ((cantidad / total) * 100).toFixed(1);
                            return `
                                <tr>
                                    <td>${causa}</td>
                                    <td>${cantidad.toLocaleString()}</td>
                                    <td>${porcentaje}%</td>
                                </tr>
                            `;
                        }).join('');
                }

                // Cargar datos al iniciar la página
                cargarDatos();
            </script>
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

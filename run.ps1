# --- CONFIGURACIÓN DE RUTAS ---
# Detectamos la carpeta donde esta este script (E:\Santiago\Programación\MapReduce)
$RutaBase = $PSScriptRoot

# Scripts de Python (en la raíz)
$ScriptMapReduce = Join-Path $RutaBase "mapreduce.py"
$ScriptApi = Join-Path $RutaBase "api.py"

# Archivo CSV (Ahora dentro de la carpeta 'data')
$ArchivoCSV = Join-Path $RutaBase "data\Accidentalidad_Municipio_de__Envigado_20251119.csv"

# Configuración de salida
$DirectorioOutput = Join-Path $RutaBase "output"
$ArchivoResultados = Join-Path $DirectorioOutput "resultados.txt"

Write-Host "--- 1. VERIFICANDO ARCHIVOS ---" -ForegroundColor Cyan
Write-Host "Carpeta base: $RutaBase" -ForegroundColor Gray
Write-Host "Buscando CSV en: $ArchivoCSV" -ForegroundColor Gray

if (-not (Test-Path $ScriptMapReduce)) { Write-Error "Falta el archivo: mapreduce.py"; exit }
if (-not (Test-Path $ScriptApi)) { Write-Error "Falta el archivo: api.py"; exit }
if (-not (Test-Path $ArchivoCSV)) { Write-Error "Falta el archivo CSV en la carpeta data."; exit }

Write-Host "--- 2. LIMPIANDO ENTORNO ---" -ForegroundColor Cyan
# Crear carpeta output si no existe
if (-not (Test-Path -Path $DirectorioOutput)) {
    New-Item -ItemType Directory -Path $DirectorioOutput | Out-Null
}

# Borrar resultados anteriores
if (Test-Path -Path $ArchivoResultados) {
    Remove-Item -Path $ArchivoResultados
}

Write-Host "--- 3. EJECUTANDO MAPREDUCE (LOCAL) ---" -ForegroundColor Cyan
Write-Host "Procesando datos... esto puede tardar unos segundos." -ForegroundColor Yellow

# Ejecutamos mapreduce.py pasándole la ruta correcta del CSV
# Redireccionamos la salida al archivo de texto
python $ScriptMapReduce $ArchivoCSV | Out-File -FilePath $ArchivoResultados -Encoding UTF8

if (Test-Path -Path $ArchivoResultados) {
    Write-Host "✅ Procesamiento completado." -ForegroundColor Green
} else {
    Write-Host "❌ Error: No se generó el archivo de salida." -ForegroundColor Red
    exit
}

Write-Host "--- 4. ABRIENDO NAVEGADOR ---" -ForegroundColor Cyan
Start-Process "http://localhost:5000/api/datos"

Write-Host "--- 5. INICIANDO API ---" -ForegroundColor Cyan
Write-Host "Presiona CTRL+C para detener el servidor." -ForegroundColor Yellow

python $ScriptApi
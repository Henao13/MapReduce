# Proyecto MapReduce: Accidentalidad Envigado

Pequeño flujo de analítica batch que usa Hadoop MapReduce (vía `mrjob`) para contar accidentes por día de la semana, gravedad y barrio. Los resultados quedan en `output/resultados.txt` y se sirven mediante una API construida con FastAPI.

## Datos utilizados
- Fuente: reporte público de accidentalidad en Envigado.
- Archivo local: `data/Accidentalidad_Municipio_de__Envigado_20251119.csv`.

## Requisitos
- Python 3.12+ (probado en 3.13).
- Herramientas opcionales para clúster: `hdfs` disponible en PATH si vas a usar Hadoop remoto.
- Instalar dependencias de la carpeta raíz:

```bash
pip install -r requirements.txt
```

## Ejecución local
1. Procesar el dataset con MapReduce (lanza la carga a HDFS si el comando `hdfs` está presente, pero siempre produce la salida local):
   ```bash
   python mapreduce.py data/Accidentalidad_Municipio_de__Envigado_20251119.csv > output/resultados.txt
   ```
2. Levantar la API para consultar los agregados:
   ```bash
   uvicorn api:app --reload --port 5000
   ```
3. Abrir `http://localhost:5000/api/datos` para ver el JSON resultante o `http://localhost:5000/docs` para la documentación interactiva.

## Ejecución guiada (PowerShell)
En Windows puedes usar `run.ps1`, que limpia la carpeta `output`, ejecuta el job y luego inicia la API:

```powershell
pwsh .\run.ps1
```

## Flujo completo
1. El CSV se sube a HDFS (si existe `hdfs dfs`).
2. `mrjob` agrupa los registros por día, gravedad y barrio.
3. La salida del reducer se guarda en `output/resultados.txt` y puede descargarse desde HDFS si se corrió en clúster.
4. FastAPI expone los conteos vía `/api/datos`.

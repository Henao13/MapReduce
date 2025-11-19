import sys
import shlex
import os

# --- ZONA DE PARCHES PARA PYTHON 3.12 Y 3.13 ---
# 1. Parche para 'distutils' (Eliminado en Python 3.12)
# Intentamos importar setuptools que trae un reemplazo de distutils
try:
    import setuptools
except ImportError:
    pass # Si falla, el usuario debe correr: pip install setuptools

# 2. Parche para 'pipes' (Eliminado en Python 3.13)
if sys.version_info >= (3, 13):
    sys.modules['pipes'] = shlex
# -----------------------------------------------

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import subprocess

# --- CONFIGURACIÓN ---
ARCHIVO_LOCAL = 'data/Accidentalidad_Municipio_de__Envigado_20251119.csv'
RUTA_INPUT_HDFS = '/user/estudiante/proyecto3/input/'

class AnalisisAccidentes(MRJob):

    def mapper(self, _, line):
        # Ignorar encabezados
        if "RADICADO" in line and "BARRIO" in line:
            return

        try:
            row = next(csv.reader([line]))

            if len(row) >= 15:
                dia = row[3].strip().upper()       # Col 3: DIA
                gravedad = row[10].strip().upper() # Col 10: GRAVEDAD
                barrio = row[14].strip().upper()   # Col 14: BARRIO

                if dia: yield f"DIA_{dia}", 1
                if gravedad: yield f"GRAVEDAD_{gravedad}", 1
                if barrio and barrio != "SIN BARRIO": yield f"BARRIO_{barrio}", 1

        except Exception:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

def cargar_datos_a_hdfs():
    print(f"--- Iniciando carga de {ARCHIVO_LOCAL} a HDFS ---")
    # Crear directorio
    subprocess.run(f"hdfs dfs -mkdir -p {RUTA_INPUT_HDFS}", shell=True)
    # Subir archivo
    subprocess.run(f'hdfs dfs -put -f "{ARCHIVO_LOCAL}" {RUTA_INPUT_HDFS}', shell=True)

if __name__ == '__main__':
    args = sys.argv
    es_worker = any(arg.startswith('--') for arg in args)

    if not es_worker:
        if os.path.exists(ARCHIVO_LOCAL):
            cargar_datos_a_hdfs()
        else:
            print(f"⚠️ ADVERTENCIA: No encuentro el archivo en: {ARCHIVO_LOCAL}")

    AnalisisAccidentes.run()
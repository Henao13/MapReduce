import sys
import shlex
import os

# Parche de compatibilidad para Python 3.12+ y 3.13+
try:
    import setuptools
except ImportError:
    pass 

if sys.version_info >= (3, 13):
    sys.modules['pipes'] = shlex

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import subprocess


ARCHIVO_INPUT = 's3://trabajo-tres-mapreduce/data/Accidentalidad_Municipio_de__Envigado_20251119.csv'
RUTA_OUTPUT_S3 = 's3://trabajo-tres-mapreduce/output'

class AnalisisAccidentes(MRJob):

    def mapper(self, _, line):
        if "RADICADO" in line and "BARRIO" in line:
            return

        try:
            row = next(csv.reader([line]))
            
            if len(row) >= 15:
                dia = row[3].strip().upper()
                gravedad = row[10].strip().upper()
                barrio = row[14].strip().upper()

                if dia: 
                    yield f"DIA_{dia}", 1
                if gravedad: 
                    yield f"GRAVEDAD_{gravedad}", 1
                if barrio and barrio != "SIN BARRIO": 
                    yield f"BARRIO_{barrio}", 1
                    
                    
        except Exception:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

def verificar_origen_datos():

    if ARCHIVO_INPUT.startswith("s3://"):
        print(f"--- MODO NUBE DETECTADO ---")
        print(f"Fuente de datos: {ARCHIVO_INPUT}")
        print("Omitiendo carga manual (los datos ya estan en S3).")
    else:
        print(f"--- MODO LOCAL ---")
        if os.path.exists(ARCHIVO_INPUT):
            print(f"Usando archivo local: {ARCHIVO_INPUT}")
        else:
            print(f"Advertencia: No encuentro el archivo local: {ARCHIVO_INPUT}")

if __name__ == '__main__':
    args = sys.argv
    es_worker = any(arg.startswith('--') for arg in args)
    
    if not es_worker:
        verificar_origen_datos()

    AnalisisAccidentes.run()
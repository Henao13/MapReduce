import sys
import shlex
import os

try:
    import setuptools
except ImportError:
    pass 

if sys.version_info >= (3, 13):
    sys.modules['pipes'] = shlex

from mrjob.job import MRJob
import csv

ARCHIVO_INPUT = 's3://proyecto-3-top-telematica/data/Accidentalidad_Envigado.csv'

class AnalisisAccidentes(MRJob):

    def mapper(self, _, line):
        if "RADICADO" in line and "BARRIO" in line:
            return

        try:
            row = next(csv.reader([line]))
            
            if len(row) >= 15:
                # Asegúrate de tener estas 5 variables definidas
                dia = row[3].strip().upper()
                gravedad = row[10].strip().upper()
                clase = row[11].strip().upper()
                causa = row[12].strip().upper()
                barrio = row[14].strip().upper()

                # Y asegúrate de tener estos 5 yields
                if dia: yield f"DIA_{dia}", 1
                if gravedad: yield f"GRAVEDAD_{gravedad}", 1
                if clase: yield f"CLASE_{clase}", 1
                if causa: yield f"CAUSA_{causa}", 1
                if barrio and barrio != "SIN BARRIO": yield f"BARRIO_{barrio}", 1
                    
        except Exception:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    AnalisisAccidentes.run()
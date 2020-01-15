import argparse
import utils as u

parser = argparse.ArgumentParser()

# Parámetro archivo que especifica la ubicación del archivo a procesar
parser.add_argument("archivo", help="indica el path del archivo a leer",
                    type=str)

# Parámetro fecha especifica la fecha en la que se procesa el archivo
parser.add_argument("fecha", help="fecha a correr, formato: YYYY-MM-DD",
                    type=u.valid_date)

# modo verbose para detección de errores
parser.add_argument("-v", "--verbosity", help="activar modo verbose",
                    required=False, action="store_true")

args = parser.parse_args()
print(args.archivo)
print(args.fecha)
print(args.verbosity)
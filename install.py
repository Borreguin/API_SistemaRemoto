# ESTE SCRIPT PERMITE INSTALAR LOS PAQUETES NECESARIOS PARA CORRER LA API
# PARA CORRER ESTE SCRIPT SE DEBE EJECUTAR EL ARCHIVO INSTALL.BAT
# DAVID PANCHI 30/09/2020
# Add C:\Users\BMS\AppData\Roaming\Python\Python39\Scripts in PATH

import subprocess as sb
import os
import traceback


def install():
    script_path = os.path.dirname(os.path.abspath(__file__))
    requirements_path = os.path.join(script_path, "requirements.txt")
    try:
        sb.run(["pip3", "install", "--upgrade", "pip", "--user"])
        sb.run(["pip3", "install", "-r", requirements_path, "--user"])
    except Exception as e:
        msg = "Problemas al instalar los paquetes necesarios \n" + str(e) +"\n" + traceback.format_exc()
        print(msg)


if __name__ == "__main__":
    install()

import subprocess as sb
import os
import traceback


def install():
    script_path = os.path.dirname(os.path.abspath(__file__))
    requirements_path = os.path.join(script_path, "requirements.txt")
    try:
        sb.run(["pip", "install", "-r", requirements_path])
    except Exception as e:
        msg = "Problemas al instalar los paquetes necesarios \n" + str(e) +"\n" + traceback.format_exc()
        print(msg)


if __name__ == "__main__":
    install()

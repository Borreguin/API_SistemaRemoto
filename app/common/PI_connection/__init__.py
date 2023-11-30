import sys
import os

script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_path)
pi_label = "PI-connect ->"
simulation = False if sys.platform == 'win32' else True
print(f"{pi_label} PI connection for OS: {sys.platform}")


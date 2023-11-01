import os
import sys

# To include the project path in the Operating System path:
print(">>>>>\tAdding project path...")
script_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
motor_path = os.path.join(script_path, "motor")
templates_path = os.path.join(script_path, "templates")
sys.path.append(project_path)


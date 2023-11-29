import sys
import os

script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_path)

pi_label = "PI-connect ->"
simulation = None
print(f"{pi_label} PI connection for OS: {sys.platform}")

if sys.platform == 'win32':
    AF_PATH = os.path.abspath(os.path.join(os.sep, "Program Files (x86)", "PIPC", "AF", "PublicAssemblies", "4.0"))
    if os.path.exists(AF_PATH):
        sys.path.append(AF_PATH)
        # verify that clr is imported from here: \lib\site-packages\clr.pyd
        import clr

        clr.AddReference('OSIsoft.AFSDK')
        from OSIsoft.AF import *
        from OSIsoft.AF.PI import *
        from OSIsoft.AF.Asset import *
        from OSIsoft.AF.Data import *
        from OSIsoft.AF.Time import *
        from OSIsoft.AF.UnitsOfMeasure import *
        print(f"{pi_label} import all AF libraries")
        simulation = False
    else:
        print(f"{pi_label} There is no AF path: {AF_PATH}, mode: simulation")
        simulation = True

elif sys.platform == 'linux' or sys.platform == 'darwin':
    simulation = True
    print(f"{pi_label} simulation mode: {simulation}")

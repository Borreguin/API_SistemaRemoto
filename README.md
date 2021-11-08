# API_SistemaRemoto

La Subgerencia Nacional de Servicios de Tiempo Real tiene como objetivo desarrollar una herramienta informática que
permita observar y analizar la disponibilidad del sistema SCADA del CENACE, a fin de determinar indicadores que permitan
fortalecer la toma de decisiones en lo referente a la administración del SCADA. Este repositorio contiene la API de
sistema Central.

**Nota**: Esta API utiliza software especializado de CENACE, por lo que es exclusivo para su uso interno. Ninguna
información sensible es expuesta, simplemente debido a que CENACE no dispone de una herramienta corporativa para
versionamiento de software se ha utilizado Github como herramienta temporal.

## Instalación:

Esta API es compatible con versiones inferiores a Python 3.7, una posible incompatibilidad puede existir para versiones
superiores o iguales a Python 3.8. La información completa acerca de su compatibilidad se encuentra en la
[página oficial de python .Net](http://pythonnet.github.io/). Esta API solamente puede ser ejecutada sobre sistemas
operativos Windows, esto debido a que se utiliza software
[especializado de OSIsoft](https://docs.osisoft.com/bundle/af-sdk/page/html/overview.htm) para la consulta de datos
históricos del sistema SCADA. El software necesario es
[PI-SDK](https://techsupport.osisoft.com/Documentation/PI-SDK/Manual/Installation/installation.htm) y
[PI-AF Client](https://techsupport.osisoft.com/Documentation/PI-SDK/Manual/Installation/installation.htm). La
instalación y autenticación de este software está a cargo de los administradores de los servidores PI de CENACE.

La instalación de los paquetes relacionados a esta API se puede realizar mediante el script
[install.bat](/install.bat) que considera los paquetes listados en [requirements.txt](/requirements.txt).

El script [init_mongo_db](/init_mongo_db.bat) inicializa una instancia de pruebas para una base de datos de
[MongoDB community](https://www.mongodb.com/try/download/community) en la carpeta "/_db". Esta base de datos solamente
es de desarrollo y pruebas. La base de datos de producción deberá tener las configuraciones necesarias para asegurar su
correcto [funcionamiento y seguridad](https://docs.mongodb.com/manual/security/).

## Configuración:

**Ambiente de pruebas:**

Las configuraciones para un ambiente de desarrollo se las realiza sobre el archivo
[flask_app/settings/env/dev.py](/flask_app/settings/env/dev.py), **un ejemplo** de cómo configurar este archivo se
encuentra en [flask_app/settings/env/dev_example.py](/flask_app/settings/env/dev_example.py).

**Ambiente de producción:**

Las configuraciones del ambiente de producción se las realiza sobre el archivo
[flask_app/settings/env/prod.py](/flask_app/settings/env/prod.py), **un ejemplo** de cómo configurar este archivo se
encuentra en [flask_app/settings/env/prod_example.py](/flask_app/settings/env/dev_example.py).

**Seguridad de credenciales:**
Una vez ejecutada y compilada la aplicación completa, en la ubicación _/flask_app/settings/env_ se encontrará la carpeta
**\_\_pycache__** que contiene archivos _.pyc_ (archivo Python compilado). A fin de mantener las credenciales ocultas,
renombrar el archivo **prod.cpython3X.pyc** por **prod.pyc** y colocarlo en
_/flask_app/settings/env_ en reemplazo del archivo *prod.py*.

## Ejecución:

**Ambiente de pruebas:**

La ejecución de la API se realiza mediante `python /flask_app/api/app_dev.py`.

**Ambiente de producción:**

La ejecución de la API se realiza mediante `python /flask_app/api/app.py`.

## Logs:

Al iniciar la aplicación se creará de manera automática la carpeta: _/logs_ donde se registrarán las acciones, eventos y
errores de la API:

1. _app_activity.log_: registra las acciones realizadas en la API
2. _app_errors.log_: registra errores generales de la aplicación
3. _mongo_engine.log_ registra errores en base de datos
4. Los demás logs registran errores particulares de los endpoints.
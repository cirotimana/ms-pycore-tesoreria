# Backend Project Documentation

---
## Overview
Este proyecto es una aplicación de backend que centraliza la lógica de dos sistemas independientes: un sistema de CCTV y Teleservicios. Cada sistema tiene su propia base de datos y funciones específicas, organizados como servicios reutilizables. La aplicación expone varias rutas como API para interactuar con ambos sistemas.

---
## Estructura del Proyecto
El proyecto está organizado de la siguiente manera:

```
backend-project
├── app
│   ├── main.py                # Punto de entrada de la aplicación
│   ├── cctv                   # Módulo para el sistema CCTV
│   │   ├── __init__.py
│   │   ├── models.py          # Modelos de datos para CCTV
│   │   ├── routes.py          # Rutas de la API para CCTV
│   │   └── services.py        # Lógica de negocio para CCTV
│   ├── teleservices           # Módulo para el sistema de Teleservicios
│   │   ├── __init__.py
│   │   ├── models.py          # Modelos de datos para Teleservicios
│   │   ├── routes.py          # Rutas de la API para Teleservicios
│   │   └── services.py        # Lógica de negocio para Teleservicios
│   ├── common                 # Módulo común para ambos sistemas
│   │   ├── __init__.py
│   │   ├── database.py        # Gestión de conexiones a bases de datos
│   │   └── utils.py           # Funciones utilitarias
│   └── config.py              # Configuración general de la aplicación
├── tests                      # Paquete de pruebas
│   ├── __init__.py
│   ├── test_cctv.py          # Pruebas unitarias para CCTV
│   ├── test_teleservices.py   # Pruebas unitarias para Teleservicios
│   └── test_common.py        # Pruebas unitarias para funciones comunes
├── .env                       # Variables de entorno
├── requirements.txt           # Dependencias del proyecto
└── README.md                  # Documentación del proyecto
```

---
## Instalación
1. Clona el repositorio en tu máquina local.
2. Navega al directorio del proyecto.
3. Asegúrate de tener instalada la versión de Python requerida (3.12.9).
4. Crea un entorno virtual y actívalo:
   ```
   python -m venv .venv
   .venv\Scripts\activate  # En Windows
   source .venv/bin/activate  # En macOS/Linux
5. Seleccionar interprete en VS Code (ctrl + shift + p) - .venv/Scripts/python.exe   ```
6. Instala las dependencias:
   ```
   pip install -r requirements.txt
   ```

---
## Uso
Para iniciar la aplicación, ejecuta el siguiente comando:
```
uvicorn app.main:app --reload
```
Esto iniciará el servidor y podrás acceder a las rutas de la API en `http://localhost:8000`.

---
### Instalación de Python con pyenv (opcional)

Si no tienes instalada la versión de Python requerida (3.12.9), puedes usar `pyenv` para instalarla siguiendo estos pasos:

1. Verifica si `pyenv` está instalado ejecutando:
   ```
   pyenv --version
   ```
   Si no está instalado, sigue las instrucciones de instalación en [pyenv](https://github.com/pyenv/pyenv#installation).

2. Lista las versiones disponibles de Python:
   ```
   pyenv install --list
   ```

3. Instala la versión requerida (3.12.9):
   ```
   pyenv install 3.12.9
   ```

4. Configura la versión instalada como global:
   ```
   pyenv global 3.12.9
   ```

5. Verifica que la versión correcta esté activa:
   ```
   python --version
   ```
   Esto debería mostrar `Python 3.12.9`.

Después de instalar la versión correcta de Python, puedes continuar con los pasos de instalación mencionados anteriormente.


## correr docker
docker-compose up --build -d

## ver el log  
docker logs -f pycore-backend


-----
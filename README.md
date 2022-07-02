# Transo GPS Processing
The module serves as an organised framework for GPS Data Processing.

## Requirements
- docker, docker-compose
- Python 3.5 or above
- Python Modules: `requirements.txt`

## Server Side
- The module uses eclipse-mosquitto. Enabling mosquitto service: `docker-compose up`
	> `docker-compose up -d` to start docker in a detached mode in the terminal

- The backend is built using python, and is present in the src folder. `python3 src/main.py` to run the service

## Client Side Example
- The client side can be accessed using any of the following node modules
- `MQQT Over Websockets`
- `paho-mqtt`

## Project Structure
```bash
├── connections
│   ├── .env
│   ├── __init__.py
│   ├── mqtt_connect.py
│   └── postgres_connect.py
├── core
│   ├── __init__.py
│   ├── errors.py
│   └── classes.py
├── __init__.py
├── environment.py__init__.py
├── main.py
└── perform_checks.py
```


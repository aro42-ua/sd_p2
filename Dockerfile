FROM python:3.10-slim

# Instalar dependencias del sistema necesarias para Tkinter (GUI)
RUN apt-get update && apt-get install -y \
    python3-tk \
    tk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar librerías Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Comando por defecto (se puede sobreescribir)
CMD ["python", "ev_central/EV_Central.py"]
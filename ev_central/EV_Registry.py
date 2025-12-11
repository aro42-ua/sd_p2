import sqlite3
import os
import sys
import secrets
from flask import Flask, request, jsonify

# --- CONFIGURACIÓN ---
# Debe acceder a la misma BBDD que la Central
DB_NAME = 'ev_central.db' 

# Configuración del Servidor
REGISTRY_HOST = '0.0.0.0'  # Escucha en todas las interfaces (necesario para Docker/Red)
REGISTRY_PORT = 6000       # Puerto 6000 (Diferente a los 5000 y 8000 de Central)

app = Flask(__name__)

def get_db_connection():
    """Establece conexión con la base de datos compartida."""
    # check_same_thread=False es necesario porque Flask es multihilo
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def generate_credentials():
    """
    Genera las credenciales de seguridad para el CP.
    Según la Release 2, el Registry debe devolver claves para autenticación y cifrado.
    """
    # Token de sesión (para identificarse en el futuro)
    auth_token = secrets.token_hex(16)      
    # Clave de cifrado simétrico (para cifrar mensajes Kafka en el futuro)
    encryption_key = secrets.token_urlsafe(32) 
    return auth_token, encryption_key

# --- ENDPOINTS API REST ---

@app.route('/api/register-cp', methods=['POST'])
def register_cp():
    """
    Endpoint principal de registro.
    Entrada (JSON): { "cp_id": "ALC1", "location": "Alicante...", "price": 0.50 }
    Salida (JSON): { "status": "OK", "auth_token": "...", "encryption_key": "..." }
    """
    data = request.json
    
    # Validamos datos mínimos
    cp_id = data.get('cp_id')
    location = data.get('location')
    price = data.get('price')

    if not cp_id or not location:
        return jsonify({"error": "Faltan datos obligatorios (cp_id, location)"}), 400

    print(f"[REGISTRY] 📩 Petición de alta recibida para: {cp_id}")

    # Generamos credenciales nuevas para este CP
    token, enc_key = generate_credentials()

    conn = None
    try:
        conn = get_db_connection()
        
        # Insertamos o actualizamos (Upsert) el CP en la base de datos.
        # Inicialmente lo marcamos como DESCONECTADO hasta que conecte su Socket con Central.
        conn.execute(
            """
            INSERT INTO ChargingPoints (cp_id, location, price_kwh, status, last_update)
            VALUES (?, ?, ?, 'DESCONECTADO', CURRENT_TIMESTAMP)
            ON CONFLICT(cp_id) DO UPDATE SET
                location = excluded.location,
                price_kwh = excluded.price_kwh,
                last_update = CURRENT_TIMESTAMP
            """,
            (cp_id, location, price)
        )
        conn.commit()
        print(f"[REGISTRY] ✅ CP {cp_id} registrado/actualizado en BBDD.")
        
        # Devolvemos las credenciales al Monitor del CP
        response = {
            "status": "OK",
            "message": "CP Registrado correctamente",
            "auth_token": token,        # El CP guardará esto para identificarse
            "encryption_key": enc_key   # El CP usará esto para cifrar mensajes
        }
        return jsonify(response), 200

    except sqlite3.Error as e:
        print(f"[REGISTRY_ERROR] Error en BBDD: {e}")
        return jsonify({"error": "Error interno de base de datos"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint para comprobar que el Registry está vivo."""
    return jsonify({"status": "Registry Online", "port": REGISTRY_PORT})

if __name__ == "__main__":
    # Comprobación de seguridad: La BBDD debe existir (la crea EV_Central)
    if not os.path.exists(DB_NAME):
        print(f"⚠️  ADVERTENCIA: No se encuentra '{DB_NAME}' en el directorio actual.")
        print("   Asegúrate de ejecutar este script desde la carpeta 'ev_central' o que la BBDD esté accesible.")
    
    print(f"--- EV_REGISTRY INICIADO (Puerto {REGISTRY_PORT}) ---")
    print(f"--- Esperando peticiones de CPs... ---")
    
    # Arrancamos Flask
    app.run(host=REGISTRY_HOST, port=REGISTRY_PORT, debug=False)
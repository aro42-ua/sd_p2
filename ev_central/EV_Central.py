import sys
import time
import queue
import json
import threading
import sqlite3
import socket
import os
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from flask import Flask, jsonify, request
from flask_cors import CORS
from cryptography.fernet import Fernet # <--- IMPORTANTE: Para descifrar

# --- CONFIGURACIÓN ---
DB_NAME = 'ev_central.db'
SOCKET_HOST = '0.0.0.0'
SOCKET_PORT = 8000     # Puerto para Sockets (CPs)
API_PORT = 5000        # Puerto para API REST (Web y Clima)
KAFKA_SERVER = 'localhost:9092'
HEARTBEAT_TIMEOUT = 15

# --- VARIABLES GLOBALES ---
active_socket_connections = {} 
connections_lock = threading.Lock()
producer = None        
gui_queue_global = None 

# --- INICIALIZACIÓN FLASK ---
app_flask = Flask(__name__)
CORS(app_flask) 
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR) 

# -------------------------------------------------------------------------
# SISTEMA DE AUDITORÍA
# -------------------------------------------------------------------------
def log_audit(source_ip, action, description):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"{timestamp} | IP: {source_ip} | ACTION: {action} | DESC: {description}"
    print(f"🔒 [AUDIT] {entry}")
    try:
        with open("system_audit.log", "a", encoding="utf-8") as f:
            f.write(entry + "\n")
    except Exception as e:
        print(f"Error escribiendo auditoría: {e}")

# -------------------------------------------------------------------------
# FUNCIONES BBDD
# -------------------------------------------------------------------------
def get_db_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def update_cp_status_in_db(cp_id, new_status):
    conn = None
    try:
        conn = get_db_connection()
        conn.execute("UPDATE ChargingPoints SET status = ?, last_update = CURRENT_TIMESTAMP WHERE cp_id = ?", (new_status, cp_id))
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] Update status: {e}")
    finally:
        if conn: conn.close()

def register_cp_in_db(cp_id, location, price):
    conn = None
    try:
        conn = get_db_connection()
        # NOTA: En Release 2, el registro real lo hace el EV_Registry.
        # Aquí solo aseguramos que exista por si entra por socket legacy.
        conn.execute("""
            INSERT INTO ChargingPoints (cp_id, location, price_kwh, status, last_heartbeat, last_update)
            VALUES (?, ?, ?, 'DESCONECTADO', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(cp_id) DO UPDATE SET last_update = CURRENT_TIMESTAMP
        """, (cp_id, location, price))
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] Register CP: {e}")
    finally:
        if conn: conn.close()
            
def update_cp_heartbeat(cp_id):
    conn = None
    try:
        conn = get_db_connection()
        conn.execute("UPDATE ChargingPoints SET last_heartbeat = CURRENT_TIMESTAMP WHERE cp_id = ?", (cp_id,))
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] Heartbeat: {e}")
    finally:
        if conn: conn.close()

def get_cp_info_from_db(cp_id):
    conn = None
    info = {'status': None, 'price_kwh': 0.50} 
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT status, price_kwh FROM ChargingPoints WHERE cp_id = ?", (cp_id,))
        result = cursor.fetchone()
        if result:
            info['status'] = result['status']
            info['price_kwh'] = result['price_kwh']
    except sqlite3.Error:
        pass
    finally:
        if conn: conn.close()
    return info

def get_charge_history_for_driver(driver_id):
    logs = []
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT * FROM ChargeLog WHERE driver_id = ? ORDER BY start_time DESC LIMIT 10", (driver_id,))
        for row in cursor.fetchall():
            logs.append(dict(row))
    except sqlite3.Error:
        pass
    finally:
        if conn: conn.close()
    return logs

def get_all_cps_status():
    cps = []
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT cp_id, location, status, price_kwh, last_update FROM ChargingPoints")
        for row in cursor.fetchall():
            cps.append(dict(row))
    finally:
        if conn: conn.close()
    return cps

def get_all_encryption_keys():
    """Recupera todas las claves de cifrado disponibles para intentar descifrar mensajes."""
    keys = {}
    conn = None
    try:
        conn = get_db_connection()
        # Solo traemos los que tienen clave
        cursor = conn.execute("SELECT cp_id, encryption_key FROM ChargingPoints WHERE encryption_key IS NOT NULL")
        for row in cursor.fetchall():
            keys[row['cp_id']] = row['encryption_key']
    except: pass
    finally:
        if conn: conn.close()
    return keys

def broadcast_status_change(kafka_prod, cp_id, new_status, location=None, price=None):
    if not kafka_prod: return
    payload = {'cp_id': cp_id, 'status': new_status}
    if location: payload['location'] = location
    if price: payload['price_kwh'] = price
    try:
        # Los mensajes de estado hacia el Driver/GUI van en claro (públicos)
        kafka_prod.send('topic_status_broadcast', payload)
        kafka_prod.flush()
    except Exception as e:
        print(f"[KAFKA ERROR] Broadcast failed: {e}")

# -------------------------------------------------------------------------
# LÓGICA DE CONTROL GLOBAL
# -------------------------------------------------------------------------
def send_admin_command_global(cp_id, new_status, socket_cmd):
    global producer, active_socket_connections, gui_queue_global
    target_conn = None
    with connections_lock:
        target_conn = active_socket_connections.get(cp_id)
        
    if target_conn:
        try:
            target_conn.sendall(f"{socket_cmd}\n".encode('utf-8'))
        except Exception as e:
            print(f"[API] Error socket {cp_id}: {e}")
    
    update_cp_status_in_db(cp_id, new_status)
    broadcast_status_change(producer, cp_id, new_status)
        
    if gui_queue_global:
        gui_queue_global.put(("ADD_MESSAGE", f"[SISTEMA] {cp_id} -> {new_status}"))
        gui_queue_global.put(("UPDATE_CP", cp_id, new_status, None))
    
    return True

# -------------------------------------------------------------------------
# API REST FLASK
# -------------------------------------------------------------------------
@app_flask.route('/api/cps', methods=['GET'])
def api_list_cps():
    return jsonify(get_all_cps_status())

@app_flask.route('/api/alert/weather', methods=['POST'])
def api_weather_alert():
    data = request.json
    location_alert = data.get('location', '')
    is_cold = data.get('alert', False)
    requester_ip = request.remote_addr 
    
    print(f"🌍 [API CLIMA] Alerta para '{location_alert}': Frío={is_cold}")
    log_audit(requester_ip, "WEATHER_ALERT", f"Alerta clima: {location_alert}. Frio={is_cold}")

    conn = get_db_connection()
    cursor = conn.execute("SELECT cp_id, location, status FROM ChargingPoints")
    affected = []
    for row in cursor.fetchall():
        if location_alert.lower() in row['location'].lower():
            affected.append(dict(row))
    conn.close()

    actions = []
    for cp in affected:
        cp_id = cp['cp_id']
        status = cp['status']
        if is_cold:
            if status not in ('PARADO', 'AVERIADO', 'DESCONECTADO'):
                send_admin_command_global(cp_id, 'PARADO', 'STOP_CP')
                actions.append(f"{cp_id} STOPPED")
        else:
            if status == 'PARADO':
                send_admin_command_global(cp_id, 'ACTIVADO', 'RESUME_CP')
                actions.append(f"{cp_id} RESUMED")

    return jsonify({"status": "processed", "actions": actions})

def run_flask_server():
    app_flask.run(host='0.0.0.0', port=API_PORT, debug=False, use_reloader=False)

# -------------------------------------------------------------------------
# SERVIDOR SOCKETS (LEGACY & AUTH)
# -------------------------------------------------------------------------
def handle_socket_client(conn, addr, producer, gui_queue):
    global active_socket_connections
    source_ip = addr[0]
    print(f"[SOCKET] Conexión: {addr}")
    
    cp_id = None
    try:
        data = conn.recv(1024).decode('utf-8').strip()
        if not data: return
        parts = data.split('\n')[0].split(';') 
        cmd = parts[0]

        if cmd == 'REGISTER':
            cp_id = parts[1]
            # Registro legacy (por si no usa Registry)
            register_cp_in_db(cp_id, parts[2], float(parts[3]))
            update_cp_status_in_db(cp_id, "DESCONECTADO")
            conn.send(b"ACK;REGISTER_OK\n")
            
            with connections_lock:
                active_socket_connections[cp_id] = conn
            
            gui_queue.put(("ADD_MESSAGE", f"CP '{cp_id}' CONECTADO (Socket)."))
            log_audit(source_ip, "AUTH_CP", f"CP {cp_id} conectado vía Socket")
        
        elif cmd == 'GET_HISTORY':
            driver_id = parts[1]
            logs = get_charge_history_for_driver(driver_id)
            conn.sendall(json.dumps(logs).encode('utf-8'))
            conn.close()
            log_audit(source_ip, "DATA_ACCESS", f"Driver {driver_id} solicitó historial")
            return
        else:
            conn.close()
            return

        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data: break
            
            for line in data.strip().split('\n'):
                if not line: continue
                parts = line.split(';')
                
                if parts[0] == 'HEARTBEAT':
                    update_cp_heartbeat(cp_id)
                    conn.send(b"ACK;HEARTBEAT_OK\n")
                
                elif parts[0] == 'STATUS':
                    new_status = parts[1]
                    update_cp_status_in_db(cp_id, new_status)
                    if new_status == 'ACTIVADO': update_cp_heartbeat(cp_id)
                    
                    broadcast_status_change(producer, cp_id, new_status)
                    gui_queue.put(("UPDATE_CP", cp_id, new_status, None))
                    conn.send(b"ACK;STATUS_UPDATED\n")

    except Exception as e:
        print(f"[SOCKET] Error {addr}: {e}")
    finally:
        if cp_id:
            print(f"[SOCKET] Cerrando {cp_id}")
            update_cp_status_in_db(cp_id, "DESCONECTADO")
            broadcast_status_change(producer, cp_id, "DESCONECTADO")
            gui_queue.put(("UPDATE_CP", cp_id, "DESCONECTADO", None))
            with connections_lock:
                active_socket_connections.pop(cp_id, None)
        conn.close()

def start_socket_server(gui_queue):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SOCKET_HOST, SOCKET_PORT))
    server.listen(5)
    print(f"OK [SOCKETS] Escuchando en {SOCKET_PORT}...")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_socket_client, args=(conn, addr, producer, gui_queue), daemon=True).start()

# -------------------------------------------------------------------------
# KAFKA LISTENER CON DESCIFRADO (NUEVO RELEASE 2)
# -------------------------------------------------------------------------
def start_kafka_listener(gui_queue):
    # OJO: Quitamos value_deserializer para recibir bytes crudos (cifrados)
    consumer = KafkaConsumer(
        'topic_requests', 'topic_data_streaming',
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='latest'
    )
    print("OK [KAFKA] Listener iniciado (Modo Seguro).")

    for msg in consumer:
        try:
            data = None
            raw_bytes = msg.value
            
            # --- INTENTO DE DESCIFRADO ---
            if msg.topic == 'topic_data_streaming':
                decrypted = False
                all_keys = get_all_encryption_keys() # Obtenemos claves de BBDD
                
                # 1. Probamos con todas las claves conocidas
                for cp_id, key_str in all_keys.items():
                    try:
                        f = Fernet(key_str.encode('utf-8'))
                        decoded_bytes = f.decrypt(raw_bytes)
                        data = json.loads(decoded_bytes.decode('utf-8'))
                        decrypted = True
                        # print(f"🔓 [SEC] Mensaje descifrado de {cp_id}")
                        break
                    except:
                        continue # Clave incorrecta, probar siguiente
                
                # 2. Fallback: Si no descifra, probar texto plano (por compatibilidad)
                if not decrypted:
                    try:
                        data = json.loads(raw_bytes.decode('utf-8'))
                    except:
                        # print(f"⚠️ [SEC] Mensaje basura o indescifrable en streaming")
                        continue
            else:
                # topic_requests (Drivers) asumimos texto plano
                try:
                    data = json.loads(raw_bytes.decode('utf-8'))
                except: continue

            # --- PROCESAMIENTO DE DATOS (IGUAL QUE ANTES) ---
            if not data: continue

            if msg.topic == 'topic_requests':
                cp_id, driver = data['cp_id'], data['driver_id']
                cp_info = get_cp_info_from_db(cp_id)
                status = cp_info.get('status')
                gui_queue.put(("ADD_REQUEST", datetime.now().strftime("%d/%m"), datetime.now().strftime("%H:%M"), driver, cp_id))

                if status == 'ACTIVADO':
                    update_cp_status_in_db(cp_id, 'ESPERANDO_INICIO')
                    broadcast_status_change(producer, cp_id, 'ESPERANDO_INICIO')
                    producer.send(f'topic_commands_{cp_id}', {
                        'action': 'START_CHARGE', 'driver_id': driver, 'price_kwh': cp_info['price_kwh']
                    })
                    if 'response_topic' in data:
                        producer.send(data['response_topic'], {'status': 'APPROVED', 'cp_id': cp_id})
                    gui_queue.put(("UPDATE_CP", cp_id, "ESPERANDO_INICIO", None))
                    log_audit("KAFKA", "CHARGE_APPROVED", f"Carga aprobada {driver}@{cp_id}")
                else:
                    if 'response_topic' in data:
                        producer.send(data['response_topic'], {'status': 'DENIED', 'cp_id': cp_id, 'reason': status})
                    log_audit("KAFKA", "CHARGE_DENIED", f"Carga denegada {driver}@{cp_id}")

            elif msg.topic == 'topic_data_streaming':
                status, cp_id = data.get('status'), data.get('cp_id')
                
                if status == 'SUMINISTRANDO':
                    conn = get_db_connection()
                    curr = conn.execute("SELECT status FROM ChargingPoints WHERE cp_id=?", (cp_id,)).fetchone()
                    conn.close()
                    if curr and curr['status'] != 'SUMINISTRANDO':
                        update_cp_status_in_db(cp_id, 'SUMINISTRANDO')
                        broadcast_status_change(producer, cp_id, 'SUMINISTRANDO')
                    
                    gui_data = {"driver": data.get('driver_id'), "kwh": f"{data.get('kwh',0):.1f}", "eur": f"{data.get('euros',0):.2f}"}
                    gui_queue.put(("UPDATE_CP", cp_id, "SUMINISTRANDO", gui_data))
                
                elif status in ('FINALIZADO', 'FINALIZADO_AVERIA', 'FINALIZADO_PARADA'):
                    try:
                        c = get_db_connection()
                        c.execute("INSERT INTO ChargeLog (cp_id, driver_id, start_time, end_time, total_kwh, total_euros) VALUES (?,?,?,?,?,?)",
                            (cp_id, data.get('driver_id'), data.get('start_time'), data.get('end_time'), data.get('total_kwh'), data.get('total_euros')))
                        c.commit()
                        c.close()
                    except: pass

                    new_st = 'ACTIVADO'
                    if status == 'FINALIZADO_AVERIA': new_st = 'AVERIADO'
                    elif status == 'FINALIZADO_PARADA': new_st = 'PARADO'
                    
                    update_cp_status_in_db(cp_id, new_st)
                    broadcast_status_change(producer, cp_id, new_st)
                    gui_queue.put(("UPDATE_CP", cp_id, new_st, None))

        except Exception as e:
            print(f"Error Kafka Msg: {e}")

# -------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(PARENT_DIR)
from central_gui import CentralApp

class BackendConnector:
    def __init__(self, gui_queue): pass
    
    def request_parar_cp(self, cp_id):
        log_audit("LOCALHOST", "ADMIN_STOP", f"Parada manual {cp_id}")
        send_admin_command_global(cp_id, 'PARADO', 'STOP_CP')
        
    def request_reanudar_cp(self, cp_id):
        log_audit("LOCALHOST", "ADMIN_RESUME", f"Reanudación manual {cp_id}")
        send_admin_command_global(cp_id, 'ACTIVADO', 'RESUME_CP')

if __name__ == "__main__":
    # 1. Asegurar BBDD
    if not os.path.exists(DB_NAME):
        import init_db, populate_db
        init_db.create_tables()
        populate_db.populate_data()

    # 2. Conectar Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"FATAL: Kafka no disponible ({e}). Asegúrate de que Docker está corriendo.")
        sys.exit(1)

    # 3. Iniciar Hilos y Colas
    gui_queue = queue.Queue()
    gui_queue_global = gui_queue

    threading.Thread(target=start_socket_server, args=(gui_queue,), daemon=True).start()
    threading.Thread(target=start_kafka_listener, args=(gui_queue,), daemon=True).start()
    threading.Thread(target=run_flask_server, daemon=True).start()

    # 4. Iniciar GUI con datos adaptados
    app = CentralApp(gui_queue)
    app.set_controller(BackendConnector(gui_queue))
    
    # --- CORRECCIÓN AQUÍ: Adaptar datos de DB a formato GUI ---
    raw_cps = get_all_cps_status()
    gui_cps = []
    for i, cp in enumerate(raw_cps):
        gui_cps.append({
            "id": cp['cp_id'],                  # La GUI espera "id", la DB tiene "cp_id"
            "loc": cp['location'],              # La GUI espera "loc", la DB tiene "location"
            "price": f"{cp['price_kwh']:.2f}€/kWh",
            "grid_row": i // 5,                 # Calculamos posición en rejilla
            "grid_col": i % 5
        })
    app.load_initial_cps(gui_cps) 
    # ----------------------------------------------------------
    
    print("--- CENTRAL READY (Secure Mode) ---")
    app.mainloop()
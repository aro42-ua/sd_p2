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

# --- NUEVO RELEASE 2: Importamos Flask para la API REST ---
from flask import Flask, jsonify, request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(PARENT_DIR)

from central_gui import CentralApp  

DB_NAME = 'ev_central.db'
SOCKET_HOST = '0.0.0.0'
SOCKET_PORT = 8000     # Puerto para los CPs (Legacy Sockets)
API_PORT = 5000        # NUEVO: Puerto para API REST (Front y Weather)
KAFKA_SERVER = 'localhost:9092'
HEARTBEAT_TIMEOUT = 15

# --- NUEVO RELEASE 2: Inicializamos la aplicación Flask ---
app_flask = Flask(__name__)

# Desactivamos el log de Flask para que no ensucie la consola
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

def get_db_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

# -------------------------------------------------------------------------
# FUNCIONES DE BASE DE DATOS
# -------------------------------------------------------------------------

def update_cp_status_in_db(cp_id, new_status):
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            "UPDATE ChargingPoints SET status = ?, last_update = CURRENT_TIMESTAMP WHERE cp_id = ?",
            (new_status, cp_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al actualizar estado: {e}")
    finally:
        if conn: conn.close()

def register_cp_in_db(cp_id, location, price):
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO ChargingPoints (cp_id, location, price_kwh, status, last_heartbeat, last_update)
            VALUES (?, ?, ?, 'DESCONECTADO', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(cp_id) DO UPDATE SET
                last_update = CURRENT_TIMESTAMP
            """,
            (cp_id, location, price)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al registrar CP: {e}")
    finally:
        if conn: conn.close()
            
def update_cp_heartbeat(cp_id):
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            "UPDATE ChargingPoints SET last_heartbeat = CURRENT_TIMESTAMP WHERE cp_id = ?",
            (cp_id,)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al actualizar heartbeat: {e}")
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
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al leer info de CP: {e}")
    finally:
        if conn: conn.close()
    return info

def get_charge_history_for_driver(driver_id):
    logs = []
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.execute(
            "SELECT * FROM ChargeLog WHERE driver_id = ? ORDER BY start_time DESC LIMIT 10", 
            (driver_id,)
        )
        for row in cursor.fetchall():
            logs.append(dict(row))
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al buscar historial para {driver_id}: {e}")
    finally:
        if conn: conn.close()
    return logs

# --- NUEVO RELEASE 2: Función para devolver TODOS los CPs al Front ---
def get_all_cps_status():
    cps = []
    conn = None
    try:
        conn = get_db_connection()
        # Seleccionamos datos para el dashboard web
        cursor = conn.execute("SELECT cp_id, location, status, price_kwh, last_update FROM ChargingPoints")
        for row in cursor.fetchall():
            cps.append(dict(row))
    finally:
        if conn: conn.close()
    return cps

def broadcast_status_change(producer, cp_id, new_status, location=None, price=None):
    payload = {'cp_id': cp_id, 'status': new_status}
    if location: payload['location'] = location
    if price: payload['price_kwh'] = price
        
    producer.send('topic_status_broadcast', payload)
    producer.flush()

# -------------------------------------------------------------------------
# NUEVO RELEASE 2: ENDPOINTS DE LA API REST (FLASK)
# -------------------------------------------------------------------------

@app_flask.route('/api/cps', methods=['GET'])
def api_list_cps():
    """
    Endpoint para el FRONTEND.
    Devuelve la lista completa de CPs y su estado en formato JSON.
    """
    cps = get_all_cps_status()
    return jsonify(cps)

@app_flask.route('/api/alert/weather', methods=['POST'])
def api_weather_alert():
    """
    Endpoint para EV_W (Weather Control Office).
    Recibe alertas de temperatura baja.
    """
    data = request.json
    location = data.get('location')
    alert = data.get('alert') # True si hace frío
    
    print(f"[API REST] Alerta Clima recibida para {location}: Alerta={alert}")
    
    # Aquí irá la lógica de parar CPs en el futuro.
    return jsonify({"status": "received", "action": "pending_logic"})

def run_flask_server():
    """Ejecuta Flask en un hilo propio en puerto 5000"""
    print(f"OK [API] Servidor REST iniciado en puerto {API_PORT}...")
    app_flask.run(host='0.0.0.0', port=API_PORT, debug=False, use_reloader=False)

# -------------------------------------------------------------------------
# SERVIDOR DE SOCKETS (LEGACY)
# -------------------------------------------------------------------------

def send_socket_message(conn, message_str):
    try:
        conn.sendall(f"{message_str}\n".encode('utf-8'))
    except (BrokenPipeError, ConnectionResetError):
        print(f"[SOCKET_SEND] Error: La conexión ya estaba cerrada.")

def handle_socket_client(conn, addr, producer, active_connections, lock, gui_queue):
    print(f"[SOCKET] Nueva conexión desde: {addr}")
    cp_id_autenticado = None
    try:
        data = conn.recv(1024).decode('utf-8')
        if not data:
            conn.close()
            return
            
        messages = data.strip().split('\n')
        msg = messages[0]
        parts = msg.strip().split(';')
        command = parts[0]

        if command == 'GET_HISTORY':
            driver_id = parts[1]
            history_logs = get_charge_history_for_driver(driver_id)            
            response_json = json.dumps(history_logs)
            conn.sendall(response_json.encode('utf-8'))
            conn.close()
            return 

        elif command == 'REGISTER':
            # Mantenemos esto para que el CP abra el canal de socket
            cp_id_autenticado = parts[1]
            location = parts[2]
            price = float(parts[3])
            
            # Registramos/Actualizamos en DB
            register_cp_in_db(cp_id_autenticado, location, price)
            update_cp_status_in_db(cp_id_autenticado, "DESCONECTADO") 
            
            print(f"CP '{cp_id_autenticado}' conectado al Socket.")
            conn.send(b"ACK;REGISTER_OK\n")
            
            with lock:
                active_connections[cp_id_autenticado] = conn
            
            gui_queue.put(("ADD_MESSAGE", f"CP '{cp_id_autenticado}' CONECTADO (Socket)."))

        else:
            conn.close()
            return

        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data: break 
            
            messages = data.strip().split('\n')
            for msg in messages:
                if not msg: continue
                parts = msg.strip().split(';')
                command = parts[0]
                
                if command == 'HEARTBEAT' and cp_id_autenticado:
                    update_cp_heartbeat(cp_id_autenticado)
                    conn.send(b"ACK;HEARTBEAT_OK\n")
                
                elif command == 'STATUS' and cp_id_autenticado:
                    new_status = parts[1]
                    update_cp_status_in_db(cp_id_autenticado, new_status)
                    if new_status == 'ACTIVADO':
                        update_cp_heartbeat(cp_id_autenticado)
                    
                    broadcast_status_change(producer, cp_id_autenticado, new_status)
                    conn.send(b"ACK;STATUS_UPDATED\n")
                    
                    gui_queue.put(("ADD_MESSAGE", f"CP '{cp_id_autenticado}' estado: {new_status}"))
                    gui_queue.put(("UPDATE_CP", cp_id_autenticado, new_status, None))
                
    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as e:
        print(f"[SOCKET] Error con {addr}: {e}")
    finally:
        if cp_id_autenticado:
            print(f"🔌 [SOCKET] Conexión cerrada con '{cp_id_autenticado}'.")
            update_cp_status_in_db(cp_id_autenticado, "DESCONECTADO")
            broadcast_status_change(producer, cp_id_autenticado, "DESCONECTADO")
            with lock:
                active_connections.pop(cp_id_autenticado, None)
            gui_queue.put(("ADD_MESSAGE", f"CP '{cp_id_autenticado}' Socket cerrado"))
            gui_queue.put(("UPDATE_CP", cp_id_autenticado, "DESCONECTADO", None))
        if conn: conn.close()

def start_socket_server(producer, active_connections, lock, gui_queue):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SOCKET_HOST, SOCKET_PORT))
    server.listen(5)
    print(f"OK [CONTROL] Servidor Sockets escuchando en {SOCKET_PORT}...")
    gui_queue.put(("ADD_MESSAGE", f"Servidor Sockets OK en puerto {SOCKET_PORT}"))

    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=handle_socket_client, args=(conn, addr, producer, active_connections, lock, gui_queue), daemon=True)
        t.start()

# -------------------------------------------------------------------------
# KAFKA Y VIGILANCIA
# -------------------------------------------------------------------------

def start_kafka_listener(producer, gui_queue):
    try:
        consumer = KafkaConsumer(
            'topic_requests', 'topic_data_streaming',
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        print(f"OK [DATOS] Oyente de Kafka conectado...")

        for msg in consumer:
            try:
                data = msg.value
                
                if msg.topic == 'topic_requests':
                    cp_id = data['cp_id']
                    driver_id = data['driver_id']
                    response_topic = data.get('response_topic')
                    
                    now = datetime.now()
                    gui_queue.put(("ADD_REQUEST", now.strftime("%d/%m"), now.strftime("%H:%M"), driver_id, cp_id))

                    cp_info = get_cp_info_from_db(cp_id)
                    status = cp_info['status']
                    price_kwh = cp_info['price_kwh']
                    
                    if status == 'ACTIVADO':
                        # Estado intermedio para evitar zombies
                        update_cp_status_in_db(cp_id, 'ESPERANDO_INICIO')
                        broadcast_status_change(producer, cp_id, 'ESPERANDO_INICIO')
                        
                        producer.send(f'topic_commands_{cp_id}', {
                            'action': 'START_CHARGE', 'driver_id': driver_id, 'price_kwh': price_kwh
                        })
                        
                        if response_topic:
                            producer.send(response_topic, {'status': 'APPROVED', 'cp_id': cp_id, 'driver_id': driver_id})
                        
                        gui_queue.put(("UPDATE_CP", cp_id, "ESPERANDO_INICIO", None))
                        gui_queue.put(("ADD_MESSAGE", f"Petición aprobada para {driver_id} en {cp_id}."))

                    else:
                        if response_topic:
                            producer.send(response_topic, {'status': 'DENIED', 'cp_id': cp_id, 'driver_id': driver_id, 'reason': f"Estado: {status}"})
                        gui_queue.put(("ADD_MESSAGE", f"Petición DENEGADA para {cp_id}"))

                elif msg.topic == 'topic_data_streaming':
                    charge_status = data.get('status')
                    cp_id = data.get('cp_id')

                    if charge_status == 'SUMINISTRANDO':
                        cp_info = get_cp_info_from_db(cp_id)
                        if cp_info['status'] != 'SUMINISTRANDO':
                            update_cp_status_in_db(cp_id, 'SUMINISTRANDO')
                            broadcast_status_change(producer, cp_id, 'SUMINISTRANDO')
                        
                        gui_data = {
                            "driver": data.get('driver_id'),
                            "kwh": f"{data.get('kwh', 0.0):.1f}",
                            "eur": f"{data.get('euros', 0.0):.2f}"
                        }
                        gui_queue.put(("UPDATE_CP", cp_id, "SUMINISTRANDO", gui_data))
                    
                    elif charge_status in ('FINALIZADO', 'FINALIZADO_AVERIA', 'FINALIZADO_PARADA'):
                        conn = get_db_connection()
                        conn.execute("INSERT INTO ChargeLog (cp_id, driver_id, start_time, end_time, total_kwh, total_euros) VALUES (?,?,?,?,?,?)",
                            (cp_id, data.get('driver_id'), data.get('start_time'), data.get('end_time'), data.get('total_kwh'), data.get('total_euros')))
                        conn.commit()
                        conn.close()

                        new_st = 'ACTIVADO'
                        if charge_status == 'FINALIZADO_AVERIA': new_st = 'AVERIADO'
                        elif charge_status == 'FINALIZADO_PARADA': new_st = 'PARADO'

                        update_cp_status_in_db(cp_id, new_st)
                        broadcast_status_change(producer, cp_id, new_st)
                        gui_queue.put(("UPDATE_CP", cp_id, new_st, None))

            except Exception as e:
                print(f"[KAFKA] Error procesando mensaje: {e}")
    except Exception as e:
        print(f"Error fatal en Kafka: {e}")

def check_cp_heartbeats(producer, gui_queue):
    while True:
        time.sleep(5)
        conn = None
        try:
            conn = get_db_connection()
            # Detectar muertos (> 15s sin heartbeat)
            cursor = conn.execute(
                "SELECT cp_id FROM ChargingPoints WHERE status != 'DESCONECTADO' AND (STRFTIME('%s','now') - STRFTIME('%s', last_heartbeat)) > ?", 
                (HEARTBEAT_TIMEOUT,))
            for row in cursor.fetchall():
                cp_id = row['cp_id']
                print(f"ERROR Heartbeat perdido {cp_id}.")
                update_cp_status_in_db(cp_id, 'DESCONECTADO')
                broadcast_status_change(producer, cp_id, 'DESCONECTADO')
                gui_queue.put(("UPDATE_CP", cp_id, "DESCONECTADO", None))
            
            # Limpieza de Zombies (ESPERANDO_INICIO > 15s)
            cursor_stuck = conn.execute(
                "SELECT cp_id FROM ChargingPoints WHERE status = 'ESPERANDO_INICIO' AND (STRFTIME('%s', 'now') - STRFTIME('%s', last_update)) > 15"
            )
            for row in cursor_stuck.fetchall():
                cp_id = row['cp_id']
                update_cp_status_in_db(cp_id, 'ACTIVADO')
                broadcast_status_change(producer, cp_id, 'ACTIVADO')
                gui_queue.put(("UPDATE_CP", cp_id, "ACTIVADO", None))

        except Exception as e:
            print(f"Watchdog error: {e}")
        finally:
            if conn: conn.close()

def get_initial_cps_from_db():
    cps = []
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT cp_id, location, price_kwh FROM ChargingPoints ORDER BY cp_id")
        rows = cursor.fetchall()
        for i, row in enumerate(rows):
            cps.append({
                "id": row['cp_id'],
                "loc": row['location'],
                "price": f"{row['price_kwh']:.2f}€/kWh",
                "grid_row": i // 5,
                "grid_col": i % 5
            })
    except Exception:
        pass
    finally:
        if conn: conn.close()
    return cps

class BackendConnector:
    def __init__(self, producer, active_connections, lock, gui_queue):
        self.producer = producer
        self.active_connections = active_connections
        self.lock = lock
        self.gui_queue = gui_queue

    def request_parar_cp(self, cp_id):
        self._send_admin_command(cp_id, 'PARADO', 'STOP_CP')

    def request_reanudar_cp(self, cp_id):
        self._send_admin_command(cp_id, 'ACTIVADO', 'RESUME_CP')

    def _send_admin_command(self, cp_id, new_status, socket_cmd):
        target_conn = None
        with self.lock:
            target_conn = self.active_connections.get(cp_id)
        
        if target_conn:
            try:
                send_socket_message(target_conn, socket_cmd)
                update_cp_status_in_db(cp_id, new_status)
                broadcast_status_change(self.producer, cp_id, new_status)
                self.gui_queue.put(("ADD_MESSAGE", f"Comando '{new_status}' enviado a {cp_id}."))
                self.gui_queue.put(("UPDATE_CP", cp_id, new_status, None))
            except Exception as e:
                print(f" Error enviando comando a {cp_id}: {e}")
        else:
            self.gui_queue.put(("ADD_MESSAGE", f"ERROR: {cp_id} no está conectado (socket)."))

if __name__ == "__main__":
    active_socket_connections = {} 
    connections_lock = threading.Lock()
    gui_queue = queue.Queue()

    if not os.path.exists(DB_NAME):
        try:
            import init_db, populate_db
            init_db.create_tables()
            populate_db.populate_data()
        except: pass

    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    except Exception as e:
        print(f"Error Kafka: {e}")
        sys.exit(1)

    app = CentralApp(gui_queue)
    backend = BackendConnector(producer, active_socket_connections, connections_lock, gui_queue)
    app.set_controller(backend)
    app.load_initial_cps(get_initial_cps_from_db())

    # Hilo 1: Sockets (Legacy) - Puerto 8000
    t_socket = threading.Thread(target=start_socket_server, args=(producer, active_socket_connections, connections_lock, gui_queue), daemon=True)
    t_socket.start()

    # Hilo 2: Kafka
    t_kafka = threading.Thread(target=start_kafka_listener, args=(producer, gui_queue), daemon=True)
    t_kafka.start()

    # Hilo 3: Watchdog
    t_wd = threading.Thread(target=check_cp_heartbeats, args=(producer, gui_queue), daemon=True)
    t_wd.start()
    
    # Hilo 4: API REST Flask (NUEVO RELEASE 2) - Puerto 5000
    t_flask = threading.Thread(target=run_flask_server, daemon=True)
    t_flask.start()

    print(f"--- CENTRAL INICIADA (Sockets: {SOCKET_PORT} | API REST: {API_PORT}) ---")
    
    try:
        app.mainloop()
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()
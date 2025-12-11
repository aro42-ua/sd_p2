import socket
import threading
import time
import argparse
import sys
import requests  # <--- NUEVO: Librería para peticiones HTTP

central_conn = None
central_lock = threading.Lock()
cp_id_global = None
last_reported_status = "DESCONECTADO" 

engine_conn = None
engine_lock = threading.Lock()
status_lock = threading.Lock()     

# Variables para guardar las credenciales que nos devolverá el Registry
auth_token = None
encryption_key = None

def register_in_registry(registry_ip, registry_port, cp_id, location, price):
    """
    NUEVO: Función que contacta con el Registry vía API REST.
    Devuelve True si el registro es exitoso.
    """
    global auth_token, encryption_key
    
    url = f"http://{registry_ip}:{registry_port}/api/register-cp"
    payload = {
        "cp_id": cp_id,
        "location": location,
        "price": price
    }
    
    print(f"[{cp_id}-Monitor] ⏳ Contactando con Registry en {url}...")
    
    try:
        # Hacemos la petición POST
        response = requests.post(url, json=payload, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            # Guardamos las claves que nos da el Registry (para uso futuro)
            auth_token = data.get('auth_token')
            encryption_key = data.get('encryption_key')
            print(f"[{cp_id}-Monitor] ✅ REGISTRO EXITOSO. Token recibido.")
            return True
        else:
            print(f"[{cp_id}-Monitor] ❌ Error en registro (Code {response.status_code}): {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"[{cp_id}-Monitor] ❌ No se puede conectar con el Registry (¿Está encendido?).")
        return False
    except Exception as e:
        print(f"[{cp_id}-Monitor] ❌ Excepción al registrar: {e}")
        return False

def send_to_central(message_str):
    global central_conn, central_lock
    with central_lock:
        if not central_conn:
            print(f"[{cp_id_global}-Monitor] Error: No conectado a Central.")
            return False
        try:
            central_conn.sendall(f"{message_str}\n".encode('utf-8'))
            return True
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{cp_id_global}-Monitor] Error fatal al enviar a Central: {e}")
            return False

def send_to_engine(message_str):
    global engine_conn, engine_lock
    with engine_lock:
        if not engine_conn:
            print(f"[{cp_id_global}-Monitor] Error: No conectado a Engine.")
            return False
        try:
            engine_conn.sendall(f"{message_str}\n".encode('utf-8'))
            return True
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{cp_id_global}-Monitor] Error fatal al enviar a Engine: {e}")
            return False

def health_check_loop(engine_ip, engine_port, cp_id):
    
    global last_reported_status, engine_conn, engine_lock, status_lock
    
    while True:
        if not engine_conn:
            try:
                print(f"[{cp_id}-Monitor] Conectando a Engine en {engine_ip}:{engine_port}...")
                conn_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn_obj.settimeout(5) # Timeout para no bloquear
                conn_obj.connect((engine_ip, engine_port))
                conn_obj.sendall(f"ID;{cp_id}\n".encode('utf-8'))
                
                response = conn_obj.recv(1024).decode('utf-8').strip()
                if response != "ID_OK":
                    conn_obj.close()
                    time.sleep(5)
                    continue
                
                conn_obj.settimeout(None) # Quitamos timeout para operación normal
                print(f"[{cp_id}-Monitor] OK Conectado y autenticado con Engine.")
                with engine_lock:
                    engine_conn = conn_obj
            except Exception:
                engine_conn = None 
            
            if not engine_conn:
                with status_lock:
                    if last_reported_status != "AVERIADO":
                        print(f"[{cp_id}-Monitor] ERROR Reportando Engine 'AVERIADO' a Central (conexión fallida).")
                        if send_to_central(f"STATUS;AVERIADO"):
                            last_reported_status = "AVERIADO"
                time.sleep(5)
                continue

        try:
            if not send_to_engine("PING"):
                 raise ConnectionResetError("Fallo al enviar PING a Engine")

            response = engine_conn.recv(1024).decode('utf-8').strip()

            current_status = ""
            if response == "OK":
                current_status = "ACTIVADO"
            elif response == "KO":
                current_status = "AVERIADO"
            else:
                raise ConnectionResetError("Respuesta de Engine inválida")

            with status_lock:
                if current_status == "AVERIADO" and last_reported_status == "PARADO":
                    continue 
                
                if current_status == "ACTIVADO" and last_reported_status == "PARADO":
                    continue 
                
                if current_status != last_reported_status:
                    print(f"[{cp_id}-Monitor]  Estado del Engine ha cambiado a {current_status}. Reportando a Central...")
                    if send_to_central(f"STATUS;{current_status}"):
                        last_reported_status = current_status
                    else:
                        print(f"[{cp_id}-Monitor] ERROR No se pudo reportar el nuevo estado a Central.")

        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{cp_id}-Monitor]  Conexión perdida con Engine: {e}. Reintentando...")
            with engine_lock:
                if engine_conn:
                    engine_conn.close()
                engine_conn = None
            
            with status_lock: 
                if last_reported_status != "AVERIADO":
                    if send_to_central("STATUS;AVERIADO"):
                         last_reported_status = "AVERIADO"
        
        time.sleep(1)

def central_heartbeat_loop():
    while True:
        if send_to_central("HEARTBEAT"):
            pass
        else:
            print(f"[{cp_id_global}-Monitor]  Fallo al enviar heartbeat. El hilo principal reconectará.")
            break 
        time.sleep(10)

def main():
    global central_conn, cp_id_global, last_reported_status, engine_conn, engine_lock, status_lock 
    
    parser = argparse.ArgumentParser(description="EV Charging Point - Monitor")
    parser.add_argument('--central-ip', required=True, help="IP de EV_Central")
    parser.add_argument('--central-port', type=int, required=True, help="Puerto de EV_Central")
    parser.add_argument('--engine-ip', required=True, help="IP de EV_CP_E (Engine local)")
    parser.add_argument('--engine-port', type=int, required=True, help="Puerto de EV_CP_E (Engine local)")
    parser.add_argument('--cp-id', required=True, help="ID único de este Charging Point")
    
    # --- NUEVOS ARGUMENTOS PARA EL REGISTRY ---
    parser.add_argument('--registry-ip', required=True, help="IP de EV_Registry")
    parser.add_argument('--registry-port', type=int, default=6000, help="Puerto de EV_Registry")
    
    args = parser.parse_args()
    cp_id_global = args.cp_id

    # Calculamos datos del CP (para enviarlos al Registro y luego a Central)
    location = f"C/{args.cp_id.lower()} 123"
    base_price = 0.50
    bonus_per_char = 0.03
    price = base_price + (len(args.cp_id)) * bonus_per_char

    # -------------------------------------------------------------------
    # PASO 1 (NUEVO): Intentar registrarse en el Registry antes de nada
    # -------------------------------------------------------------------
    registered = False
    while not registered:
        registered = register_in_registry(
            args.registry_ip, 
            args.registry_port, 
            args.cp_id, 
            location, 
            price
        )
        if not registered:
            print(f"[{cp_id_global}-Monitor] Reintentando registro en 5 segundos...")
            time.sleep(5)

    # Una vez registrado, arrancamos el hilo de salud del Engine
    health_thread = threading.Thread(
        target=health_check_loop, 
        args=(args.engine_ip, args.engine_port, args.cp_id), 
        daemon=True
    )
    health_thread.start()

    # -------------------------------------------------------------------
    # PASO 2: Conexión Normal a Central (Sockets)
    # -------------------------------------------------------------------
    while True: 
        heartbeat_thread = None
        try:
            print(f"[{cp_id_global}-Monitor] Conectando a Central en {args.central_ip}:{args.central_port}...")
            conn_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn_obj.connect((args.central_ip, args.central_port))
            
            with central_lock:
                central_conn = conn_obj
            
            # Mantenemos el mensaje REGISTER por compatibilidad con el socket legacy,
            # aunque ya nos hemos registrado por API.
            register_msg = f"REGISTER;{args.cp_id};{location};{price:.2f}"
            
            if not send_to_central(register_msg):
                raise ConnectionError("Fallo al registrarse (Handshake Socket).")
            
            print(f"[{cp_id_global}-Monitor] OK Socket establecido con Central.")
            with status_lock:
                last_reported_status = "DESCONECTADO"
            
            heartbeat_thread = threading.Thread(target=central_heartbeat_loop, daemon=True)
            heartbeat_thread.start()

            while True:
                data = central_conn.recv(1024)
                if not data:
                    raise ConnectionError("Central se desconectó.")
                
                commands = data.decode('utf-8').strip().split('\n')
                for cmd in commands:
                    if not cmd:
                        continue
                        
                    print(f"[{cp_id_global}-Monitor]  Comando recibido de Central: {cmd}")
                    
                    if cmd == "STOP_CP":
                        print(f"[{cp_id_global}-Monitor]    Central ha ordenado PARAR.")
                        with status_lock:
                            last_reported_status = "PARADO"
                        send_to_engine("FORCE_STOP")
                    
                    elif cmd == "RESUME_CP":
                        print(f"[{cp_id_global}-Monitor]   -> ▶️ Central ha ordenado REANUDAR.")
                        with status_lock:
                            last_reported_status = "ACTIVADO"
                        send_to_engine("FORCE_RESUME")

                    elif cmd.startswith("ACK;"):
                        pass
                    else:
                        print(f"[{cp_id_global}-Monitor]    Comando desconocido.")

        except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError, ConnectionError, OSError) as e:
            print(f"[{cp_id_global}-Monitor]  Conexión perdida con Central: {e}. Reintentando en 5s...")
            with central_lock:
                if central_conn:
                    central_conn.close()
                central_conn = None
            if heartbeat_thread and heartbeat_thread.is_alive():
                heartbeat_thread.join(1.0)
        
        time.sleep(5)

if __name__ == "__main__":
    main()
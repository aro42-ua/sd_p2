import socket
import threading
import time
import json
import argparse
from kafka import KafkaConsumer, KafkaProducer
import random
import os
import sys

is_healthy = True 
is_running = True 
state_lock = threading.Lock() 

cp_id_global = None
kafka_producer = None
kafka_broker_global = None


def send_kafka_message(topic, message):
    global kafka_producer
    try:
        if kafka_producer is None:
            print("[KAFKA_ERROR] El productor no está inicializado.")
            return
        kafka_producer.send(topic, message)
        kafka_producer.flush()
    except Exception as e:
        print(f"[KAFKA_ERROR] No se pudo enviar mensaje a {topic}: {e}")

def simulate_charging(driver_id, cp_id, price_kwh):
   
    print(f"Iniciando recarga para {driver_id} en {cp_id} (Precio: {price_kwh} €/kWh)...")
    
    start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    total_kwh = 0
    total_euros = 0
    
    charge_interrupted = None 
    duracion_carga = random.randint(500, 700)
    
    for i in range(duracion_carga):
        
        with state_lock:
            if not is_healthy:
                print(f"\n[{cp_id}] AVERÍA DETECTADA Finalizando suministro...")
                charge_interrupted = "AVERIA"
                break
            if not is_running:
                print(f"\n[{cp_id}] PARADA ADMIN DETECTADA Finalizando suministro...")
                charge_interrupted = "PARADO"
                break

        time.sleep(1)
        kwh_this_second = 0.5
        total_kwh += kwh_this_second
        total_euros += kwh_this_second * price_kwh
        
        telemetry_data = {
            'cp_id': cp_id, 'driver_id': driver_id, 'status': 'SUMINISTRANDO',
            'kwh': round(total_kwh, 2), 'euros': round(total_euros, 2),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        send_kafka_message('topic_data_streaming', telemetry_data)
        print(f"  -> {total_kwh:.2f} kWh | {total_euros:.2f} €")

    end_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    final_status = 'FINALIZADO'
    if charge_interrupted == "AVERIA":
        final_status = 'FINALIZADO_AVERIA'
        print(f"ERROR Recarga INTERRUMPIDA POR AVERÍA para {driver_id}.")
    elif charge_interrupted == "PARADO":
        final_status = 'FINALIZADO_PARADA'
        print(f"ERROR  Recarga PARADA POR CENTRAL para {driver_id}.")
    else:
        print(f"OK  Recarga finalizada para {driver_id}.")
    
    final_data = {
        'cp_id': cp_id, 'driver_id': driver_id, 'status': final_status,
        'start_time': start_time, 'end_time': end_time,
        'total_kwh': round(total_kwh, 2), 'total_euros': round(total_euros, 2)
    }
    send_kafka_message('topic_data_streaming', final_data)

def start_kafka_listener(cp_id, kafka_broker):
    global kafka_producer, kafka_broker_global
    kafka_broker_global = kafka_broker
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        command_topic = f'topic_commands_{cp_id}'
        consumer = KafkaConsumer(
            command_topic,
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        print(f"OK [KAFKA] Oyente de Kafka conectado. Escuchando en '{command_topic}'")
        for msg in consumer:
            data = msg.value
            action = data.get('action')
            if action == 'START_CHARGE':
                driver_id = data.get('driver_id')
                price_kwh = data.get('price_kwh', 0.50)
                charge_thread = threading.Thread(
                    target=simulate_charging, 
                    args=(driver_id, cp_id, price_kwh),
                    daemon=True
                )
                charge_thread.start()
    except Exception as e:
        print(f"ERROR [KAFKA_ERROR] Fallo fatal en el oyente de Kafka: {e}")

def handle_monitor_connection(conn, addr):
  
    global cp_id_global, is_healthy, is_running, state_lock
    print(f" [SOCKET] Monitor conectado desde: {addr}")
    
    cp_identificado = False
    
    try:
        while True:
            data_raw = conn.recv(1024)
            if not data_raw:
                print(" [SOCKET] El Monitor se ha desconectado.")
                break
            
            messages = data_raw.decode('utf-8').strip().split('\n')
            
            for msg in messages:
                if not msg:
                    continue
                
                if msg.startswith('ID;') and not cp_identificado:
                    cp_id = msg.split(';')[1]
                    cp_id_global = cp_id
                    cp_identificado = True
                    print(f" [SOCKET] Este Engine ha sido identificado como: {cp_id}")
                    conn.sendall(b"ID_OK\n")
                    kafka_thread = threading.Thread(
                        target=start_kafka_listener, 
                        args=(cp_id, kafka_broker_global), 
                        daemon=True
                    )
                    kafka_thread.start()

                elif msg == "PING" and cp_identificado:
                    with state_lock:
                        current_health = is_healthy
                    
                    if current_health:
                        conn.sendall(b"OK\n") 
                    else:
                        conn.sendall(b"KO\n") 
                
                elif msg == "FORCE_STOP" and cp_identificado:
                    print(" [SOCKET] Recibida orden de PARADA FORZOSA desde Central.")
                    with state_lock:
                        is_running = False 
                    conn.sendall(b"OK\n") 

                elif msg == "FORCE_RESUME" and cp_identificado:
                    print("▶ [SOCKET] Recibida orden de REANUDACIÓN desde Central.")
                    with state_lock:
                        is_running = True 
                    conn.sendall(b"OK\n") 

                elif not cp_identificado:
                    print("ERROR [SOCKET] El Monitor envió un PING antes de un ID. Cerrando.")
                    conn.sendall(b"ID_FAIL\n")
                    raise ConnectionAbortedError("Protocolo incorrecto")

            time.sleep(0.1)

    except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
        print(f"vaya por dios [SOCKET] Conexión perdida o protocolo fallido con el Monitor {addr}")
    except Exception as e:
        print(f"ERROR [SOCKET] Error en la conexión con el Monitor: {e}")
    finally:
        print(" [SOCKET] Cerrando socket del Monitor.")
        conn.close()

def start_socket_server(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(('0.0.0.0', port))
        server.listen(1)
        print(f"OK [SOCKET] Engine Server escuchando en el puerto {port}...")
        while True:
            conn, addr = server.accept()
            monitor_thread = threading.Thread(
                target=handle_monitor_connection, 
                args=(conn, addr), 
                daemon=True
            )
            monitor_thread.start()
    except Exception as e:
        print(f"ERROR [SOCKET_ERROR] No se pudo iniciar el servidor en el puerto {port}: {e}")
    finally:
        server.close()

def failure_simulator():
    
    global is_healthy, state_lock
    print("OK [SIMULADOR] Simulador de averías iniciado.")
    print("  Presiona [Enter] en esta terminal para simular/resolver una avería ")
    
    while True:
        try:
            input() 
            
            with state_lock:
                is_healthy = not is_healthy
                status = "BIEN OK" if is_healthy else "AVERIADO ERROR"
            print(f"\n [SIMULADOR] ¡Estado de SALUD cambiado! Ahora está: {status}\n")

        except EOFError:
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EV Charging Point - Engine")
    parser.add_argument('--socket-port', type=int, required=True, help="Puerto para el socket del Monitor")
    parser.add_argument('--kafka-broker', type=str, required=True, help="IP:Puerto del broker de Kafka")
    args = parser.parse_args()
    
    kafka_broker_global = args.kafka_broker

    fail_thread = threading.Thread(target=failure_simulator, daemon=True)
    fail_thread.start()

    socket_server_thread = threading.Thread(
        target=start_socket_server, 
        args=(args.socket_port,), 
        daemon=True
    )
    socket_server_thread.start()


    print(f"OK [Engine] Módulos iniciados. (PID: {os.getpid()}). Presiona Ctrl+C para salir.")
    try:
        while True:
        
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n [Engine] Cerrando... (Ctrl+C detectado)")
        sys.exit(0)
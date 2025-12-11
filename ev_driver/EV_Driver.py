import sys
import time
import json
import threading
import queue
import os
import socket 
import random
from kafka import KafkaConsumer, KafkaProducer
from tkinter import messagebox

from driver_gui import DriverApp

class BackendController:
    
    def __init__(self, gui_queue):
        self.gui_queue = gui_queue
        self.producer = None
        self.driver_id = None
        self.response_topic = None
        self.kafka_broker = None
        
        self.active_cp_id = None
        self.state_lock = threading.Lock()
        self.state_file = None
        self.service_finished_event = threading.Event()

    def connect(self, driver_id, broker):
        
        self.driver_id = driver_id
        self.kafka_broker = broker
        self.response_topic = f'topic_driver_{self.driver_id}'
        self.state_file = f"driver_state_{self.driver_id}.json"
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            self.gui_queue.put(("ADD_MESSAGE", "Productor Kafka OK."))
        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f"ERROR Error conectando Productor: {e}"))
            return

        history_thread = threading.Thread(
            target=self._fetch_offline_history,
            daemon=True
        )
        history_thread.start()

        consumer_thread = threading.Thread(
            target=self._start_kafka_listener,
            daemon=True
        )
        consumer_thread.start()
        time.sleep(1) 
        
    def _fetch_offline_history(self):
        
        self.gui_queue.put(("ADD_MESSAGE", "Comprobando historial de recargas..."))
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            central_host = self.kafka_broker.split(':')[0]
            sock.connect((central_host, 8000)) 
            
            request_msg = f"GET_HISTORY;{self.driver_id}\n"
            sock.sendall(request_msg.encode('utf-8'))
            
            response_data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response_data += chunk
            
            sock.close()

            logs = json.loads(response_data.decode('utf-8'))
            if logs:
                self.gui_queue.put(("ADD_MESSAGE", f"--- {len(logs)} Recarga(s) Offline Recuperada(s) ---"))
                for log in logs:
                    self.gui_queue.put(("ADD_MESSAGE", f"  CP: {log['cp_id']}"))
                    self.gui_queue.put(("ADD_MESSAGE", f"  Total: {log.get('total_kwh', 0):.2f} kWh | {log.get('total_euros', 0):.2f} €"))
                    self.gui_queue.put(("ADD_MESSAGE", f"  Terminada: {log['end_time']}"))
                    self.gui_queue.put(("ADD_MESSAGE", "--------------------"))
            else:
                self.gui_queue.put(("ADD_MESSAGE", "No hay recargas offline pendientes."))

        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f" Error recuperando historial: {e}"))
            print(f"Error recuperando historial: {e}")
            
    # (Dentro de la clase BackendController en EV_Driver.py)

    def _start_kafka_listener(self):
        """
        Inicia el consumidor. Escucha en 3 tópicos.
        """
        try:
            # --- INICIO DE LA CORRECCIÓN ---
            consumer = KafkaConsumer(
                self.response_topic,        # Tópico de respuesta de Central
                'topic_data_streaming',   # Tópico de telemetría de los Engines
                'topic_status_broadcast', # Tópico de estado de TODOS los CPs
                bootstrap_servers=self.kafka_broker,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
               
                # ¡ESTA LÍNEA ES LA CORRECCIÓN!
                # Le da una ID fija al consumidor para que Kafka guarde los mensajes
                # si el Driver se cae o si el mensaje llega demasiado rápido.
                group_id=f'driver_group_{self.driver_id}',
               
                auto_offset_reset='latest'
            )
            # --- FIN DE LA CORRECCIÓN ---
           
            self.gui_queue.put(("ADD_MESSAGE", "Oyente Kafka OK. Escuchando..."))

            for msg in consumer:
                data = msg.value
                msg_cp_id = data.get('cp_id')

                # --- LÓGICA DE SINCRONIZACIÓN ---
                with self.state_lock:
                    if msg.topic != 'topic_status_broadcast' and msg_cp_id != self.active_cp_id:
                        continue
                # --- FIN LÓGICA ---
           
                # --- Tópico 1: Respuesta de Central (Aprobado/Denegado) ---
                if msg.topic == self.response_topic:
                    status = data.get('status')
                    if status == 'APPROVED':
                        msg_txt = f" SOLICITUD APROBADA para {data.get('cp_id')}"
                        self.gui_queue.put(("ADD_MESSAGE", msg_txt))
                        self.gui_queue.put(("ADD_MESSAGE", "...Esperando telemetría..."))
                   
                    elif status == 'DENIED':
                        msg_txt = f" SOLICITUD DENEGADA para {data.get('cp_id')}"
                        self.gui_queue.put(("ADD_MESSAGE", msg_txt))
                        self.gui_queue.put(("ADD_MESSAGE", f"   Razón: {data.get('reason')}"))
                        with self.state_lock:
                            self.active_cp_id = None
                        self.service_finished_event.set() # <-- Esto desbloqueará el hilo

                # --- Tópico 2: Telemetría del Engine (Suministro/Finalizado) ---
                elif msg.topic == 'topic_data_streaming':
                    if data.get('driver_id') != self.driver_id:
                        continue
                   
                    status = data.get('status')
                   
                    if status == 'SUMINISTRANDO':
                        self.gui_queue.put(("UPDATE_CHARGE", data))
                   
                    elif status in ('FINALIZADO', 'FINALIZADO_AVERIA', 'FINALIZADO_PARADA'):
                        self.gui_queue.put(("ADD_MESSAGE", "\n" + "="*20))
                        if status == 'FINALIZADO':
                            self.gui_queue.put(("ADD_MESSAGE", f" Recarga FINALIZADA."))
                        elif status == 'FINALIZADO_AVERIA':
                            self.gui_queue.put(("ADD_MESSAGE", f" Recarga INTERRUMPIDA POR AVERÍA."))
                        elif status == 'FINALIZADO_PARADA':
                             self.gui_queue.put(("ADD_MESSAGE", f" Recarga PARADA por la Central."))
                       
                        self.gui_queue.put(("ADD_MESSAGE", f"   Total: {data.get('total_kwh'):.2f} kWh"))
                        self.gui_queue.put(("ADD_MESSAGE", f"   Coste: {data.get('total_euros'):.2f} €"))
                        self.gui_queue.put(("ADD_MESSAGE", "="*20 + "\n"))
                       
                        self.gui_queue.put(("RESET_CHARGE", None))
                        with self.state_lock:
                            self.active_cp_id = None
                        self.service_finished_event.set() # <-- Esto desbloqueará el hilo
               
                # --- Tópico 3: Estado de TODOS los CPs (para la lista) ---
                elif msg.topic == 'topic_status_broadcast':
                    self.gui_queue.put(("UPDATE_CP_LIST", data))

        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f" Error fatal en oyente Kafka: {e}"))
            
    def _send_request(self, cp_id):
        if not self.producer:
            self.gui_queue.put(("ADD_MESSAGE", "Error: Productor no conectado."))
            return
        
        with self.state_lock:
            self.active_cp_id = cp_id
            
        request_payload = {
            'driver_id': self.driver_id,
            'cp_id': cp_id,
            'response_topic': self.response_topic
        }
        
        try:
            self.producer.send('topic_requests', request_payload)
            self.producer.flush()
            self.gui_queue.put(("ADD_MESSAGE", f"[{self.driver_id}] Solicitud para {cp_id} enviada..."))
        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f" Error enviando solicitud: {e}"))
            with self.state_lock:
                self.active_cp_id = None
            
    
    def request_manual_service(self, cp_id):
        self.gui_queue.put(("ADD_MESSAGE", "\n--- Nueva Petición Manual ---"))
        
        with self.state_lock:
            if self.active_cp_id is not None:
                self.gui_queue.put(("ADD_MESSAGE", f"Error: Ya hay una carga en curso en {self.active_cp_id}."))
                return
        
        self._send_request(cp_id)
        
    def start_file_services(self, file_path):
        self.gui_queue.put(("ADD_MESSAGE", f"Iniciando servicios desde: {file_path}"))
        file_thread = threading.Thread(
            target=self._run_file_loop,
            args=(file_path,),
            daemon=True
        )
        file_thread.start()

    def _load_driver_state(self):
        if not os.path.exists(self.state_file):
            return 0
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                return state.get('next_service_index', 0)
        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f"Advertencia: No se pudo leer estado. Empezando de cero. ({e})"))
            return 0

    def _save_driver_state(self, next_index):
        try:
            with open(self.state_file, 'w') as f:
                json.dump({'next_service_index': next_index}, f)
        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f" Error CRÍTICO: No se pudo guardar el estado."))
            
    def _clear_driver_state(self):
        if os.path.exists(self.state_file):
            os.remove(self.state_file)


    def _run_file_loop(self, file_path):
        
        try:
            with open(file_path, 'r') as f:
                services = [line.strip() for line in f if line.strip()]
            random.shuffle(services)
            self.gui_queue.put(("ADD_MESSAGE", f"Servicios (barajados) a solicitar: {services}"))
            
        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f" Error leyendo archivo: {e}"))
            return
            
        if not services:
            self.gui_queue.put(("ADD_MESSAGE", "Archivo de servicios vacío."))
            return
            
        start_index = self._load_driver_state()
        total_services = len(services)
        
        if start_index >= total_services:
            self.gui_queue.put(("ADD_MESSAGE", "Todos los servicios ya estaban completados. Limpiando estado."))
            self._clear_driver_state()
            return
        elif start_index > 0:
            self.gui_queue.put(("ADD_MESSAGE", f"Reanudando desde el servicio {start_index + 1} de {total_services}..."))

        for i in range(start_index, total_services):
            cp_id = services[i]
            
            
            # 1. Comprobamos el estado DENTRO de un cerrojo corto
            needs_to_wait = False
            with self.state_lock:
                if self.active_cp_id == cp_id:
                    self.gui_queue.put(("ADD_MESSAGE", f"Ya hay una carga activa en {cp_id}, esperando a que termine..."))
                    needs_to_wait = True

            # 2. Si no estaba activo, enviamos la solicitud FUERA del cerrojo
            if not needs_to_wait:
                self.gui_queue.put(("ADD_MESSAGE", "\n" + "="*30))
                self.gui_queue.put(("ADD_MESSAGE", f"[{self.driver_id}] Solicitando servicio {i+1}/{total_services} en: {cp_id}"))
                
                # Esta llamada ya no está dentro del 'with self.state_lock' de este bucle
                self._send_request(cp_id) 
            

            self.service_finished_event.wait()
            self._save_driver_state(i + 1)
            self.service_finished_event.clear()
            
            self.gui_queue.put(("ADD_MESSAGE", "...Esperando 4 segundos..."))
            time.sleep(4)
        
        self.gui_queue.put(("ADD_MESSAGE", "\n" + "="*30))
        self.gui_queue.put(("ADD_MESSAGE", "Todos los servicios del archivo han sido procesados."))
        self._clear_driver_state()

    def close_producer(self):
        if self.producer:
            self.producer.close()
            print("Productor de Kafka cerrado.")

def main():
    gui_queue = queue.Queue()
    app = DriverApp(gui_queue)
    backend = BackendController(gui_queue)
    app.set_controller(backend)
    
    def on_closing():
        if messagebox.askokcancel("Salir", "seguro que quieres salir?"):
            backend.close_producer()
            app.destroy()
            
    app.protocol("WM_DELETE_WINDOW", on_closing)
    
    try:
        app.mainloop()
    except KeyboardInterrupt:
        backend.close_producer()
        app.destroy()

if __name__ == "__main__":
    main()
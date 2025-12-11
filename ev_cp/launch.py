import os
import time
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

CENTRAL_IP = "localhost"
CENTRAL_PORT = "8000"
REGISTRY_IP = "localhost"   # <--- NUEVO: IP del Registry
REGISTRY_PORT = "6000"      # <--- NUEVO: Puerto del Registry
KAFKA_BROKER = "localhost:9092"
ENGINE_IP = "localhost" 

CP_IDS = ["CRT1", "MAD1", "ALC1"] 
BASE_PORT = 6000 
NUM_CPS = len(CP_IDS)

PYTHON_EXECUTABLE = sys.executable 
if not PYTHON_EXECUTABLE:
    PYTHON_EXECUTABLE = 'python'

def build_engine_command(cp_id, port):
    engine_path = os.path.join(PROJECT_ROOT, "EV_CP_E.py")
    engine_cmd = (
        f'"{PYTHON_EXECUTABLE}" "{engine_path}" '  
        f"--socket-port {port} "
        f"--kafka-broker {KAFKA_BROKER}"
    )
    return f'start "CP {cp_id} - ENGINE {port}" cmd /k "{engine_cmd}"'

def build_monitor_command(cp_id, port):
    monitor_path = os.path.join(PROJECT_ROOT, "EV_CP_M.py")
    
    # AÑADIMOS LOS ARGUMENTOS DEL REGISTRY QUE FALTABAN
    monitor_cmd = (
        f'"{PYTHON_EXECUTABLE}" "{monitor_path}" ' 
        f"--cp-id {cp_id} "
        f"--central-ip {CENTRAL_IP} "
        f"--central-port {CENTRAL_PORT} "
        f"--engine-ip {ENGINE_IP} "
        f"--engine-port {port} "
        f"--registry-ip {REGISTRY_IP} "
        f"--registry-port {REGISTRY_PORT}"
    )
    return f'start "CP {cp_id} - MONITOR" cmd /k "{monitor_cmd}"'

if __name__ == "__main__":
    print(f"--- INICIANDO {NUM_CPS} PARES CP (Release 2) ---")
    
    for i in range(NUM_CPS):
        cp_id = CP_IDS[i]
        port = BASE_PORT + i + 1 
        
        os.system(build_engine_command(cp_id, port))
        time.sleep(1) 
        os.system(build_monitor_command(cp_id, port))
        time.sleep(1) 
        
    print("\n[LAUNCHER] CPs lanzados. Asegúrate de que EV_Registry y EV_Central estén corriendo.")
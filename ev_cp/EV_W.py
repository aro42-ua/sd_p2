import time
import requests
import json
import random
import sys

# --- CONFIGURACIÓN ---
CENTRAL_API_URL = "http://localhost:5000/api/alert/weather"
INTERVALO_SEGUNDOS = 4

# Si tienes una API KEY real, ponla aquí. Si no, déjalo vacío para usar MODO SIMULADO.
OPENWEATHER_API_KEY = ""  
# Ejemplo: "a1b2c3d4e5f6..."

# Mapeo de CPs a Ciudades (Según tu BBDD/Enunciado)
# En un sistema real, esto podría pedirse a la API de Central, 
# pero por simplicidad lo definimos aquí.
UBICACIONES = {
    "ALC1": "Alicante,ES",
    "MAD1": "Madrid,ES",
    "CRT1": "Ciudad Real,ES" # O la ciudad que sea CRT
}

# Estado interno para no spammear a la Central si el clima no cambia
ultimo_estado_alerta = {cp_id: False for cp_id in UBICACIONES}

def obtener_clima_real(ciudad):
    """Consulta la API real de OpenWeather."""
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={OPENWEATHER_API_KEY}&units=metric"
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            data = response.json()
            temp = data['main']['temp']
            return temp
        else:
            print(f"[EV_W] Error API OpenWeather ({ciudad}): {response.status_code}")
            return None
    except Exception as e:
        print(f"[EV_W] Excepción conectando a OpenWeather: {e}")
        return None

def obtener_clima_simulado(ciudad):
    """
    Genera una temperatura aleatoria para pruebas.
    Permite probar el sistema sin internet o sin API Key.
    """
    # Simulamos que Madrid hace frío a veces
    if "Madrid" in ciudad:
        return random.uniform(-5, 10) 
    # Alicante siempre buena temperatura ;)
    elif "Alicante" in ciudad:
        return random.uniform(10, 25)
    else:
        return random.uniform(-2, 15)

def main():
    print(f"--- WEATHER CONTROL OFFICE (EV_W) INICIADO ---")
    print(f"--- Monitorizando {len(UBICACIONES)} ubicaciones cada {INTERVALO_SEGUNDOS}s ---")
    
    modo_simulado = not bool(OPENWEATHER_API_KEY)
    if modo_simulado:
        print("⚠️  AVISO: API KEY no configurada. Usando MODO SIMULACIÓN (Temperaturas aleatorias).")
    else:
        print("✅ MODO REAL: Conectando con OpenWeatherMap.")

    while True:
        print("\n[EV_W] 🔍 Consultando clima...")
        
        for cp_id, ciudad in UBICACIONES.items():
            # 1. Obtener Temperatura
            if modo_simulado:
                temp = obtener_clima_simulado(ciudad)
            else:
                temp = obtener_clima_real(ciudad)
            
            if temp is None:
                continue

            # 2. Evaluar Regla de Negocio (< 0ºC)
            hace_frio_extremo = temp < 0
            
            # Solo notificamos si el estado de alerta ha cambiado para no saturar
            # (O si hace frío, insistimos por seguridad)
            if hace_frio_extremo != ultimo_estado_alerta[cp_id] or hace_frio_extremo:
                
                tipo_alerta = "❄️ ALERTA FRÍO" if hace_frio_extremo else "☀️ CLIMA OK"
                print(f"   -> {cp_id} ({ciudad}): {temp:.1f}ºC | {tipo_alerta}")
                
                # 3. Notificar a Central (API REST)
                payload = {
                    "location": ciudad,
                    "cp_id": cp_id, # Enviamos el ID para ayudar a Central
                    "alert": hace_frio_extremo,
                    "temp": temp
                }
                
                try:
                    resp = requests.post(CENTRAL_API_URL, json=payload)
                    if resp.status_code == 200:
                        print(f"      [API] Notificación enviada a Central: OK")
                        ultimo_estado_alerta[cp_id] = hace_frio_extremo
                    else:
                        print(f"      [API] Error enviando a Central: {resp.status_code}")
                except Exception as e:
                    print(f"      [API] Fallo de conexión con Central: {e}")
            else:
                print(f"   -> {cp_id}: {temp:.1f}ºC (Sin cambios)")

        time.sleep(INTERVALO_SEGUNDOS)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[EV_W] Apagando servicio de clima...")
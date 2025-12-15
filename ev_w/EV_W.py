import time
import requests
import json

# --- CONFIGURACIÓN ---
CENTRAL_API_URL = "http://localhost:5000/api/alert/weather"

# Si tienes API KEY de OpenWeatherMap, ponla aquí. Si no, usa el modo manual.
API_KEY = "TU_API_KEY_AQUI" 
CIUDADES_VIGILADAS = ["Alicante", "Madrid", "Barcelona", "Valencia", "Sevilla"]

def check_weather_api(city):
    """Consulta real a OpenWeatherMap."""
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        res = requests.get(url, timeout=5)
        if res.status_code == 200:
            return res.json()['main']['temp']
    except Exception as e:
        print(f"Error API clima: {e}")
    return None

def send_alert(city, is_cold):
    """Envía la alerta a tu Central (EV_Central)."""
    payload = {
        "location": city,
        "alert": is_cold  # True = Frio (Parar), False = Normal (Reanudar)
    }
    try:
        res = requests.post(CENTRAL_API_URL, json=payload)
        print(f" -> Enviado {city} (Frio={is_cold}): {res.status_code} - {res.json()}")
    except Exception as e:
        print(f" -> Error conectando con Central: {e}")

def manual_mode():
    """Modo interactivo para probar sin esperar al clima real."""
    print("\n--- MODO MANUAL DE CLIMA ---")
    print("Escribe una temperatura para simular (ej: -5 para parar, 20 para reanudar).")
    
    while True:
        try:
            val = input(f"\nIntroduce Temp para {CIUDADES_VIGILADAS}: ")
            temp = float(val)
            is_cold = temp < 0
            
            print(f"Simulando {temp}ºC (Alerta Frío: {is_cold})...")
            
            # Enviamos la alerta para TODAS las ciudades para ver el efecto rápido
            for city in CIUDADES_VIGILADAS:
                send_alert(city, is_cold)
                time.sleep(0.1)
                
        except ValueError:
            print("Por favor, introduce un número válido.")
        except KeyboardInterrupt:
            print("\nSaliendo...")
            break

if __name__ == "__main__":
    print("Iniciando EV_W (Weather Control Office)...")
    
    # Elige el modo que prefieras:
    manual_mode()
    
    # MODO AUTOMÁTICO (Descomentar si tienes API Key)
    # while True:
    #     for city in CIUDADES_VIGILADAS:
    #         t = check_weather_api(city)
    #         if t is not None:
    #             print(f"Clima en {city}: {t}ºC")
    #             send_alert(city, t < 0)
    #     time.sleep(60)
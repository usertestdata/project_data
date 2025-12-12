import json
import random
from datetime import datetime
import pandas as pd

df = pd.read_csv("./datos_vuelos_historicos_complex.csv")
VALID_FLIGHT_IDS = df["ID_Vuelo"].tolist()

def generate_event_hub_message():
    """Genera un evento de tracking aleatorio."""
    
    flight_id = random.choice(VALID_FLIGHT_IDS)
    now = datetime.utcnow().isoformat() + "Z"
    
    estados = ["En Vuelo", "Aterrizado", "Retrasado", "Cancelado", "Puerta Asignada"]
    estado = random.choice(estados)
    
    retraso = 0
    nivel_servicio = 1
    if estado == "Retrasado":
        retraso = random.randint(15, 180)
        nivel_servicio = random.choice([2, 3]) # El bajo servicio genera retraso
    elif estado == "Cancelado":
        retraso = 999
        nivel_servicio = 3
        
    combustible = random.uniform(5000.0, 30000.0)
    
    event_data = {
        "ID_Vuelo": flight_id,
        "Timestamp_Evento": now,
        "Estado_Actual": estado,
        "Retraso_Minutos": retraso,
        "Puerta_Asignada": f"{random.choice(['A', 'B', 'C'])}{random.randint(1, 20)}",
        "Ruta_Evento": "SCL->MIA", 
        "Nivel_Servicio": nivel_servicio,
        "Consumo_Combustible_Litros": round(combustible, 2)
    }
    
    # El mensaje JSON se enviaría al Event Hub
    return json.dumps(event_data)


print(generate_event_hub_message())
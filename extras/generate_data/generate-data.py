import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


# --- Parámetros de Generación ---
N_REGISTROS = 10000
AEROLINEAS = ["AirCorp", "AeroSky", "GlobalAir", "FlyFast"]
AEROPUERTOS = ["SCL", "MIA", "JFK", "MAD", "BUE", "LIM", "FRA", "CDG"]
TIPOS_AVION = {"B787": 300, "A320": 180, "B737": 210, "A380": 550}
CLASES = ["Económica", "Negocios", "Primera"]


data = {
    "ID_Vuelo": [f"{random.choice(['AA', 'LA', 'IB', 'DL'])}{random.randint(100, 9999)}" for _ in range(N_REGISTROS)],
    "Aerolinea": np.random.choice(AEROLINEAS, N_REGISTROS),
    "Origen": np.random.choice(AEROPUERTOS, N_REGISTROS),
    "Destino": np.random.choice(AEROPUERTOS, N_REGISTROS),
}

# Asegurar Origen != Destino
for i in range(N_REGISTROS):
    while data["Origen"][i] == data["Destino"][i]:
        data["Destino"][i] = random.choice(AEROPUERTOS)

# Generación de Fechas
start_date = datetime(2025, 1, 1)
data["Fecha_Programada"] = [(start_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d') for _ in range(N_REGISTROS)]
data["Hora_Salida_UTC"] = [f"{random.randint(0, 23):02d}:{random.choice(['00', '30'])}" for _ in range(N_REGISTROS)]

# Capacidad y Ocupación
data["Tipo_Avion"] = np.random.choice(list(TIPOS_AVION.keys()), N_REGISTROS)
data["Capacidad_Pasajeros"] = [TIPOS_AVION[tipo] for tipo in data["Tipo_Avion"]]
data["Pasajeros_Reservados"] = [int(cap * random.uniform(0.6, 1.0)) for cap in data["Capacidad_Pasajeros"]]

# Distancia (simulación basada en el tipo de avión)
data["Distancia_KM"] = [int(random.uniform(2000, 10000) if tipo in ["B787", "A380"] else random.uniform(500, 4500)) 
                        for tipo in data["Tipo_Avion"]]

# Tarifas
data["Clase_Tarifa"] = np.random.choice(CLASES, N_REGISTROS)
base_cost = []
taxes = []
for clase in data["Clase_Tarifa"]:
    if clase == "Económica":
        base_cost.append(random.uniform(150, 600))
        taxes.append(random.uniform(50, 100))
    elif clase == "Negocios":
        base_cost.append(random.uniform(800, 2500))
        taxes.append(random.uniform(100, 300))
    else: # Primera
        base_cost.append(random.uniform(3000, 8000))
        taxes.append(random.uniform(300, 800))

data["Costo_Base"] = [round(c, 2) for c in base_cost]
data["Impuestos"] = [round(t, 2) for t in taxes]


# Crear DataFrame y guardar en CSV
df_historicos = pd.DataFrame(data)
df_historicos.to_csv("datos_vuelos_historicos_complex.csv", index=False)
print(f"CSV generado con {len(df_historicos)} registros.")
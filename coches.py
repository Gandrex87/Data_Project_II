from google.cloud import pubsub_v1
import json
import xml.etree.ElementTree as ET
import time
import threading
import os

# Configura el cliente de Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('civic-summer-413119', 'pro-bucket1')

def publicar_coordenada(coordenada, car_id, route_id):
    message_data = {
        "car_id": car_id,
        "destino_coche": route_id,
        "lat": coordenada[0],
        "lon": coordenada[1],
        "plazas_disponibles": 3,
    }
    data_bytes = json.dumps(message_data).encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(f"mensaje coches: {data_bytes}")
    return future.result()

def extraer_datos_kml(file_path, car_id):
    tree = ET.parse(file_path)
    root = tree.getroot()
    namespace = {'kml': 'http://www.opengis.net/kml/2.2'}
    route_id = os.path.basename(file_path).split('.')[0]

    for placemark in root.findall(".//kml:Placemark", namespace):
        for line_string in placemark.findall(".//kml:LineString", namespace):
            for coord in line_string.find(".//kml:coordinates", namespace).text.split():
                lon, lat = coord.split(',')[:2]
                publicar_coordenada((float(lat), float(lon)), car_id, route_id)
                time.sleep(0.5)

# Crear y empezar los hilos para diferentes coches
h_coche1 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Av_Baleares_a_Oceanografic.kml", "coche_1"))
h_coche2 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Benicalap_a_Ensanche.kml", "coche_2"))

h_coche1.start()
time.sleep(1)
h_coche2.start()

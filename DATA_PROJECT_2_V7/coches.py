from google.cloud import pubsub_v1
import json
import xml.etree.ElementTree as ET
import time
import threading
import os
import random

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
        "precio_por_viaje" : 0,
        "personas_transportadas": 0
    }
    data_bytes = json.dumps(message_data).encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(f"mensaje coches: {data_bytes}")
    return future.result()


def extraer_datos_kml(car_id, ruta_dir="./Rutas"):
    while True:  
        archivos_ruta = [f for f in os.listdir(ruta_dir) if f.endswith('.kml')]
        file_path = os.path.join(ruta_dir, random.choice(archivos_ruta))
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
        time.sleep(7)



# Creacion y empezar los hilos para diferentes coches
h_coche1 = threading.Thread(target=extraer_datos_kml, args=("coche_1",))
h_coche2 = threading.Thread(target=extraer_datos_kml, args=("coche_2",))
h_coche3 = threading.Thread(target=extraer_datos_kml, args=("coche_3",))
h_coche4 = threading.Thread(target=extraer_datos_kml, args=("coche_4",))
h_coche5 = threading.Thread(target=extraer_datos_kml, args=("coche_5",))
h_coche6 = threading.Thread(target=extraer_datos_kml, args=("coche_6",))
h_coche7 = threading.Thread(target=extraer_datos_kml, args=("coche_7",))
h_coche8 = threading.Thread(target=extraer_datos_kml, args=("coche_8",))
h_coche9 = threading.Thread(target=extraer_datos_kml, args=("coche_9",))
h_coche10 = threading.Thread(target=extraer_datos_kml, args=("coche_10",))
h_coche20 = threading.Thread(target=extraer_datos_kml, args=("coche_20",))
h_coche21 = threading.Thread(target=extraer_datos_kml, args=("coche_21",))
h_coche22 = threading.Thread(target=extraer_datos_kml, args=("coche_22",))
h_coche23 = threading.Thread(target=extraer_datos_kml, args=("coche_23",))
h_coche24 = threading.Thread(target=extraer_datos_kml, args=("coche_24",))
h_coche25 = threading.Thread(target=extraer_datos_kml, args=("coche_25",))
h_coche30 = threading.Thread(target=extraer_datos_kml, args=("coche_30",))
h_coche31 = threading.Thread(target=extraer_datos_kml, args=("coche_31",))
h_coche32 = threading.Thread(target=extraer_datos_kml, args=("coche_32",))
h_coche33 = threading.Thread(target=extraer_datos_kml, args=("coche_33",))
h_coche34 = threading.Thread(target=extraer_datos_kml, args=("coche_34",))
h_coche35 = threading.Thread(target=extraer_datos_kml, args=("coche_35",))
h_coche36 = threading.Thread(target=extraer_datos_kml, args=("coche_36",))
h_coche37 = threading.Thread(target=extraer_datos_kml, args=("coche_37",))
h_coche38 = threading.Thread(target=extraer_datos_kml, args=("coche_38",))


h_coche1.start()
h_coche2.start()
h_coche3.start()
h_coche4.start()
h_coche5.start()
h_coche6.start()
h_coche7.start()
h_coche8.start()
h_coche9.start()
h_coche10.start()
h_coche20.start()
h_coche21.start()
h_coche22.start()
h_coche23.start()
time.sleep(1)
h_coche24.start()
h_coche25.start()
h_coche30.start()
h_coche31.start()
time.sleep(2)
h_coche32.start()
h_coche33.start()
h_coche34.start()
h_coche35.start()
time.sleep(2)
h_coche36.start()
h_coche37.start()
h_coche38.start()







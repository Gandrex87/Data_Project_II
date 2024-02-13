# CÓDIGO FUNCIONANDO PARA ENVIAR POR CADA PAR DE COORDENADAS UN MENSAJE CON LAT Y LONG
# PUNTO DE LA RUTA. 
from google.cloud import pubsub_v1
import json
import os
import xml.etree.ElementTree as ET
import time
import threading
import random

# Configura el cliente de Pub/Sub
publisher = pubsub_v1.PublisherClient()
# ojo - Reemplazar 'your-project-id' y 'topic-name' Y No olvidar generar credenciales de la service account.
topic_path = publisher.topic_path('civic-summer-413119', 'pro-bucket1')

def publicar_coordenada(coordenada, car_id, route_id):
    # Incluir identificadores en el mensaje
    message_data = {
        "car_id": car_id,
        "route_id": route_id,
        "lat": coordenada[0],
        "lon": coordenada[1]
    }
    # Convertir el mensaje a una cadena JSON y luego a bytes
    data_bytes = json.dumps(message_data).encode("utf-8")
    # Publicar el mensaje
    future = publisher.publish(topic_path, data_bytes)
    print(f"Enviando mensaje: {data_bytes}")
    return future.result()



def extraer_datos_kml(ruta_carpeta, car_id):
    # Elige de manera random el KML de la carpeta 'Rutas'
    archivos_kml = [archivo for archivo in os.listdir(ruta_carpeta) if archivo.endswith('.kml')]
    archivo_seleccionado = random.choice(archivos_kml)
    file_path = os.path.join(ruta_carpeta, archivo_seleccionado)

    # Procesa el archivo KML seleccionado
    tree = ET.parse(file_path)
    root = tree.getroot()

    namespace = {'kml': 'http://www.opengis.net/kml/2.2'}

    route_id = os.path.basename(file_path).split('.')[0]  # Usar el nombre del archivo como identificador de ruta
    while True: #temporal para pruebas
        for placemark in root.findall(".//kml:Placemark", namespace):
            for line_string in placemark.findall(".//kml:LineString", namespace):
                for coord in line_string.find(".//kml:coordinates", namespace).text.split():
                    lon, lat = coord.split(',')[:2]  # Ignorar la altitud
                    # Publicar cada par de coordenadas como un mensaje en Pub/Sub, incluyendo los identificadores
                    publicar_coordenada((float(lat), float(lon)), car_id, route_id)
                    time.sleep(1)

                    


# Ejemplo de cómo llamar a la función para diferentes coches
extraer_datos_kml(ruta_carpeta="./Rutas", car_id="coche_1")
#extraer_datos_kml(ruta_carpeta="./Rutas", car_id="coche_2")
# Añade más llamadas según sea necesario para más coches y rutas

# Crear y empezar los hilos para diferentes coches
h_coche1 = threading.Thread(target=extraer_datos_kml, args=("./Rutas", "coche_1"))
#h_coche2 = threading.Thread(target=extraer_datos_kml, args=("./Rutas", "coche_2"))

h_coche1.start()
#h_coche2.start()

# Esperar a que ambos hilos terminen
h_coche1.join()
#h_coche2.join()
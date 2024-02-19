from google.cloud import pubsub_v1
import json
import xml.etree.ElementTree as ET
import time
import threading
import os
import random

# Configura el cliente de Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('data-project-33-413616', 'prueba_coches')

def publicar_coordenada(coordenada, car_id, route_id):
    message_data = {
        "car_id": car_id,
        "destino_coche": route_id,
        "lat": coordenada[0],
        "lon": coordenada[1],
        "plazas_disponibles": 3,
        "personas_transportadas": 0,
        "timestamp": time.time()
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

















# Creacion y empezar los hilos para diferentes coches
h_coche1 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Av_Baleares_a_Oceanografic.kml", "coche_1"))
h_coche2 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Benicalap_a_Ensanche.kml", "coche_2"))
h_coche3 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Av_de_Francia_Val_Malvarrosa.kml", "coche_3"))
h_coche4 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Av_Pio_Baroja_a_Calle_Marino_Albesa.kml", "coche_4"))
h_coche5 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Bioparc_a_Lococlub.kml", "coche_5"))
h_coche6 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Burjasot_a_Mislata.kml", "coche_6"))
h_coche7 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Calle_Brasil_a_Estadio_Mestalla.kml", "coche_7"))
h_coche8 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/CalleVall_de_Ballestera_a_Joaquin_Benlloch.kml", "coche_8"))
h_coche9 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Cementerio_General_de_Valencia_a_Avenida_Blasco_Ibanez.kml", "coche_9"))
h_coche10 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Centro_Comercial_Gran_Turia_a_Chirivella.kml", "coche_10"))
h_coche20 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Ciudad_de_las_Artes_a_Av_Professor_Lopez.kml", "coche_20"))
h_coche21 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Carrer_de_Llanera_de_Ranes_a_Carrer_del_Cine.kml", "coche_21"))
h_coche22 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/EDEM_a_Plaza_Laigua.kml", "coche_22"))
h_coche23 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Edifici_ITURBI_a_Poblados_Marítimos.kml", "coche_23"))
h_coche24 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Estacio_del_Nord_a_Casino_CIRSA.kml", "coche_24"))
h_coche25 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Estacion_Joaquin_Sorolla_a_Tabernes_Blanques.kml", "coche_25"))
h_coche30 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Fuente_San_Luis_a_Akuarela_Playa.kml", "coche_30"))
h_coche31 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Hospital_Univ_Politecnic_La_Fe_a_Av_Fernando_Abril.kml", "coche_31"))
h_coche32 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Hospital_La_Fe_a_Calle_del_General_Llorens.kml", "coche_32"))
h_coche33 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Hospital_Universitario_de_Manises_a_Casino_CIRSA.kml", "coche_33"))
h_coche34 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Malilla_a_Carrer_Colón.kml", "coche_34"))
h_coche35 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Malilla_a_UPV.kml", "coche_35"))
h_coche36 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Natural_Climb_VLC_a_Av_Dr_Peset.kml", "coche_36"))
h_coche37 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Nazaret_parque_Marchalenes_a_Calle_Luis_Crumiere.kml", "coche_37"))
h_coche38 = threading.Thread(target=extraer_datos_kml, args=("./Rutas/Ruzafa_a_Mestalla.kml", "coche_38"))


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







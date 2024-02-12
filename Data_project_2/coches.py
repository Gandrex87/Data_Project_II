# CÓDIGO FUNCIONANDO PARA ENVIAR POR CADA PAR DE COORDENADAS UN MENSAJE CON LAT Y LONG
# PUNTO DE LA RUTA. 
from google.cloud import pubsub_v1
import json
import os
import xml.etree.ElementTree as ET
import time
import threading
import random
import argparse
import logging

#Input arguments
parser = argparse.ArgumentParser(description=('People Data Generator'))

parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')

args, opts = parser.parse_known_args()


class PubSubMessages:

    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id: str, topic_name: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message: str):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("A New vehicle has been monitored. Id: %s", message['coche_id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")


class coche:
    def publicar_coordenada(project_id: str, topic_name: str):
        try:
            publisher = PubSubMessages(project_id, topic_name)
            
            with open("resultados_coordenadas.json", 'r') as json_file:
                lista_resultados = json.load(json_file)

            archivo_seleccionado = random.choice(lista_resultados)
            
            for i in range(2):
                plazas_disponibles = random.randint(1,4)
                precio_distancia = round(random.uniform(0.05, 0.3),2)

                lista_coord = archivo_seleccionado['coordenadas']
                for coord in lista_coord:
                     coordenadas = coord
                     coche_id = f"coche_{i+1}"
                     ruta_id = archivo_seleccionado['numero_ruta']
                     punto_inicio = archivo_seleccionado['punto_inicio']
                     punto_destino = archivo_seleccionado['punto_destino']

                     coche_payload = {
                        "coche_id": coche_id,
                        "ruta_id": ruta_id,
                        "punto_inicio": punto_inicio,
                        "punto_destino": punto_destino,
                        "coordenadas": str(coordenadas),
                        "plazas_disponibles": plazas_disponibles,
                        "precio_distancia": precio_distancia
                        }
                     print(f'datos generados:{coche_payload}')

                     publisher.publishMessages(coche_payload)

        except Exception as err:
            logging.error("Error while inserting car into the PubSub Topic: %s", err)



def run_generator(project_id: str, topic_name: str):

    while True:

        # Get Vehicle Data
        threads = []
        num_threads = 2
        
        for i in range(num_threads):
        
            # Create Concurrent threads to simulate the random movement of vehicles.
            thread = threading.Thread(target=coche.publicar_coordenada, args=(project_id,topic_name))
            threads.append(thread)

        for thread in threads:
            thread.start()

        # Simulate randomness
        time.sleep(random.uniform(1, 10))

if __name__ == "__main__":
    
    # Set Logs
    logging.getLogger().setLevel(logging.INFO)
    
    # Run Generator
    run_generator(
        args.project_id, args.topic_name)

# cambiar estructura tabla en GCP y en pipeline.py
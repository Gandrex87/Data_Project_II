import random
import json
from google.cloud import pubsub_v1
import argparse
import logging
from faker import Faker
import threading
import time
fake = Faker('es_ES')


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
        logging.info("A New person has been monitored. Id: %s", message['persona_id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")



class persona:
    def coordenadas_separadas():
        with open("resultados_coordenadas.json", 'r') as json_file:
            lista_resultados = json.load(json_file)

        coord_individuales = []

        for ruta in lista_resultados:
            lista_coord = ruta['coordenadas']
            for coord in lista_coord:
                coord_individuales.append(str(coord))
            
        return coord_individuales
    
    def caracteristicas(project_id: str, topic_name: str):
            try:
                pubsub_class = PubSubMessages(project_id, topic_name)
                # Coordenada fija para la simulación
                punto_fijo = "39.47849,-0.40798,0"
                
                for i in range(2): #rango según cuántas personas quieras simular
                    persona_id = random.randint(10000000, 99999999) 
                    nombre = fake.name()
                    #punto_inicio = str(random.choice(persona.coordenadas_separadas()))
                    #punto_destino = str(random.choice(persona.coordenadas_separadas()))
                    presupuesto = round(random.uniform(0.5, 30), 2)
                    # Descomponer la coordenada fija en latitud y longitud
                    lat, lon, _ = punto_fijo.split(',')

                    persona_payload = {
                        'persona_id':'54321' ,#persona_id,
                        'nombre': 'pedro rios', # nombre,
                        'lat': float(lat),  # Convertir la latitud a float
                        'lon': float(lon),  # Convertir la longitud a float
                        #'punto_inicio': str(punto_inicio),
                        #'punto_destino': str(punto_destino),
                        'presupuesto': presupuesto
                    }
                    print(persona_payload)

                    pubsub_class.publishMessages(persona_payload)
            
            except Exception as err:
                logging.error("Error while inserting person into the PubSub Topic: %s", err)

def run_generator(project_id: str, topic_name: str):

    while True:

        # Get Vehicle Data
        threads = []
        num_threads = 1
        
        for i in range(num_threads):
        
            # Create Concurrent threads to simulate the random movement of vehicles.
            thread = threading.Thread(target=persona.caracteristicas, args=(project_id,topic_name))
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

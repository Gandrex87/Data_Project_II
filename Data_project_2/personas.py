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
                with open("resultados_coordenadas.json", 'r') as json_file:
                    lista_resultados = json.load(json_file)
                
                for i in range(5):
                    archivo_seleccionado = random.choice(lista_resultados)
                    lista_coord = archivo_seleccionado['coordenadas']
                

                    persona_id = random.randint(10000000, 99999999) 
                    nombre = fake.name()
                    presupuesto = round(random.uniform(0.5, 30), 2)


                    punto_inicio = random.choice(lista_coord)
                    lat_inicio = punto_inicio[0]
                    lon_inicio = punto_inicio[1]

                    # Obtener el índice del punto de inicio
                    indice_inicio = lista_coord.index(punto_inicio) 
                    print(f'indice punto inicio: {indice_inicio}')

                    # Seleccionar un índice aleatorio mayor o igual al índice del punto de inicio
                    indice_destino = random.randint(indice_inicio + 1, len(lista_coord) - 1)
                    punto_destino = lista_coord[indice_destino]
                    lat_destino = punto_destino[0]
                    lon_destino = punto_destino[1]
                    print(f'indice punto destino: {indice_destino}')
                    viajes_realizados = 0
                    pagado:float = 0.00
    
                    persona_payload = {
                        'persona_id': persona_id,
                        'nombre': nombre,
                        'lat_inicio': float(lat_inicio),
                        'lon_inicio': float(lon_inicio),
                        'lat_destino': float(lat_destino),
                        'lon_destino': float(lon_destino),
                        'presupuesto': presupuesto,
                        'viajes_realizados': viajes_realizados,
                        'pagado': pagado
                    }
                    print(persona_payload)

                pubsub_class.publishMessages(persona_payload)
            
            except Exception as err:
                logging.error("Error while inserting person: %s", err)

def run_generator(project_id: str, topic_name: str):

    while True:

        # Get Vehicle Data
        threads = []
        num_threads = 3
        
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

# comprobar estructura tabla en GCP y en pipeline.py
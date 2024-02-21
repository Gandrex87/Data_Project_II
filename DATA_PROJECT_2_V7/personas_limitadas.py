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

puntos = [
    [39.45242, -0.34858], #Punto 1
    [39.44358, -0.36653], #Punto 2
    [39.46004, -0.34806], #punto 3 av francia
    [39.47575, -0.3964],  #punto Pio Baroja
    [39.47256, -0.406],  #punto Bioparc a lococlub
    [40.47555, -0.39414],  #punto Bioparc a lococlub2
    [39.50045, -0.40824],  #punto Burjasot mislata
    [39.49736, -0.444],  #punto Burjasot mislata
    [39.47152, -0.36336],  #punto Clle Brasil
    [41.46323, -0.38744],  #punto VallBallestera
    [39.45526, -0.38057],  #punto VallBallestera
    [39.46471, -0.3973],  #punto LlaneraRanes
    [39.48476, -0.38735],  #punto LlaneraRanes
    [39.46547, -0.41626], #Cementerio gral valencia
    [39.44785, -0.39244], #Cementerio CC Turia
    [39.49865, -0.4017], #CC GTuria
    [39.45142, -0.34123], #CC GTuria
    [39.47943, -0.35127], #Ciudad CC Av Artes Prof
    [39.4844, -0.35408], #Ciudad CC Av Artes Prof
    [39.46474, -0.3589], #Edem plazaAig
    [39.46461, -0.38083], #Edem plazaAig
    [39.46496, -0.3575], #Edif ITURBI
    [39.47749, -0.38], #EstacDelNord
    [39.50498, -0.40283], #EstacDelNord
    [39.49498, -0.43283], #EstacDelNord
    [39.46325, -0.37905], #EstacJoaquinS
    [39.53742, -0.36419], #EstacJoaquinS
    [39.45525, -0.3389], #FuenteSanluis
    [39.48835, -0.445], #HospitalLaFe
    [39.44267, -0.37863], #HospitalUniv_Politec
    [39.44267, -0.37863], #HospitalUniv_Politec
    [39.44282, -0.37352], #HospitalUniversitario
    [39.45355, -0.4154], #HospitalUniversitario
    [39.4057, -0.37874], #Malilla
    [39.44659, -0.37212], #Malilla
    [39.48308, -0.3479], #Malilla
    [39.44659, -0.37212], #Malilla
    [39.46333, -0.40935], #NaturalC
    [39.46318, -0.36051], #Nazareth
    [39.47256, -0.37009], #Nazareth
    # Añade más puntos según sea necesario
]

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
    persona_id_counter = 1100
    # Modificamos para que genere solo 50 personas en total
    total_personas_generadas = 0  # Nuevo contador para el total de personas generadas

    def caracteristicas(project_id: str, topic_name: str):
        global total_personas_generadas  # Referencia a la variable global para mantener la cuenta

        try:
            pubsub_class = PubSubMessages(project_id, topic_name)
            
            while persona.total_personas_generadas < 50:  # Condición modificada para limitar a 50 personas
                punto_aleatorio = random.choice(puntos)
                lat, lon = punto_aleatorio

                persona_id = persona.persona_id_counter
                persona.persona_id_counter += 1

                nombre = fake.name()
                presupuesto = round(random.uniform(0.5, 20), 1)

                persona_payload = {
                    'persona_id': persona_id,
                    'nombre': nombre,
                    'lat': lat,
                    'lon': lon,
                    'presupuesto': presupuesto
                }

                pubsub_class.publishMessages(persona_payload)
                persona.total_personas_generadas += 1  # Incrementa el contador de personas generadas

        except Exception as err:
            logging.error("Error while inserting person into the PubSub Topic: %s", err)

def run_generator(project_id: str, topic_name: str):
    # Modificación para no usar hilos ya que solo generaremos 50 personas en total
    caracteristicas(project_id, topic_name)  # Llama directamente a generar 50 personas

if __name__ == "__main__":
    # Configuración de logging y ejecución
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)
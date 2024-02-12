""" Import libraries """

# Import Beam Libraries

import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics
from apache_beam import PTransform, ParDo, DoFn
from apache_beam import window

# Import Common Libraries
import argparse
import requests
import logging
import json
import re
import io
import math

beam.options.pipeline_options.PipelineOptions.allow_non_parallel_instruction_output = True
DataflowRunner.__test__ = False


""" Helpful functions """
def ParsePubSubMessage(message):

    # Decode PubSub message in order to deal with
    pubsub_message = message.decode('utf-8')
    
    # Convert string decoded in JSON format
    msg = json.loads(pubsub_message)

    logging.info("New message in PubSub: %s", msg)

    # Return function
    return msg

def haversine(lat1, lon1, lat2, lon2):
    """
    Calcula la distancia del círculo mayor en kilómetros entre dos puntos 
    en la Tierra dados por latitud y longitud en grados decimales.
    """
    # Convertir grados decimales a radianes
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    # Fórmula de Haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    # Radio de la Tierra en kilómetros.
    r = 6371

    # Calcular el resultado
    return c * r

def add_dummy_key(element):
    return ('dummy_key', element)

def format_message_coches(message):
    # Asume que el mensaje es un diccionario con 'car_id', 'route_id', 'lat', y 'lon'.
    return {
        'car_id': message['car_id'],
        'route_id': message['route_id'],
        'lat': message['lat'],
        'lon': message['lon']
    }
    
def format_message_personas(message):
    # Asume que el mensaje es un diccionario con 'persona_id', 'nombre', 'lat', 'lon', y 'presupuesto'.
    return {
        'persona_id': message['persona_id'],
        'nombre': message['nombre'],
        'lat': message['lat'],
        'lon': message['lon'],
        'presupuesto': message['presupuesto']
    }
""" Dataflow Process """

class FindMatchFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        coches = element[1]['coches']
        personas = element[1]['personas']
        for coche in coches:
            for persona in personas:
                distance = haversine(coche['lat'], coche['lon'], persona['lat'], persona['lon'])
                if distance <= TU_UMBRAL_DE_DISTANCIA:
                    match = {'coche_id': coche['car_id'], 'persona_id': persona['persona_id'], 'distance': distance}
                    print("Coincidencia encontrada:", match)
                    yield match


def run():

    """ Input Arguments"""
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))
    
    parser.add_argument(
    '--project_id',
    required=True,
    help='GCP cloud project ID to use.')

    parser.add_argument(
    '--input_subscription_coches',
    required=True,
    help='PubSub subscription from which we will read data for coches.')

    parser.add_argument(
    '--input_subscription_personas',
    required=True,
    help='PubSub subscription from which we will read data for personas.')

    args, pipeline_opts = parser.parse_known_args()

    
    """ Apache Beam Pipeline """
    
    # Pipeline Options
    options = PipelineOptions(pipeline_opts,
        save_main_session=True, streaming=True, project=args.project_id)

    # Pipeline
    window_duration = 0.5 #en minutos (definir tiempos)
    with beam.Pipeline(argv=pipeline_opts, options=options) as p:
    # Datos de coches
        coches = (
            p
            | "Leer datos de coches desde PubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription_coches)
            | "Parsear mensajes de coches" >> beam.Map(ParsePubSubMessage)
            | "Aplicar ventana a coches" >> beam.WindowInto(window.FixedWindows(window_duration * 60))  # Ventanas de 1 minuto
            | "Formatear mensaje de coches" >> beam.Map(format_message_coches)
            #| "Agregar clave dummy a coches" >> beam.Map(add_dummy_key)
        )
        
        # Datos de personas
        personas = (
            p
            | "Leer datos de personas desde PubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription_personas)
            | "Parsear mensajes de personas" >> beam.Map(ParsePubSubMessage)
            | "Aplicar ventana a personas" >> beam.WindowInto(window.FixedWindows(window_duration * 60))  # Ventanas de 1 minuto
            | "Formatear mensaje de personas" >> beam.Map(format_message_personas)
            #| "Agregar clave dummy a personas" >> beam.Map(add_dummy_key)
        )
        
        # Procesamiento adicional y unión de datos
    # Aquí iría tu lógica para unir y procesar los datos de coches y personas
    
    



    # Escritura de datos de coches en BigQuery
        coches | "Escribir datos de coches en BigQuery" >> beam.io.WriteToBigQuery(
        table="civic-summer-413119:midataset_coches.coches",
        schema='car_id:STRING, route_id:STRING, lat:FLOAT, lon:FLOAT',
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

    # Escritura de datos de personas en BigQuery (ajusta según sea necesario)
        personas | "Escribir datos de personas en BigQuery" >> beam.io.WriteToBigQuery(
        table="civic-summer-413119:midataset_coches.personas",
        schema='persona_id:STRING, nombre:STRING, lat:FLOAT, lon:FLOAT, presupuesto:FLOAT',
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        
        

if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")

    # Run Process
    run()
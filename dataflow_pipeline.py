""" Import libraries """

# Import Beam Libraries

import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics
from apache_beam import PTransform, ParDo, DoFn
from apache_beam import window
from datetime import datetime, timedelta
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

# Import Common Libraries
import argparse
import requests
import logging
import json
import re
import io
import math
import time

beam.options.pipeline_options.PipelineOptions.allow_non_parallel_instruction_output = True
DataflowRunner.__test__ = False


""" Helpful functions """
def ParsePubSubMessage(message):
    try:
        # Decodifica el mensaje PubSub
        pubsub_message = message.decode('utf-8')
        # Convierte la cadena decodificada a un objeto JSON
        msg = json.loads(pubsub_message)
        # Añade un timestamp al mensaje
        msg['timestamp'] = time.time()
    except Exception as e:
        logging.error(f"Error procesando el mensaje: {e}")
        return None

    logging.info(f"Msg procesado: {msg}")
    return msg

def haversine(lat1, lon1, lat2, lon2):
    # Convertir grados decimales a radianes
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    # Diferencias en radianes
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Pre-calcular senos y cosenos
    sin_dlat_over_2 = math.sin(dlat / 2)
    sin_dlon_over_2 = math.sin(dlon / 2)

    # Fórmula de Haversine
    a = sin_dlat_over_2 * sin_dlat_over_2 + math.cos(lat1) * math.cos(lat2) * sin_dlon_over_2 * sin_dlon_over_2
    c = 2 * math.asin(math.sqrt(a))

    # Radio de la Tierra en kilómetros
    r = 6371

    # Calcular el resultado
    return c * r



def assign_temporal_key(timestamp, interval='hour'):
    """Asigna una clave basada en el tiempo redondeando el timestamp dado a la 'hora' más cercana o al 'minuto' más cercano."""
    # Convierte el timestamp epoch a un objeto datetime
    timestamp_dt = datetime.fromtimestamp(timestamp)

    if interval == 'hour':
        # Redondea a la hora más cercana
        rounded_dt = timestamp_dt.replace(minute=0, second=0, microsecond=0)
    elif interval == 'minute':
        # Redondea al minuto más cercano
        rounded_dt = timestamp_dt.replace(second=0, microsecond=0)
    else:
        raise ValueError("Intervalo no soportado. Elija 'hour' o 'minute'.")

    # Convierte el datetime redondeado de vuelta a un timestamp epoch para una clave simple
    return rounded_dt.timestamp()


def format_message_coches(message):
    # Verifica si 'timestamp' está en el mensaje
    if 'timestamp' not in message:
        logging.error(f"Mensaje sin timestamp: {message}")
        # Decide cómo manejar este caso: omitir el mensaje, usar un valor predeterminado, etc.
        return None
    
    temporal_key = assign_temporal_key(message['timestamp'], 'hour')
    return (temporal_key, {
        'car_id': message['car_id'],
        'destino_coche': message['destino_coche'],
        'lat': message['lat'],
        'lon': message['lon'],
        'plazas_disponibles': message['plazas_disponibles'],
        'timestamp': message['timestamp']
    })
    

def format_message_personas(message):
    # Verifica si 'timestamp' está en el mensaje
    if 'timestamp' not in message:
        logging.error(f"Mensaje sin timestamp: {message}")
        # Decide cómo manejar este caso: omitir el mensaje, usar un valor predeterminado, etc.
        return None
    
    temporal_key = assign_temporal_key(message['timestamp'], 'hour')
    return (temporal_key, {
        'persona_id': message['persona_id'],
        'nombre': message['nombre'],
        'lat': message['lat'],
        'lon': message['lon'],
        'presupuesto': message['presupuesto'],
        'timestamp': message['timestamp']
    })

def debug_print(element):
    print(element)
    return element

def print_data(element):
    print("Datos finales enviados a BigQuery:", element)
    return element


def log_message(message):
    logging.info(f"Mensaje recibido: {message}")
    return message

def format_message_matches(message):
    # Asegúrate de que los tipos de datos y los nombres de los campos coincidan con el esquema de BigQuery
    return {
        'car_id': str(message['car_id']),  # Convierte a cadena si es necesario
        'destino_coche': message['destino_coche'],
        'plazas_disponibles': int(message['plazas_disponibles']),  # Asegúrate de que sea un entero
        'persona_id': str(message['persona_id']),  # Convierte a cadena si es necesario
        'distance': float(message['distance'])  # Asegúrate de que sea un float
    }



""" Dataflow Process """

DISTANCIA_MAXIMA=3
class BuscarCoincidenciasFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        _, datos_agrupados = element
        coches = datos_agrupados['coches']
        personas = datos_agrupados['personas']

        coincidencias = []
        for coche in coches:
            for persona in personas:
                distance = haversine(coche['lat'], coche['lon'], persona['lat'], persona['lon'])

                if distance <= DISTANCIA_MAXIMA:
                    logging.info(f"Coincidencia encontrada: Coche {coche['car_id']} y Persona {persona['persona_id']} a una distancia de {distance:.2f} km")
                    coincidencia = {
                        'car_id': coche['car_id'],
                        'destino_coche': coche['destino_coche'],
                        'plazas_disponibles': coche['plazas_disponibles'],
                        'persona_id': persona['persona_id'],
                        'distanceDelPasajero': distance
                    }
                    coincidencias.append(coincidencia)

        # Selecciona la coincidencia más cercana (menor distancia)
        if coincidencias:
            coincidencia_seleccionada = min(coincidencias, key=lambda x: x['distanceDelPasajero'])
            yield coincidencia_seleccionada






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
    window_duration = 0.2 #en minutos (definir tiempos)
    with beam.Pipeline(argv=pipeline_opts, options=options) as p:
    # Datos de coches
        coches = (
            p
            | "Leer datos de coches desde PubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription_coches)
            | "Parsear mensajes de coches" >> beam.Map(ParsePubSubMessage)
            | "Aplicar ventana a coches" >> beam.WindowInto(window.FixedWindows(window_duration * 60))  # Ventanas de 1 minuto
            | "Formatear mensaje de coches" >> beam.Map(format_message_coches)
            
        )
        
        # Datos de personas
        personas = (
            p
            | "Leer datos de personas desde PubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription_personas)
            | "Parsear mensajes de personas" >> beam.Map(ParsePubSubMessage)
            | "Aplicar ventana a personas" >> beam.WindowInto(window.FixedWindows(window_duration * 60))  # Ventanas de 1 minuto
            | "Formatear mensaje de personas" >> beam.Map(format_message_personas)            
        )
         
        datos_agrupados = (
            {'coches': coches, 'personas': personas} 
            | "Agrupar coches y personas por clave temporal" >> beam.CoGroupByKey()
        )
        
            #Aplica FindMatchFn para procesar los datos agrupados
        coincidencias = (
              datos_agrupados 
            | "Buscar coincidencias" >> beam.ParDo(BuscarCoincidenciasFn()))
        #coincidencias | 'Debug Print' >> beam.Map(debug_print)
        coincidencias | 'Print Data' >> beam.Map(print_data)
        schema = 'car_id:STRING, destino_coche:STRING, plazas_disponibles:INTEGER, persona_id:STRING, distanceDelPasajero:FLOAT'
        coincidencias | "Escribir en BigQuery" >> WriteToBigQuery(
                table='midataset_coches.matches',
                schema=schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                project='civic-summer-413119'
            )
    
        
        

if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")

    # Run Process
    run()
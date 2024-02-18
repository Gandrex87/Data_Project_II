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
        return None
    
    temporal_key = assign_temporal_key(message['timestamp'], 'hour')
    return (temporal_key, {
        'car_id': message['car_id'],
        'destino_coche': message['destino_coche'],
        'lat': message['lat'],
        'lon': message['lon'],
        'plazas_disponibles': message['plazas_disponibles'],
        'precio_por_viaje': message.get('precio_por_viaje', 0.0),
        'personas_transportadas': message.get('personas_transportadas', 0),
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
        'distance': float(message['distance']),  # Asegúrate de que sea un float
        'dinero_recaudado': message['dinero_recaudado'],
        'personas_transportadas': message['personas_transportadas']
    
    }



""" Dataflow Process """

DISTANCIA_MAXIMA = 3
COSTO_POR_KM = 0.8
class BuscarCoincidenciasFn(DoFn):
    def process(self, element, *args, **kwargs):
        _, datos_agrupados = element
        coches = datos_agrupados['coches']
        personas = datos_agrupados['personas']

        # Almacenar las personas ya asignadas para evitar duplicados
        personas_asignadas = set()

        for coche in coches:
            # Saltar este coche si no quedan plazas disponibles
            if coche['plazas_disponibles'] <= 0:
                continue

            coincidencia_seleccionada = None
            menor_distancia = float('inf')

            for persona in personas:
                # Saltar si la persona ya ha sido asignada a un coche
                if persona['persona_id'] in personas_asignadas:
                    continue

                distance = haversine(coche['lat'], coche['lon'], persona['lat'], persona['lon'])
                costo_viaje = distance * COSTO_POR_KM

                # Verificar si la persona está dentro del rango y presupuesto, y si es la más cercana hasta ahora
                if distance <= DISTANCIA_MAXIMA and costo_viaje <= persona['presupuesto'] and distance < menor_distancia:
                    menor_distancia = distance
                    coincidencia_seleccionada = {
                        'car_id': coche['car_id'],
                        'destino_coche': coche['destino_coche'],
                        'plazas_disponibles': coche['plazas_disponibles'] - 1,  # Restar una plaza disponible
                        'persona_id': persona['persona_id'],
                        'distanceDelPasajero': round(distance, 2),
                        'precio_por_viaje': round(costo_viaje, 2),
                        'personas_transportadas': 1 , # Inicializar como 1 ya que se encontró una coincidencia
                        'timestamp': coche['timestamp']  
                    }
                    personas_asignadas.add(persona['persona_id'])

                    # Emitir la coincidencia seleccionada si se encontró alguna
                    if coincidencia_seleccionada:
                        # Actualizar el estado del coche
                        coche['plazas_disponibles'] = coincidencia_seleccionada['plazas_disponibles']
                        coche['precio_por_viaje'] += coincidencia_seleccionada['precio_por_viaje']
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
    window_duration = 0.3 #en minutos (definir tiempos)
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
        schema = 'car_id:STRING, destino_coche:STRING, plazas_disponibles:INTEGER, persona_id:STRING, distanceDelPasajero:FLOAT, precio_por_viaje:FLOAT, personas_transportadas:INTEGER, timestamp:FLOAT'
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
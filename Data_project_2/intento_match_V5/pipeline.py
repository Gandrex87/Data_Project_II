""" Import libraries """

# Import Beam Libraries

import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics
from apache_beam import PTransform, ParDo, DoFn
from datetime import datetime, timedelta
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


# Import Common Libraries
import argparse
import requests
import logging
import json
import re
import io
import time
import math
import time

beam.options.pipeline_options.PipelineOptions.allow_non_parallel_instruction_output = True
DataflowRunner.__test__ = False


""" Helpful functions """
def ParsePubSubMessage(message):
    try:
        # Decode PubSub message in order to deal with
        pubsub_message = message.decode('utf-8')
    
        # Convert string decoded in JSON format
        msg = json.loads(pubsub_message)
        # Añade un timestamp al mensaje
        msg['timestamp'] = time.time()
    
    except Exception as e:
        logging.error(f"Error procesando el mensaje: {e}")
        return None

    #logging.info("New message in PubSub: %s", msg)

    # Return function
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

# Función para procesar y formatear los datos antes de escribirlos en BigQuery
def format_message_people(message):
        # Verifica si 'timestamp' está en el mensaje
    if 'timestamp' not in message:
        logging.error(f"Mensaje sin timestamp: {message}")
        # Decide cómo manejar este caso: omitir el mensaje, usar un valor predeterminado, etc.
        return None
    
    temporal_key = assign_temporal_key(message['timestamp'], 'hour')

    return (temporal_key, {
        'persona_id': message['persona_id'],
        'nombre': message['nombre'],
        'lat_inicio': message['lat_inicio'],
        'lon_inicio': message['lon_inicio'],
        'lat_destino': message['lat_destino'],
        'lon_destino': message['lon_destino'],
        'presupuesto': message['presupuesto'],
        'viajes_realizados': message['viajes_realizados'],
        'pagado': message['pagado']
    })

def format_message_vehicles(message):
    # Verifica si 'timestamp' está en el mensaje
    if 'timestamp' not in message:
        logging.error(f"Mensaje sin timestamp: {message}")
        # Decide cómo manejar este caso: omitir el mensaje, usar un valor predeterminado, etc.
        return None
    
    temporal_key = assign_temporal_key(message['timestamp'], 'hour')
    
    return (temporal_key, {
        'coche_id': message['coche_id'],
        'ruta_id': message['ruta_id'],
        'punto_inicio': message['punto_inicio'],
        'punto_destino': message['punto_destino'],
        'lat': message['lat'],
        'lon': message['lon'],
        'plazas_disponibles': message['plazas_disponibles'],
        'precio_distancia': message['precio_distancia'],
        'trayectos_realizados': message['trayectos_realizados'],
        'personas_transportadas': message['personas_transportadas'],
        'dinero_recaudado': message['dinero_recaudado']
    })

def print_data(element):
    print("Datos finales enviados a BigQuery:", element)
    return element

def precio_recorrido_persona(coche, persona):
    recorrido = haversine(persona['lat_inicio'], persona['lon_inicio'], persona['lat_destino'], persona['lon_destino'])
    precio_recorrido = round(recorrido * coche['precio_distancia'], 2)

    return precio_recorrido

""" Dataflow Process """

DISTANCIA_MAXIMA=0
class BuscarCoincidenciasFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        _, datos_agrupados = element
        coches = datos_agrupados['coches']
        personas = datos_agrupados['personas']

        coincidencias = []
        for coche in coches:
            if coche['plazas_disponibles'] > 0:
                for persona in personas:
                    #distance = haversine(coche['lat'], coche['lon'], persona['lat_inicio'], persona['lon_inicio'])
                    precio_trayecto_persona = precio_recorrido_persona(coche, persona)

                    if coche['lat'] == persona['lat_inicio'] and coche['lon'] == persona['lon_inicio']:
                        if precio_trayecto_persona < persona['presupuesto']:
                    
                            logging.info(f"Coincidencia encontrada: Coche {coche['coche_id']} y Persona {persona['persona_id']} en la ruta {coche['ruta_id']}")
                            coche['plazas_disponibles'] -= 1
                            coche['personas_transportadas'] += 1
                            coche['dinero_recaudado'] += precio_trayecto_persona
                            persona['viajes_realizados'] += 1
                            persona['pagado'] += precio_trayecto_persona

                            coincidencia = {
                                'coche_id': coche['coche_id'],
                                'destino_coche': coche['punto_destino'],
                                'plazas_disponibles': coche['plazas_disponibles'],
                                'persona_id': persona['persona_id'],
                                #'distanceDelPasajero': distance, 
                                'precio_trayecto': precio_trayecto_persona,
                                'personas_transportadas': coche['personas_transportadas']
                            }
                            coincidencias.append(coincidencia)

                        else:
                            logging.info(f"La persona {persona['persona_id']} no está dispuesta a pagar tanto por el trayecto.")

                    if coche['lat'] == persona['lat_destino'] and coche['lon'] == persona['lon_destino']:
                        logging.info(f"La persona {persona['persona_id']} ha llegado a su destino")
                        break

            else:
                logging.info(f"El coche {coche['coche_id']} no tiene más plazas disponibles")
                break
        
               

                # # Selecciona la coincidencia más cercana (menor distancia)
                # if coincidencias:
                #     coincidencia_seleccionada = min(coincidencias, key=lambda x: x['distanceDelPasajero'])
                #     yield coincidencia_seleccionada

def run():

    """ Input Arguments"""
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--input_subscription1',
                required=True,
                help='PubSub subscription from which we will read data from the people generator.')

    parser.add_argument(
                '--input_subscription2',
                required=True,
                help='PubSub subscription from which we will read data from the vehicles generator.')

    args, pipeline_opts = parser.parse_known_args()

    
    """ Apache Beam Pipeline """
    
    # Pipeline Options
    options = PipelineOptions(pipeline_opts,
        save_main_session=True, streaming=True, project=args.project_id)

    # Pipeline

    with beam.Pipeline(argv=pipeline_opts,options=options) as p:

        """ Part 01: Read data from PubSub. """
        messages_topic1 = (
            p
            | "Read People From PubSub Topic 1" >> beam.io.ReadFromPubSub(subscription=args.input_subscription1)
            | "Parse JSON messages 1" >> beam.Map(ParsePubSubMessage)
            | "Aplicar ventana a personas" >> beam.WindowInto(window.FixedWindows(60))
            | "Format People Messages 1" >> beam.Map(format_message_people)
        )

        messages_topic2 = (
            p
            | "Read Vehicles From PubSub Topic 2" >> beam.io.ReadFromPubSub(subscription=args.input_subscription2)
            | "Parse JSON messages 2" >> beam.Map(ParsePubSubMessage)
            | "Aplicar ventana a coches" >> beam.WindowInto(window.FixedWindows(60))
            | "Format Vehicles Messages 2" >> beam.Map(format_message_vehicles)
        )

        """ Part 02: Write raw data to BigQuery. """
        
        # raw_people_bq = (
        #     messages_topic1 
        #         | "Write people to BigQuery" >> beam.io.WriteToBigQuery(
        #             table = "data-project-33-413616:dataproject2.personas_todas", # Required Format: PROJECT_ID:DATASET.TABLE
        #             schema='persona_id:INTEGER, nombre:STRING, lat_inicio:FLOAT, lon_inicio: FLOAT, lat_destino:FLOAT, lon_destino: FLOAT, presupuesto:FLOAT, viajes_realizados:INTEGER, pagado:FLOAT', # Required Format: field:TYPE
        #             create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        #             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        #     )
        # )

        # raw_vehicles_bq = (
        #     messages_topic2 
        #         | "Write vehicles to BigQuery" >> beam.io.WriteToBigQuery(
        #             table = "data-project-33-413616:dataproject2.coches_todos", # Required Format: PROJECT_ID:DATASET.TABLE
        #             schema='coche_id:STRING, ruta_id:INTEGER, punto_inicio:STRING, punto_destino:STRING, lat:FLOAT, lon:FLOAT, plazas_disponibles:INTEGER, precio_distancia:FLOAT, trayectos_realizados:INTEGER, personas_transportadas:INTEGER, dinero_recaudado:FLOAT', # Required Format: field:TYPE
        #             create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        #             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        #     )
        # )


        """ Part 03: Transform and Match """
        
        datos_agrupados = (
            {'coches': messages_topic2, 'personas': messages_topic1} 
            | "Agrupar coches y personas por clave temporal" >> beam.CoGroupByKey()
        )

        #Aplica FindMatchFn para procesar los datos agrupados
        coincidencias = (
              datos_agrupados 
            | "Buscar coincidencias" >> beam.ParDo(BuscarCoincidenciasFn()))
        
        #coincidencias | 'Debug Print' >> beam.Map(debug_print)
        coincidencias | 'Print Data' >> beam.Map(print_data)
        schema = 'coche_id:STRING, destino_coche:STRING, plazas_disponibles:INTEGER, persona_id:STRING, distanceDelPasajero:FLOAT, precio_trayecto:FLOAT, personas_transportadas:INTEGER'
        coincidencias | "Escribir en BigQuery" >> beam.io.WriteToBigQuery(
                table='data-project-33-413616:dataproject2.matches',
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )


if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")

    # Run Process
    run()
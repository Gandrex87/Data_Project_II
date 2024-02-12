""" Import libraries """

# Import Beam Libraries

import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics


# Import Common Libraries
import argparse
import requests
import logging
import json
import re
import io

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

# Función para procesar y formatear los datos antes de escribirlos en BigQuery
def format_message_people(message):
    # Aquí puedes realizar cualquier procesamiento adicional según tus necesidades
    return {
        'persona_id': message['persona_id'],
        'nombre': message['nombre'],
        'punto_inicio': message['punto_inicio'],
        'punto_destino': message['punto_destino'],
        'presupuesto': message['presupuesto']
    }

def format_message_vehicles(message):
    return {
        'coche_id': message['coche_id'],
        'ruta_id': message['ruta_id'],
        'punto_inicio': message['punto_inicio'],
        'punto_destino': message['punto_destino'],
        'coordenadas': message['coordenadas'],
        'plazas_disponibles': message['plazas_disponibles'],
        'precio_distancia': message['precio_distancia']
    }

""" Dataflow Process """

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
            | "Format People Messages 1" >> beam.Map(format_message_people)
        )

        messages_topic2 = (
            p
            | "Read Vehicles From PubSub Topic 2" >> beam.io.ReadFromPubSub(subscription=args.input_subscription2)
            | "Parse JSON messages 2" >> beam.Map(ParsePubSubMessage)
            | "Format Vehicles Messages 2" >> beam.Map(format_message_vehicles)
        )

        """ Part 02: Write raw data to BigQuery. """
        (
            messages_topic1 
                | "Write people to BigQuery" >> beam.io.WriteToBigQuery(
                    table = "data-project-33-413616:dataset_33.personas", # Required Format: PROJECT_ID:DATASET.TABLE
                    schema='persona_id:INTEGER, nombre:STRING, punto_inicio:STRING, punto_destino:STRING, presupuesto:FLOAT', # Required Format: field:TYPE
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        (
            messages_topic2 
                | "Write vehicles to BigQuery" >> beam.io.WriteToBigQuery(
                    table = "data-project-33-413616:dataset_33.coches", # Required Format: PROJECT_ID:DATASET.TABLE
                    schema='coche_id:STRING, ruta_id:INTEGER, punto_inicio:STRING, punto_destino:STRING, coordenadas:STRING, plazas_disponibles:INTEGER, precio_distancia:FLOAT', # Required Format: field:TYPE
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        

if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")

    # Run Process
    run()
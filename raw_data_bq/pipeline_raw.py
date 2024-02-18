""" Import libraries """

# Import Beam Libraries

import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


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
        'lat': message['lat'],
        'lon': message['lon'],
        'presupuesto': message['presupuesto'],
        'timestamp': message['timestamp']
    }


def format_message_vehicles(message):
    return {
        'car_id': message['car_id'],
        'destino_coche': message['destino_coche'],
        'lat': message['lat'],
        'lon': message['lon'],
        'plazas_disponibles': message['plazas_disponibles'],
        'personas_transportadas': message['personas_transportadas'],
        'timestamp': message['timestamp']
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
                '--input_subscription_personas',
                required=True,
                help='PubSub subscription from which we will read data from the people generator.')

    parser.add_argument(
                '--input_subscription_coches',
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
        personas = (
            p
            | "Read People From PubSub Topic 1" >> beam.io.ReadFromPubSub(subscription=args.input_subscription_personas)
            | "Parse JSON messages 1" >> beam.Map(ParsePubSubMessage)
            | "Format People Messages 1" >> beam.Map(format_message_people)
        )

        coches = (
            p
            | "Read Vehicles From PubSub Topic 2" >> beam.io.ReadFromPubSub(subscription=args.input_subscription_coches)
            | "Parse JSON messages 2" >> beam.Map(ParsePubSubMessage)
            | "Format Vehicles Messages 2" >> beam.Map(format_message_vehicles)
        )

        """ Part 02: Write raw data to BigQuery. """
        (
            personas
                | "Write people to BigQuery" >> beam.io.WriteToBigQuery(
                    table = "data-project-33-413616:dataprojectg4.personas_todas", # Required Format: PROJECT_ID:DATASET.TABLE
                    schema='persona_id:STRING, nombre:STRING, lat:FLOAT, lon: FLOAT, presupuesto:FLOAT, timestamp:FLOAT', # Required Format: field:TYPE
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        (
            coches
                | "Write vehicles to BigQuery" >> beam.io.WriteToBigQuery(
                    table = "data-project-33-413616:dataprojectg4.coches_todos", # Required Format: PROJECT_ID:DATASET.TABLE
                    schema='car_id:STRING, destino_coche:STRING, lat:FLOAT, lon:FLOAT, plazas_disponibles:INTEGER, personas_transportadas:INTEGER, timestamp:FLOAT', # Required Format: field:TYPE
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
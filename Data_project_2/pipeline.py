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
def format_message(message):
    # Aquí puedes realizar cualquier procesamiento adicional según tus necesidades
    return {
        'persona_id': message['persona_id'],
        'nombre': message['nombre'],
        'punto_inicio': message['punto_inicio'],
        'punto_destino': message['punto_destino'],
        'presupuesto': message['presupuesto']
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
                '--input_subscription',
                required=True,
                help='PubSub subscription from which we will read data from the generator.')

    args, pipeline_opts = parser.parse_known_args()

    
    """ Apache Beam Pipeline """
    
    # Pipeline Options
    options = PipelineOptions(pipeline_opts,
        save_main_session=True, streaming=True, project=args.project_id)

    # Pipeline

    with beam.Pipeline(argv=pipeline_opts,options=options) as p:

        """ Part 01: Read data from PubSub. """

        (p
            | "Read From PubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
            | "Parse JSON messages" >> beam.Map(ParsePubSubMessage)
            | "Format Message" >> beam.Map(format_message)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = "data-project-33-413616:dataset_33.tabla_33", # Required Format: PROJECT_ID:DATASET.TABLE
                schema='persona_id:INTEGER, nombre:STRING, punto_inicio:STRING, punto_destino:STRING, presupuesto:FLOAT', # Required Format: field:TYPE
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
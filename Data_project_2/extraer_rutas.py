# CÓDIGO FUNCIONANDO PARA SACAR NOMBRES DE LOS PUNTOS Y COORDENADAS EN UN JSON

import xml.etree.ElementTree as ET
import json
import os

# Función para extraer coordenadas del .KML
def extraer_datos_kml(ruta_carpeta, output_file):
    resultados_coord = []
    #Para que lea directamente todos los archivos que sean KML de la carpeta "Rutas" 
    archivos_kml = [os.path.join(ruta_carpeta, archivo) for archivo in os.listdir(ruta_carpeta) if archivo.endswith('.kml')]

    # Para que extraiga los datos de cada archivo kml de uno en uno
    for numero_ruta, file_path in enumerate(archivos_kml, start = 1):
        coordenadas = []
        tree = ET.parse(file_path)
        root = tree.getroot()
        nombres_placemarks = []

        # Namespace utilizado en archivos KML
        namespace = {'kml': 'http://www.opengis.net/kml/2.2'}

        # Buscar todos los elementos LineString dentro del archivo KML
        for placemark in root.findall(".//kml:Placemark", namespace):
            for line_string in placemark.findall(".//kml:LineString", namespace):
                for coord in line_string.find(".//kml:coordinates", namespace).text.split():
                    lon, lat = coord.split(',')[:2]  # Ignorar la altitud si está presente
                    coordenadas.append((float(lat), float(lon)))
        
        #Buscar todos los elementos <name> en el KML y añadirlos a la lista nombres_placemarks
        for placemark in root.findall(".//kml:Placemark", namespace):
            nombre = placemark.find(".//kml:name", namespace).text
            nombres_placemarks.append(nombre)
        
        resultado = {
                    "numero_ruta": numero_ruta,
                    "punto_inicio": nombres_placemarks[1], # El elemento 0 es del tipo 'Indicaciones de Avenida de Baleares, Valencia a Oceanogràfic, Valencia' y no nos hace falta
                    "punto_destino": nombres_placemarks[2],
                    "coordenadas": coordenadas
                    }
        resultados_coord.append(resultado)

    # Escribir la lista completa en el archivo JSON
    with open(output_file, 'w') as json_file:
        json.dump(resultados_coord, json_file)

    return "Proceso completado"

output_file = "./resultados_coordenadas.json"

# Ruta de la carpeta que contiene los archivos KML (en la terminal hay que estar en la carpeta DATA_PROJECT_II que contiene las carpetas 'Data_Project_II' y 'Rutas')
ruta_carpeta = "./Rutas"

# Se genera/actualiza el archivo resultados_coord.json
resultado_total_funcion = extraer_datos_kml(ruta_carpeta, output_file)

print(resultado_total_funcion)


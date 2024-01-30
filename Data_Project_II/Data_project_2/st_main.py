import xml.etree.ElementTree as ET
import folium
from streamlit_folium import folium_static
import time
import random


# Función para extraer coordenadas del .KML
def extraer_coordenadas_kml(file_path):
    coordenadas = []
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Namespace utilizado en archivos KML
    namespace = {'kml': 'http://www.opengis.net/kml/2.2'}

    # Buscar todos los elementos LineString dentro del archivo KML
    for placemark in root.findall(".//kml:Placemark", namespace):
        for line_string in placemark.findall(".//kml:LineString", namespace):
            for coord in line_string.find(".//kml:coordinates", namespace).text.split():
                lon, lat = coord.split(',')[:2]  # Ignorar la altitud si está presente
                coordenadas.append((float(lat), float(lon)))

    return coordenadas


# Definiendo correctamente la ruta del archivo KML
file_path = './Ruz_Mest.kml'

# Extraer las coordenadas
coordenadas = extraer_coordenadas_kml(file_path)

# Crear un mapa
mapa_ruta = folium.Map(location=coordenadas[0], zoom_start=14)

# Agregar la línea de la ruta al mapa
folium.PolyLine(coordenadas, color='red', weight=2.8, opacity=1).add_to(mapa_ruta)

# Agregar un marcador para el punto de inicio (Punto A)
inicio_marker = folium.Marker(
    coordenadas[0],
    popup="Punto A: Inicio",
    icon=folium.Icon(color="green", icon="play")
).add_to(mapa_ruta)

# Agregar un marcador para el punto final (Punto B)
fin_marker = folium.Marker(
    coordenadas[-1],
    popup="Punto B: Fin",
    icon=folium.Icon(color="red", icon="stop")
).add_to(mapa_ruta)

# Guardar el mapa como archivo HTML
ruta_mapa_ruta = './mapa_ruta_completa.html'
mapa_ruta.save(ruta_mapa_ruta)

# Posicion de una persona
def persona():
    punto_inicio = random.choice(coordenadas)
    folium.Marker(
        location =punto_inicio,
        icon=folium.Icon(color='blue', icon='person', prefix='fa')
    ).add_to(mapa_ruta)

# Simular el movimiento del coche
def simular_movimiento_coche(coordenadas, tiempo_espera):
    icono_coche = folium.Icon(color='blue', icon="car", prefix="fa")

    # Inicializar el marcador del coche
    coche_marker = folium.Marker(
        coordenadas[0],
        icon=icono_coche,
        popup="Punto 1",
    ).add_to(mapa_ruta)

    for indice, coord in enumerate(coordenadas):
        print(f"El coche se ha movido al punto {indice + 1}: {coord}")

        # Actualizar la posición del marcador del coche
        coche_marker.location = coord
        coche_marker.popup = f"Punto {indice + 1}"
        time.sleep(tiempo_espera)
        mapa_ruta.save(ruta_mapa_ruta)
        folium_static(mapa_ruta, width=800, height=600)

persona()
simular_movimiento_coche(coordenadas, 0.3)



'''
Ejecutando el comando 'streamlit run st_main.py.py' en la terminal se abre una pestaña localhost en el navegador.
En el mapa se puede ver el marcador del punto de inicio y el de destino, el del coche y la ruta que va a seguir el coche.
El problema es que streamlit no está diseñado para representar datos en streaming, 
así que lo máximo que he conseguido es que se vea un mapa por cada posición del vehículo :(

'''
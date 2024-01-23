import xml.etree.ElementTree as ET
import folium
import time

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
file_path = '/Users/andresrsalamanca/master_edem/Data_project_2/Ruz_Mest.kml'

#Extraer las coordenadas
coordenadas = extraer_coordenadas_kml(file_path)
#print(coordenadas)

# Crear un mapa
mapa_ruta = folium.Map(location=coordenadas[0], zoom_start=16)

# Agregar la línea de la ruta al mapa
folium.PolyLine(coordenadas, color='red', weight=2.8, opacity=1).add_to(mapa_ruta)

# Agregar un marcador para el punto de inicio (Punto A)
folium.Marker(
    coordenadas[0],
    popup="Punto A: Inicio",
    icon=folium.Icon(color="green", icon="play")
).add_to(mapa_ruta)

# Agregar un marcador para el punto final (Punto B)
folium.Marker(
    coordenadas[-1],
    popup="Punto B: Fin",
    icon=folium.Icon(color="red", icon="stop")
).add_to(mapa_ruta)

# Guardar el mapa como archivo HTML
ruta_mapa_ruta = '/Users/andresrsalamanca/master_edem/Data_project_2/mapa_ruta_completa.html'
mapa_ruta.save(ruta_mapa_ruta)

# Simular el movimiento del coche
def simular_movimiento_coche(coordenadas, tiempo_espera):
    for indice, coord in enumerate(coordenadas):
        print(f"El coche se ha movido al punto {indice + 1}: {coord}")
        time.sleep(tiempo_espera) 

# Simular el movimiento con un intervalo de 0.3 segundos entre puntos
simular_movimiento_coche(coordenadas, 0.3)





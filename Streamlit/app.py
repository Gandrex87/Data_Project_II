import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as plt
import seaborn as sns

# Configuración de credenciales (Opcional si ya tienes configuradas tus credenciales de GCP)
# Comenta esta sección si ya has configurado las credenciales de GCP de otra manera
credentials = service_account.Credentials.from_service_account_file(
    '/Users/miguelherrerofuertes/Documents/GitHub/Data_Project_II/mis_cosas/data-project-33-413616-7bc1975d859f-1.json')
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

#Cambiar el color del fondo de estremlit (pero hace que se vuelvan enormes las gráficas)
#st.set_page_config(page_title="Mi Aplicación", page_icon=":tada:", layout="wide")
# Intento de configuración del tema con CSS más específico
#st.markdown(
#    """
#    <style>
#    .stApp {
#        background-color: #ABECEC;
#    }
#    </style>
#    """,
#    unsafe_allow_html=True
#)

# Función para realizar consultas a BigQuery
def consulta_bigquery(sql_query):
    query_job = client.query(sql_query)
    results = query_job.result()  # Espera a que la consulta se complete.
    df = results.to_dataframe()
    # Renombrar columna aquí
    df = df.rename(columns={'car_id': 'ID Coche'})
    df = df.rename(columns={'ruta_id': 'ID Ruta'})
    df = df.rename(columns={'punto_inicio': 'Punto de Inicio'})
    df = df.rename(columns={'punto_destino': 'Punto de Destino'})
    df = df.rename(columns={'lat': 'Latitud'})
    df = df.rename(columns={'lon': 'Longitud'})
    df = df.rename(columns={'plazas_disponibles': 'Nº Plazas Disponibles'})
    df = df.rename(columns={'precio_distancia': 'Precio por Distancia (€/m)'})
    df = df.rename(columns={'trayectos_realizados': 'Nº Trayectos Realizado'})
    df = df.rename(columns={'personas_transportadas': 'Nº Personas Transportadas'})
    df = df.rename(columns={'persona_id': 'ID Persona'})
    df = df.rename(columns={'nombre': 'Nombre'})
    df = df.rename(columns={'lat_inicio': 'Latitud Inicio'})    
    df = df.rename(columns={'lon_inicio': 'Longitud Inicio'})
    df = df.rename(columns={'lat_destino': 'Latitud Destino'})    
    df = df.rename(columns={'lon_destino': 'Longitud Destino'})
    df = df.rename(columns={'presupuesto': 'Presupuesto (€)'})    
    df = df.rename(columns={'viajes_realizados': 'Nº Viajes Realizados'})
    df = df.rename(columns={'destino_coche': 'Destino del Coche'})
    df = df.rename(columns={'distanceDelPasajero': 'Distancia del Pasajero'})
    df = df.rename(columns={'dinero_recaudado': 'Precio del Trayecto (€)'})
    df = df.rename(columns={'precio_por_viaje': 'Precio del Trayecto (€)'}) 
    df = df.rename(columns={'timestamp': 'Timestamp'})
    return df


# -----------------   Funciones para Graficar   ------------------
# Función para graficar los 10 viajes de mayor precio
def graficar_top_viajes(df_matches):
    # Ordenar el DataFrame por la columna 'Precio del Trayecto (€)' de manera descendente
    df_top_viajes = df_matches.sort_values(by='Precio del Trayecto (€)', ascending=False).head(10)    
    df_top_viajes = df_top_viajes.iloc[::-1]
    # Crear un gráfico de barras vertical
    plt.figure(figsize=(10, 8))
    # Generar colores de claro a oscuro para cada barra
    colores = sns.color_palette("Greens", n_colors=len(df_top_viajes))

    plt.bar(df_top_viajes['ID Coche'], df_top_viajes['Precio del Trayecto (€)'], color=colores)
    plt.xlabel('ID Coche')
    plt.ylabel('Precio del Trayecto (€)')
    plt.title('Top 10 Viajes de Mayor Precio')
    plt.xticks(rotation=45)  # Rotar las etiquetas del eje X para mejor visualización

    st.pyplot(plt)

# Función para graficar las 20 personas con mayor presupuesto
def graficar_top_personas(df_personas):
    # Ordenar el DataFrame por la columna 'Presupuesto (€)' de manera descendente
    df_top_personas = df_personas.sort_values(by='Presupuesto (€)', ascending=False).head(20)
    df_top_personas = df_top_personas.iloc[::-1] 
    # Crear una paleta de colores naranjas de claro a oscuro
    colores = sns.color_palette("YlOrBr", len(df_top_personas))
    
    # Crear un gráfico de barras
    plt.figure(figsize=(10, 8))
    plt.barh(df_top_personas['Nombre'], df_top_personas['Presupuesto (€)'], color=colores)
    plt.xlabel('Presupuesto (€)')
    plt.ylabel('Nombre')
    plt.title('Top 20 Personas con Mayor Presupuesto')
    st.pyplot(plt)

# Función para graficar el mayor número de viajes
def graficar_top_destinos(df_matches):
    # Agrupar por 'Destino del Coche' y contar el número de viajes a cada destino
    df_top_destinos = df_matches.groupby('Destino del Coche').size().reset_index(name='Nº Viajes Realizados')
    # Ordenar el DataFrame por la columna 'Nº Viajes Realizados' de manera descendente
    df_top_destinos = df_top_destinos.sort_values(by='Nº Viajes Realizados', ascending=False)
    # Definir colores para cada sección del gráfico circular
    colores = ['lightblue', 'lightgreen', 'coral', 'cyan', 'blue', 'green']

    # Crear un gráfico circular
    plt.figure(figsize=(8, 8))
    plt.pie(df_top_destinos['Nº Viajes Realizados'], labels=df_top_destinos['Destino del Coche'], colors=colores, autopct='%1.1f%%', startangle=140)
    plt.title('Top 5 Viajes Realizados')
    st.pyplot(plt)

# Función para graficar los coches que más personas han transportado de la suma de todos los viajes que han hecho
def graficar_top_coches_por_personas_transportadas(df_matches):
    # Agrupar por 'ID Coche' y sumar el número de personas transportadas por cada coche
    df_top_coches = df_matches.groupby('ID Coche')['Nº Personas Transportadas'].sum().reset_index(name='Total Personas Transportadas')
    # Ordenar el DataFrame por 'Total Personas Transportadas' de manera descendente
    df_top_coches = df_top_coches.sort_values(by='Total Personas Transportadas', ascending=False).head(10)
    df_top_coches = df_top_coches.iloc[::-1]
    # Crear un gráfico de barras vertical
    plt.figure(figsize=(10, 8))

    # Generar colores de claro a oscuro
    colores = sns.color_palette("Greens", len(df_top_coches))  # Usando '_d' para asegurar colores oscuros en 'Blues'

    plt.bar(df_top_coches['ID Coche'], df_top_coches['Total Personas Transportadas'], color=colores)
    plt.ylabel('Total Personas Transportadas')  # Este es ahora el eje Y
    plt.xlabel('ID Coche')  # Este es el eje X
    plt.xticks(rotation=45)  # Rotar las etiquetas del eje X para mejor visualización
    plt.title('Top 10 Coches con Mayor Número de Personas Transportadas')
    st.pyplot(plt)

# Función para graficar los coches que más número de viajes han realizado
def graficar_top_coches_por_viajes_realizados(df_matches):
    # Agrupar por 'ID Coche' y contar el número de viajes realizados por cada coche
    df_top_coches_viajes = df_matches.groupby('ID Coche').size().reset_index(name='Nº Viajes Realizados')

    # Ordenar el DataFrame por 'Nº Viajes Realizados' de manera descendente
    df_top_coches_viajes = df_top_coches_viajes.sort_values(by='Nº Viajes Realizados', ascending=False).head(10)

    df_top_coches_viajes = df_top_coches_viajes.iloc[::-1]
    
    # Crear un gráfico de barras
    plt.figure(figsize=(10, 8))
    plt.barh(df_top_coches_viajes['ID Coche'], df_top_coches_viajes['Nº Viajes Realizados'])
    plt.xlabel('Nº Viajes Realizados')
    plt.ylabel('ID Coche')
    plt.title('Top 10 Coches con Mayor Número de Viajes Realizados')
    st.pyplot(plt)

# Función para graficar los coches que más dinero han recaudado de la suma de todos los viajes realizados
def graficar_top_coches_por_dinero_recaudado(df_matches):
    # Agrupar por 'ID Coche' y sumar el 'Precio del Trayecto (€)' para cada coche
    df_top_coches_dinero = df_matches.groupby('ID Coche')['Precio del Trayecto (€)'].sum().reset_index(name='Dinero Recaudado')
    # Ordenar el DataFrame por 'Dinero Recaudado' de manera descendente
    df_top_coches_dinero = df_top_coches_dinero.sort_values(by='Dinero Recaudado', ascending=False).head(10)
    df_top_coches_dinero = df_top_coches_dinero.iloc[::-1]
    # Crear un gráfico de barras vertical
    plt.figure(figsize=(10, 8))
    # Generar colores de claro a oscuro para cada barra
    colores = sns.color_palette("Greens", n_colors=len(df_top_coches_dinero))

    plt.bar(df_top_coches_dinero['ID Coche'], df_top_coches_dinero['Dinero Recaudado'], color=colores)
    plt.ylabel('Dinero Recaudado (€)')  # Este es ahora el eje Y
    plt.xlabel('ID Coche')  # Este es el eje X
    plt.xticks(rotation=45)  # Rotar las etiquetas del eje X para mejorar la legibilidad
    plt.title('Top 10 Coches con Mayor Dinero Recaudado')
    st.pyplot(plt)

# Función para graficar los coches que más media de personas por viaje realizado tienen
def graficar_top_coches_por_media_personas_por_viaje(df_matches):
    # Calcular la suma total de personas transportadas por cada coche
    total_personas = df_matches.groupby('ID Coche')['Nº Personas Transportadas'].sum().reset_index(name='Total Personas Transportadas')
    # Calcular el número total de viajes realizados por cada coche
    total_viajes = df_matches.groupby('ID Coche').size().reset_index(name='Total Viajes Realizados')
    # Unir ambos DataFrames para tener el total de personas transportadas y el total de viajes por coche
    df_media = pd.merge(total_personas, total_viajes, on='ID Coche')
    # Calcular la media de personas por viaje para cada coche
    df_media['Media Personas por Viaje'] = df_media['Total Personas Transportadas'] / df_media['Total Viajes Realizados']
    # Ordenar el DataFrame por 'Media Personas por Viaje' de manera descendente
    df_top_media = df_media.sort_values(by='Media Personas por Viaje', ascending=False).head(10)

    df_top_media = df_top_media.iloc[::-1]
    
    # Crear un gráfico de barras
    plt.figure(figsize=(10, 8))
    plt.barh(df_top_media['ID Coche'], df_top_media['Media Personas por Viaje'])
    plt.xlabel('Media de Personas por Viaje')
    plt.ylabel('ID Coche')
    plt.title('Top 10 Coches con Mayor Media de Personas por Viaje Realizado')
    st.pyplot(plt)

# Función para graficar las personas que más porcentaje de su presupuesto han gastado en viajes
def graficar_top_personas_por_porcentaje_presupuesto_gastado(df_personas, df_viajes):
    # Calcular el total de dinero gastado por cada persona en viajes
    total_gastado = df_viajes.groupby('ID Persona')['Precio del Trayecto (€)'].sum().reset_index(name='Total Gastado')
    # Unir el DataFrame de total gastado con el DataFrame de personas para tener tanto el presupuesto inicial como el nombre
    df_total_presupuesto = pd.merge(total_gastado, df_personas[['ID Persona', 'Presupuesto (€)', 'Nombre']], on='ID Persona')
    # Calcular el porcentaje del presupuesto gastado
    df_total_presupuesto['Porcentaje del Presupuesto Gastado'] = (df_total_presupuesto['Total Gastado'] / df_total_presupuesto['Presupuesto (€)']) * 100
    # Ordenar el DataFrame por 'Porcentaje del Presupuesto Gastado' de manera descendente
    df_top_porcentaje_gastado = df_total_presupuesto.sort_values(by='Porcentaje del Presupuesto Gastado', ascending=False).head(10)
    df_top_porcentaje_gastado = df_top_porcentaje_gastado.iloc[::-1]
    plt.figure(figsize=(10, 8))
    # Generar colores de claro a oscuro para cada barra utilizando seaborn
    colores = sns.color_palette("Greens", len(df_top_porcentaje_gastado))

    plt.bar(df_top_porcentaje_gastado['Nombre'], df_top_porcentaje_gastado['Porcentaje del Presupuesto Gastado'], color=colores)
    plt.ylabel('Porcentaje del Presupuesto Gastado (%)')  # Actualizar etiqueta del eje Y
    plt.xlabel('Nombre')  # Actualizar etiqueta del eje X
    plt.title('Top Personas por Porcentaje del Presupuesto Gastado en Viajes')
    plt.xticks(rotation=45)  # Rotar nombres para mejor visualización
    st.pyplot(plt)




# -----------------   Función principal del dashboard   ------------------
def main():
    st.title('Dashboard del Proyecto')

    # Tabla de Todos los Coches
    st.header('Tabla de Todos los Coches')
    query_coches = "SELECT * FROM `data-project-33-413616.dataprojectg4.coches_todos`"
    df_coches = consulta_bigquery(query_coches)
    st.write(df_coches)

    # Tabla de Todas las Personas
    st.header('Tabla de Todas las Personas')
    query_personas = "SELECT * FROM `data-project-33-413616.dataprojectg4.personas_todas`"
    df_personas = consulta_bigquery(query_personas)
    st.write(df_personas)

    # Tabla de Matches
    st.header('Tabla de Matches')
    query_matches = "SELECT * FROM `data-project-33-413616.dataprojectg4.matches`"
    df_matches = consulta_bigquery(query_matches)
    st.write(df_matches)

    # Llamar a la función para graficar el top de viajes de mayor precio
    st.subheader('Top 10 Viajes de Mayor Precio')
    graficar_top_viajes(df_matches)

    # Llamar a la función para graficar el top de personas con mayor presupuesto
    st.subheader('Top 20 Personas con Mayor Presupuesto')
    graficar_top_personas(df_personas)

    # Llamar a la función para graficar el top de los detinos con mayores viajes
    st.subheader('Top 5 destinos con Mayor Número de Viajes')
    graficar_top_destinos(df_matches)

    # Llamar a la función para graficar los coches que más personas han transportado
    st.subheader('Top 10 Coches con Mayor Número de Personas Transportadas')
    graficar_top_coches_por_personas_transportadas(df_matches)

    # Llamar a la función para graficar los coches que más número de viajes han realizado
    st.subheader('Top 10 Coches con Mayor Número de Viajes Realizados')
    graficar_top_coches_por_viajes_realizados(df_matches)

    # Llamar a la función para graficar los coches que más dinero han recaudado
    st.subheader('Top 10 Coches con Mayor Dinero Recaudado')
    graficar_top_coches_por_dinero_recaudado(df_matches)

    # Llamar a la función para graficar los coches que más media de personas por viaje realizado tienen
    st.subheader('Top 10 Coches con Mayor Media de Personas por Viaje Realizado')
    graficar_top_coches_por_media_personas_por_viaje(df_matches)

    # Llamar a la función para graficar las personas que más porcentaje de su presupuesto han gastado
    st.subheader('Top Personas por Porcentaje del Presupuesto Gastado en Viajes')
    graficar_top_personas_por_porcentaje_presupuesto_gastado(df_personas, df_matches)

if __name__ == "__main__":
    main()

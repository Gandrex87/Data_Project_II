#######################
# Import libraries
import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as plt
import seaborn as sns

client = bigquery.Client()

st.set_page_config(
    page_title="BlaBlaCar CityConnect Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

alt.themes.enable("dark")


# Configuración del tema con CSS más específico
st.markdown(
   """
   <style>
   .stApp {
       background-color: #111111;
   }
   .title-text {
        color: white !important;
    }
   </style>
   """,
   unsafe_allow_html=True
)

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

# Función para graficar los coches que más personas han transportado de la suma de todos los viajes que han hecho
def graficar_top_coches_por_personas_transportadas(df_matches):
    # Agrupar por 'ID Coche' y sumar el número de personas transportadas por cada coche
    df_top_coches = df_matches.groupby('ID Coche')['Nº Personas Transportadas'].sum().reset_index(name='Total Personas Transportadas')
    # Ordenar el DataFrame por 'Total Personas Transportadas' de manera descendente
    df_top_coches = df_top_coches.sort_values(by='Total Personas Transportadas', ascending=False).head(10)
    df_top_coches = df_top_coches
    # Crear un gráfico de barras vertical
    plt.figure(figsize=(11.5, 6))

    colores = sns.color_palette("Blues_r", len(df_top_coches))

    plt.bar(df_top_coches['ID Coche'], df_top_coches['Total Personas Transportadas'], color=colores)

    plt.ylabel('Total Personas Transportadas')
    plt.xlabel('ID Coche')  
    plt.xticks(rotation=60)  
    st.pyplot(plt)


# Función para graficar los coches que más número de viajes han realizado
def graficar_top_coches_por_viajes_realizados(df_matches):
    # Agrupar por 'ID Coche' y contar el número de viajes realizados por cada coche
    df_top_coches_viajes = df_matches.groupby('ID Coche').size().reset_index(name='Nº Viajes Realizados')

    # Ordenar el DataFrame por 'Nº Viajes Realizados' de manera descendente
    df_top_coches_viajes = df_top_coches_viajes.sort_values(by='Nº Viajes Realizados', ascending=False).head(10)

    df_top_coches_viajes = df_top_coches_viajes.iloc[::-1]
    colores = sns.color_palette("Blues", len(df_top_coches_viajes))
    
    # Crear un gráfico de barras
    plt.figure(figsize=(10, 6))
    plt.barh(df_top_coches_viajes['ID Coche'], df_top_coches_viajes['Nº Viajes Realizados'], color=colores)
    plt.xlabel('Nº Viajes Realizados')
    plt.ylabel('ID Coche')
    st.pyplot(plt)


# Función para graficar las 20 personas con mayor presupuesto
def graficar_top_personas(df_personas):
    # Ordenar el DataFrame por la columna 'Presupuesto (€)' de manera descendente
    df_top_personas = df_personas.sort_values(by='Presupuesto (€)', ascending=False).head(20)
    df_top_personas = df_top_personas.iloc[::-1] 
    # Crear una paleta de colores naranjas de claro a oscuro
    colores = sns.color_palette("Blues", len(df_top_personas))
    
    # Crear un gráfico de barras
    plt.figure(figsize=(7.5, 6))
    plt.barh(df_top_personas['Nombre'], df_top_personas['Presupuesto (€)'], color=colores)
    plt.xlabel('Presupuesto (€)')
    plt.ylabel('Nombre')
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
    df_top_porcentaje_gastado = df_top_porcentaje_gastado
    plt.figure(figsize=(12, 6))

    colores = sns.color_palette("Blues_r", len(df_top_porcentaje_gastado))

    plt.bar(df_top_porcentaje_gastado['Nombre'], df_top_porcentaje_gastado['Porcentaje del Presupuesto Gastado'], color=colores)
    plt.ylabel('Porcentaje del Presupuesto Gastado (%)')  
    plt.xlabel('Nombre')  
    plt.xticks(rotation=20)  # Rotar nombres para mejor visualización
    st.pyplot(plt)



# Función para graficar el mayor número de viajes
def graficar_top_destinos(df_matches):
     # Agrupar por 'Destino del Coche' y contar el número de viajes a cada destino
    df_top_destinos = df_matches.groupby('Destino del Coche').size().reset_index(name='Nº Viajes Realizados')
    # Ordenar el DataFrame por la columna 'Nº Viajes Realizados' de manera descendente
    df_top_destinos = df_top_destinos.sort_values(by='Nº Viajes Realizados', ascending=False).head(5)

    colores = sns.color_palette("Blues", len(df_top_destinos))

    # Crear un gráfico circular
    plt.figure(figsize=(6, 4.5))
    patches, texts, _ = plt.pie(df_top_destinos['Nº Viajes Realizados'], colors=colores, startangle=140, autopct='%1.1f%%')

    # Configurar la leyenda
    plt.legend(patches, df_top_destinos['Destino del Coche'], loc="lower center", bbox_to_anchor=(0.5, -0.3), fancybox=True, shadow=True, ncol=1)

    st.pyplot(plt)


# -----------------   Función principal del dashboard   ------------------
def main():
    st.markdown(
    """
    <h3 style='color: white;'>BlaBlaCar CityConnect</h1>
    """,
    unsafe_allow_html=True
    )
    col = st.columns((1, 4, 4, 2), gap='medium')
    
    query_matches = "SELECT * FROM `data-project-33-413616.dataprojectg4.matches2`"
    df_matches = consulta_bigquery(query_matches)

    query_personas = "SELECT * FROM `data-project-33-413616.dataprojectg4.personas_todas`"
    df_personas = consulta_bigquery(query_personas)

    query_carmoney = "SELECT `car_id`, round(sum(`precio_por_viaje`), 2) as `Dinero Recaudado` FROM `data-project-33-413616.dataprojectg4.matches2` GROUP BY `car_id` "
    df_carmoney = consulta_bigquery(query_carmoney)

    query_cochesgroup = "SELECT DISTINCT `car_id` FROM `data-project-33-413616.dataprojectg4.matches2`"
    df_cochesgroup = consulta_bigquery(query_cochesgroup)

    query_personasgroup = "SELECT DISTINCT `persona_id` FROM `data-project-33-413616.dataprojectg4.matches2`"
    df_personasgroup = consulta_bigquery(query_personasgroup)

    with col[0]:
        st.markdown("""
        <h5 style='color: white;'>Recuento</h1>
        """,
        unsafe_allow_html=True
        )
        coches_participando = len(df_cochesgroup)
        #st.metric(label="Nº Coches", value=coches_participando)

        # Crear el HTML para un cuadro blanco con texto negro centrado
        html_content = f"""
            <div style="background-color: white; padding: 10px; border-radius: 5px; margin-bottom: 20px;">
                <p style="color: black; text-align: center; margin: 0;">Nº Coches</p>
                <p style="color: black; text-align: center; font-size: 24px; margin: 0;"><b>{coches_participando}</b></p>
            </div>
            """

        # Mostrar el HTML en Streamlit usando st.markdown
        st.markdown(html_content, unsafe_allow_html=True)

        personas_participando = len(df_personasgroup)
        #st.metric(label="Nº Coches", value=coches_participando)

        # Crear el HTML para un cuadro blanco con texto negro centrado
        html_content = f"""
            <div style="background-color: white; padding: 10px; border-radius: 5px;">
                <p style="color: black; text-align: center; margin: 0;">Nº Personas</p>
                <p style="color: black; text-align: center; font-size: 24px; margin: 0;"><b>{personas_participando}</b></p>
            </div>
            """

        # Mostrar el HTML en Streamlit usando st.markdown
        st.markdown(html_content, unsafe_allow_html=True)

        

    with col[1]:
        st.markdown(
        """
        <h5 style='color: white;'>Top 10 Coches con Más Personas Transportadas</h1>
        """,
        unsafe_allow_html=True
        )
        # Llamar a la función para graficar los coches que más personas han transportado
        graficar_top_coches_por_personas_transportadas(df_matches)
        

        st.markdown(
        """
        <h5 style='color: white;'>Top 10 Coches con Más Viajes Realizados</h1>
        """,
        unsafe_allow_html=True
        )
        # Llamar a la función para graficar los coches que más número de viajes han realizado
        graficar_top_coches_por_viajes_realizados(df_matches)


    with col[2]:
        st.markdown(
        """
        <h5 style='color: white;'>Top 20 Personas con Mayor Presupuesto</h1>
        """,
        unsafe_allow_html=True
        )
        # Llamar a la función para graficar el top de personas con mayor presupuesto
        graficar_top_personas(df_personas)


        st.markdown(
        """
        <h5 style='color: white;'>Top Personas por % Presupuesto Gastado</h1>
        """,
        unsafe_allow_html=True
        )
        # Llamar a la función para graficar las personas que más porcentaje de su presupuesto han gastado
        graficar_top_personas_por_porcentaje_presupuesto_gastado(df_personas, df_matches)



    with col[3]:
        st.markdown(
        """
        <h5 style='color: white;'>Dinero Recaudado</h1>
        """,
        unsafe_allow_html=True
        )

        st.dataframe(df_carmoney,
                 column_order=("ID Coche", "Dinero Recaudado"),
                 hide_index=True,
                 width=200,
                 height = 300
                )
        
        st.markdown(
        """
        <h5 style='color: white;'>Top 5 Viajes</h1>
        """,
        unsafe_allow_html=True
        )
        graficar_top_destinos(df_matches)
                 
#---------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

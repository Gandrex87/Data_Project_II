import dash
from dash.dependencies import Input, Output
from dash import dcc, html 
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime


# Configura tus credenciales de BigQuery
client = bigquery.Client

# Inicializa la aplicación Dash
app = dash.Dash(__name__)

# Crea una figura de mapa en blanco para empezar
fig = go.Figure(go.Scattermapbox())

# Configura el layout (la visualización) del mapa
fig.update_layout(
    mapbox_style="open-street-map",
    mapbox_zoom=12,
    mapbox_center={"lat": 39.4699, "lon": -0.3763},  # Coordenadas aproximadas de Valencia
    height=800,  # Altura del mapa en píxeles
    width=1200    # Anchura del mapa en píxeles
)

# Función para obtener los datos de las coches de BigQuery y convertirlos en un DataFrame de pandas
def get_route_data():
    query = """
    SELECT *
    FROM `data-project-33-413616.dataprojectg4.coches_todos`
    ORDER BY car_id, timestamp ASC
    """
    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

# Función para obtener los datos de las personas de BigQuery y convertirlos en un DataFrame de pandas
def get_person_data(last_index, limit=10):
    query = f"""
    SELECT *
    FROM `data-project-33-413616.dataprojectg4.personas_todas`
    WHERE persona_id NOT IN (
        SELECT persona_id
        FROM `data-project-33-413616.dataprojectg4.matches2`
    )
    ORDER BY persona_id
    """
    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

# Función para obtener los IDs de las personas con matches
def get_matched_person_ids():
    query = """
    SELECT persona_id
    FROM `data-project-33-413616.dataprojectg4.matches2`
    """
    query_job = client.query(query)
    results = query_job.result()
    matched_person_ids = [row.persona_id for row in results]
    return matched_person_ids

# Función para obtener los nuevos matches desde el último tiempo de verificación
def get_new_matches(last_check_time):
    query = f"""
    SELECT car_id, persona_id
    FROM `data-project-33-413616.dataprojectg4.matches2`
    WHERE timestamp > '{last_check_time}'
    """
    query_job = client.query(query)
    results = query_job.result()
    return [(row.car_id, row.persona_id) for row in results]

# Define el layout de la aplicación Dash
app.layout = html.Div([
    html.H1("BlaBlaCar CityConnect", style={'textAlign': 'center'}),
    html.Div(id='match-message', style={'color': 'green', 'fontSize': '16px', 'textAlign': 'center'}),
    dcc.Store(id='last-check-time', data=str(datetime.utcnow())),  # Almacena el último momento de verificación
    html.Div(
        dcc.Graph(id='map-graph', style={'width': '80vw', 'height': '80vh'}),
        style={
            'display': 'flex',
            'justifyContent': 'center',
            'alignItems': 'center'
        }
    ),
    dcc.Interval(
        id='interval-component',
        interval=50000,  # Intervalo ajustado a 50 segundos para el ejemplo
        n_intervals=0
    )
])

# Callback para mostrar en el mapa los nuevos matches
@app.callback(
    Output('match-message', 'children'),
    [Input('interval-component', 'n_intervals'),
     Input('last-check-time', 'data')]  # Agrega Input para obtener el último tiempo de verificación
)
def update_match_message(n_intervals, last_check_time):
    # Suponiendo que implementas una función que verifica nuevos matches
    new_matches = get_new_matches(last_check_time)  # Proporciona last_check_time a la función
    
    if new_matches:
        # Genera los mensajes de match para mostrar
        messages = [f"¡¡Match del {match['car_id']}!!" for match in new_matches]
        return ", ".join(messages)
    return "No hay nuevos matches."


# Callback para actualizar el mapa con la ruta seleccionada
@app.callback(
    Output('map-graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_map(n_intervals):
    df = get_route_data()
    last_index = 0  # Definido para la paginación
    df_personas = get_person_data(last_index)
    matched_person_ids = get_matched_person_ids()
    # Filtra df_personas para excluir a las personas que están en matched_person_ids
    df_personas_filtered = df_personas[~df_personas['persona_id'].isin(matched_person_ids)]
    if not df.empty:
        fig = go.Figure()

        # Iterar sobre cada coche único
        for car_id in df['car_id'].unique():
            car_df = df[df['car_id'] == car_id]

            # El destino se considera como la última posición registrada para este car_id
            if not car_df.empty:
                destino_coche = car_df.iloc[-1]['destino_coche']  # Asumiendo que 'destino' es una columna en tu DataFrame

                # Añadir la ruta de este coche
                fig.add_trace(go.Scattermapbox(
                    mode="lines+markers",
                    lon=car_df['lon'],
                    lat=car_df['lat'],
                    text=[f"{car_id}"] * len(car_df), 
                    hoverinfo="text",
                    marker={'size': 5},
                    line={'width': 5},
                    name=f'Ruta {car_id}',
                    showlegend=False
                ))

                # Añadir un marcador especial para la posición actual del coche
                hover_text = f"{car_id} - Viaje: {destino_coche}"
                fig.add_trace(go.Scattermapbox(
                    mode="markers",
                    lon=[car_df.iloc[-1]['lon']],
                    lat=[car_df.iloc[-1]['lat']],
                    marker={'size': 10, 'color': 'orange'},
                    text=[hover_text],  # Establece el texto personalizado aquí
                    hoverinfo="text",  # Muestra solo el texto personalizado al pasar el cursor
                    name=f'Posición actual {car_id}',
                    showlegend=False
                ))
                
                # Marcador de inicio para esta ruta
                fig.add_trace(go.Scattermapbox(
                    mode="markers+text",
                    lon=[car_df['lon'].iloc[0]],
                    lat=[car_df['lat'].iloc[0]],
                    text=["Inicio"],
                    textposition="top right",
                    marker={'size': 12, 'color': 'green'},
                    hoverinfo='text',
                    showlegend=False
                ))
                
                # Marcador de destino para esta ruta
                fig.add_trace(go.Scattermapbox(
                    mode="markers+text",
                    lon=[car_df['lon'].iloc[-1]],
                    lat=[car_df['lat'].iloc[-1]],
                    text=["Destino"],
                    textposition="bottom right",
                    marker={'size': 12, 'color': 'red'},
                    hoverinfo='text',
                    showlegend=False
                ))
        
        # Añade marcadores para las personas
        for _, row in df_personas_filtered.iterrows():
            fig.add_trace(go.Scattermapbox(
                mode="markers+text",
                lon=[row['lon']],
                lat=[row['lat']],
                marker={'size': 10, 'color': 'purple'},
                text=[row['nombre']], 
                textposition="bottom right",
                hoverinfo='text',
                showlegend=False  
            ))

        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox_zoom=12,
            mapbox_center={"lat": df['lat'].mean(), "lon": df['lon'].mean()},
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        return fig
    return dash.no_update

if __name__ == '__main__':
    app.run_server(debug=True)
from fastapi import FastAPI
import pandas as pd
import uvicorn 
import numpy as np
from typing import Optional


app = FastAPI(title = 'Platforms_MLOPS')
df=pd.read_csv("datasets/plataformas.csv")

@app.get("/")
async def index():
    return "Hola! aqui puedes realizar consultas plataformas de streaming, para mas informacion ir a /docs"

@app.get('/get_max_duration/{year}/{platform}/{duration_type}')
def get_max_duration(year: int, platform: str, duration_type: str):
    platform_ids = {"amazon": "a", "disney": "d", "hulu": "h", "netflix": "n"}
    if platform not in platform_ids:
        return "Invalid platform. Please choose from 'amazon', 'disney', 'hulu', or 'netflix'."
    if year not in df['release_year'].unique():
        return f"No movies found for the year {year}."
    if duration_type not in df['duration_type'].unique():
        return f"No movies found for the duration type {duration_type}."
    id_plat = platform_ids[platform]
    temp_df = df[(df['release_year'] == year) & (df['id'].str.contains(id_plat)) & (df['type'] == "movie") & (df['duration_type'] == duration_type)].copy()
    if temp_df.empty:
        return f"No movies found for the year {year} and duration type {duration_type} on {platform}."
    temp_df['duration_minutes'] = temp_df['duration_int'].fillna(-99999)
    max_row = temp_df.loc[temp_df['duration_minutes'].idxmax()]
    return {'pelicula': max_row['title']}

@app.get('/get_score_count/{platform}/{scored}/{year}')
def get_score_count(platform: str, scored: float, year: int): #devolverme la cantidad de (solo) peliculas que tienen un score mayor a scored
    df_temp = df[(df["rating_y"] > scored ) & (df["release_year"] == year)]  #Filtro de busqueda por Score y Año
    platform_ids = {"amazon": "a", "disney": "d", "hulu": "h", "netflix": "n"}
    id_plat = platform_ids[platform]
    if platform not in platform_ids:
        return "Invalid platform. Please choose from 'amazon', 'disney', 'hulu', or 'netflix'."
    else: 
        df1 = df_temp[(df_temp["type"] == "movie") & (df_temp["id"].str.findall(id_plat))]             
        print("En", platform, "hay", df1.shape[0], "Peliculas de del año", year, "con puntaje mayor a", scored) 
        return {
            'plataforma': platform,
            'cantidad': df1.shape[0],
            'anio': year,
            'score': scored
        }      


@app.get('/prod_per_county/{tipo}/{pais}/{anio}')
def prod_per_county(tipo: str, pais: str, anio: int):
    count = df[(df['country'] == pais) & (df['release_year'] == anio) & (df['type'] == tipo)].shape[0]
    return {'pais': pais, 'anio': anio, 'peliculas': count}

@app.get('/get_actor/{platform}/{year}')
def get_actor(platform: str, year: int):
    platform_ids = {"amazon": "a", "disney": "d", "hulu": "h", "netflix": "n"}
    id_plat = platform_ids[platform]
    if platform not in platform_ids:
        return "Invalid platform. Please choose from 'amazon', 'disney', 'hulu', or 'netflix'."
    temp_df = df[(df['id'].str.contains(id_plat)) & (df['release_year'] == year)]
    cast_string = ','.join(temp_df['cast'].dropna().astype(str)) # Concatenate all the cast lists into a single string
    actor_list = cast_string.split(',') # Split the string into a list of individual actors
    actors = pd.Series(actor_list) #Create a pandas Series of the actors
    actors_strip = actors.str.strip()
    actor_counts = actors_strip.value_counts() #count pandas series 
    actor_name = actor_counts.index[0].split(',')[0] # Split the first element of actor_counts.index by comma and return the first item
    if actor_name == '':
        print(f"There are non actors related for the year",year, "in the platform",platform)  
    return actor_name  #preguntar y revisar el return

@app.get('/get_count_platform/{platform}')
def get_count_platform(platform: str,):  #devolver un iint con el numero total de peliculas de la plataforma amazon, netflix, hulu, disney
    platform_ids = {"amazon": "a", "disney": "d", "hulu": "h", "netflix": "n"}
    id_plat = platform_ids[platform]
    if platform not in platform_ids:
        return "Invalid platform. Please choose from 'amazon', 'disney', 'hulu', or 'netflix'."
    temp_df = df['id'].str.contains(id_plat) & (df.type == "movie")
    count = temp_df.value_counts().get(True, 0)
    print(f"{count} peliculas tiene la plataforma {platform}")
    return {'plataforma': platform, 'peliculas': count}
    
@app.get('/get_contents/{rating}')
def get_contents(rating): #int con cantidad de contenidos con la clasificacion indicada
    df_temp= df[df['rating_x'] == rating]
    total= df_temp['title'].count() 
    return {'rating': rating, 'contenido': int(total)}

if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)
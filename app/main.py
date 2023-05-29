import json
import math
import sys
import logging
import asyncio
from typing import Any, Dict, List
from fastapi import Body, FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from json import JSONEncoder
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
# Importo los endpoint del taller 2
from taller_2.get_tweets import router as tweets_router
from taller_2.polarity_model import router as polarity_router

app = FastAPI()
app.include_router(tweets_router)
app.include_router(polarity_router)
client = MongoClient('mongodb://localhost:27017')
# Seleccionar la base de datos y la colección
db = client['taller_1']
coleccion = db['datos_2']

mongostr = "mongodb+srv://ffserrano42:WB5I8PRECKh6rTEv@cluster0.ul9f2rr.mongodb.net/test"
port = 8000


#class path(BaseModel):
 #   path: str


#class file(BaseModel):
 #   path: str
  #  top: int | None

# class files(BaseModel):
    # paths: List[archivo] | None = None


#class pydantic_C(BaseModel):
 #   path: str | None
  #  count: int | None
   # words: dict | None


#class pydantic_L(BaseModel):
 #   paths: List[pydantic_C] | None = []


origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:4200",
    "http://127.0.0.1",
    "http://127.0.0.1:4200",
    "http://172.24.99.153:4200",
    "https://172.24.99.153:4200",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# tarea1_=tarea1
#file = archivo


@app.get('/')
async def heatlh():
    return {"healthcheck": "ok"}


@app.get("/taller_1/ejercicio_1/{fecha_inicio}/{fecha_fin}")
async def t1_ej_1(fecha_inicio: str, fecha_fin: str) -> List:
    client2 = MongoClient(mongostr, port)
    db_2 = client2["Taller1"]
    collecion2 = db_2["T2017_v2"]
    fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
    fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
    pipeline = [
        {
            "$match": {
                "DayM": {"$gte": fecha_inicio.day, "$lte": fecha_fin.day},
                "Month": {"$gte": fecha_inicio.month, "$lte": fecha_fin.month},
                "Year": {"$gte": fecha_inicio.year, "$lte": fecha_fin.year}
            }
        },
        {
            "$group": {
                "_id": {"Zone": "$Zone"},
                "count": {"$sum": '$count'}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
        {
            "$limit": 4
        }
    ]
    result = await asyncio.to_thread(list, collecion2.aggregate(pipeline))
    return result

# este metodo agrupa los viajes por zona en un intervalo de fechas
# retorna por año, por zona el numero total de viajes
@app.get("/taller_1/ejercicio_2/{Zone}/{fecha_inicio}/{fecha_fin}")
async def t1_ej_2(Zone: str, fecha_inicio: str, fecha_fin: str) -> List:
    # Creo el cliente de mongo a la base de datos en MongoAtlas
    # Adicione todas colecciones en una sola que se llama Boats, asi que solo busco esa coleccion.
    client2 = MongoClient(mongostr, port)
    db_2 = client2["Taller1"]
    collecion2 = db_2["T2017_v2"]
    fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
    fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
    pipeline = [
        {
            '$match': {
                "DayM": {"$gte": fecha_inicio.day, "$lte": fecha_fin.day},
                "Month": {"$gte": fecha_inicio.month, "$lte": fecha_fin.month},
                "Year": {"$gte": fecha_inicio.year, "$lte": fecha_fin.year},
                "Zone": Zone,
                'Cargo': {'$not': {'$type': 1}}
            }
        },      {
            '$group': {
                '_id': {
                    'Cargo': '$Cargo'
                },
                'trips': {
                    '$sum': '$count'
                }
            }
        }, {
            '$sort': {'_id': -1}
        },
        {
            "$limit": 1
        }
    ]
    # Imprimo en consola el pipeline que se envia a mongo.
    print(pipeline)
    result = await asyncio.to_thread(list, collecion2.aggregate(pipeline))
    return result


# este metodo agrupa los viajes en un intervalo de fechas y retorna por año, por zona, por mes el numero total de viajes
@app.get("/taller_1/ejercicio_3/{fecha_inicio}/{fecha_fin}")
async def t1_ej_3(fecha_inicio: str, fecha_fin: str) -> List:
    # Creo el cliente de mongo a la base de datos en MongoAtlas
    # Adicione todas colecciones en una sola que se llama Boats, asi que solo busco esa coleccion.
    client2 = MongoClient(mongostr, port)
    db_2 = client2["Taller1"]
    collecion2 = db_2["T2017_v2"]
    fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
    fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
    pipeline = [
        {
            '$match': {
                'Year': {
                    '$gte': fecha_inicio.year,
                    '$lte': fecha_fin.year
                }
            }
        }, {
            '$group': {
                '_id': {
                    'Year': '$Year',
                    'Zone': '$Zone',
                    'Month': '$Month'
                },
                'trips': {
                    '$sum': '$count'
                }
            }
        }, {
            '$sort': {'_id': -1}
        }
    ]
    # Imprimo en consola el pipeline que se envia a mongo.
    print(pipeline)
    result = await asyncio.to_thread(list, collecion2.aggregate(pipeline))
    return result

#este el metodo del punto cuatro del ejercicio 3
@app.get("/taller_1/ejercicio_4/{fecha_inicio}/{fecha_fin}")
async def t1_ej_4(fecha_inicio: str, fecha_fin: str) -> List:
    client2 = MongoClient(mongostr, port)
    db_2 = client2["Taller1"]
    collecion2 = db_2["T2017_v2"]
    fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
    fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
    pipeline = [
                    {
                    "$match": {
                    "DayM": {"$gte": fecha_inicio.day, "$lte": fecha_fin.day},
                    "Month": {"$gte": fecha_inicio.month, "$lte": fecha_fin.month},
                    "Year": {"$gte": fecha_inicio.year, "$lte": fecha_fin.year}
                    }
                    },
                    {
                    "$group": {
                    "_id": {"Zone": "$Zone"},
                    "count": {'$sum': '$count'}
                    }
                    },
                    {
                    "$sort": {
                    "count": -1
                    }
                    }
                ]
    result = await asyncio.to_thread(list, collecion2.aggregate(pipeline))
    return result



# este metodo agrupa los viajes en un intervalo años y los agrupa por año mes zona y dia de la semana
@app.get("/taller_1/ejercicio_5/{fecha_inicio}/{fecha_fin}")
async def t1_ej_5(fecha_inicio: str, fecha_fin: str) -> List:
    # Creo el cliente de mongo a la base de datos en MongoAtlas
    # Adicione todas colecciones en una sola que se llama Boats, asi que solo busco esa coleccion.
    client2 = MongoClient(mongostr, port)
    db_2 = client2["Taller1"]
    collecion2 = db_2["T2017_v2"]
    fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
    fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
    pipeline = [
        {
            '$match': {
                'Year': {
                    '$gte': fecha_inicio.year,
                    '$lte': fecha_fin.year
                }
            }
        }, {
            '$group': {
                '_id': {
                    'Year': '$Year',
                    'Zone': '$Zone',
                    'Month': '$Month',
                    "DayW":'$DayW'
                },
                'trips': {
                    '$sum': '$count'
                }
            }
        }, {
            '$sort': {'_id': -1}
        }
    ]
    # Imprimo en consola el pipeline que se envia a mongo.
    print(pipeline)
    result = await asyncio.to_thread(list, collecion2.aggregate(pipeline))
    return result

# este metodo funciona igual que el punto 5 pero hace un join con otra coleccion de zonas para agrupar por id de zona y obtener la densidad poblacional y su dinero (gdp)
@app.get("/taller_1/p4/{fecha_inicio}/{fecha_fin}")
async def t1_p4(fecha_inicio: str, fecha_fin: str) -> List:
    # Creo el cliente de mongo a la base de datos en MongoAtlas
    # Adicione todas colecciones en una sola que se llama Boats, asi que solo busco esa coleccion.
    client2 = MongoClient(mongostr, port)
    db_2 = client2["Taller1"]
    collecion2 = db_2["T2017_v2"]
    fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
    fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
    pipeline = [
                    {
                        '$match': {
                            'Year': {
                                '$gte': fecha_inicio.year, 
                                '$lte': fecha_fin.year
                            }
                        }
                    }, {
                        '$lookup': {
                            'from': 'Zone2', 
                            'localField': 'Zone', 
                            'foreignField': 'zone', 
                            'as': 'Zones'
                        }
                    }, {
                        '$unwind': {
                            'path': '$Zones'
                        }
                    }, {
                        '$group': {
                            '_id': {
                                'Year': '$Year', 
                                'Zone': '$Zone', 
                                'Month': '$Month', 
                                'Pop': '$Zones.pop', 
                                'Money': '$Zones.money'
                            }, 
                            'trips': {
                                '$sum': '$count'
                            }
                        }
                    }, {
                        '$sort': {
                            '_id': -1
                        }
                    }
                ]
    # Imprimo en consola el pipeline que se envia a mongo.
    print(pipeline)
    result = await asyncio.to_thread(list, collecion2.aggregate(pipeline))
    return result


# este metodo permite conocer las zonas que estan en la coleccion agrupada de botes
@app.get("/taller_1/zones/")
async def t1_Zones() -> List:
    # Creo el cliente de mongo a la base de datos en MongoAtlas
    # Adicione todas colecciones en una sola que se llama Boats, asi que solo busco esa coleccion.
    client2 = MongoClient(mongostr, port)
    db_2 = client2["Taller1"]
    collecion2 = db_2["T2017_v2"]
    pipeline = [
        {
            "$unwind": {
                "path": '$Zone',
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$group": {
                "_id": "Null",
                "Zones": {"$addToSet": '$Zone'}
            }
        }
    ]
    # Imprimo en consola el pipeline que se envia a mongo.
    print(pipeline)
    result = await asyncio.to_thread(list, collecion2.aggregate(pipeline))
    return result


@app.get("/collection_name")
async def collection_name():
    client2 = MongoClient(mongostr, port)
    db_2 = client2["Taller1"]
    collecion2 = db_2["T2017_v2"]
    logging.info("Se realizó la conexión a la base de datos exitosamente")
    return {"collection_name": collecion2.name}
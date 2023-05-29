from datetime import datetime
import nltk
import spacy
import asyncio
from nltk.corpus import stopwords
from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient
from datetime import datetime, timezone
from fastapi import APIRouter
from bson import ObjectId
from typing import List
from taller_2.dbPedia import consultar_dbpedia_spotlight
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import make_pipeline
import nltk
import re

# Crear el modelo
model = make_pipeline(CountVectorizer(), MultinomialNB())

router = APIRouter()

nltk.download('punkt')
nltk.download('stopwords')
nlp = spacy.load('es_core_news_sm')

# Conecta al servidor remoto de MongoDB
mongostr = "mongodb+srv://ffserrano42:WB5I8PRECKh6rTEv@cluster0.ul9f2rr.mongodb.net/test"
port = 8000
client = MongoClient(mongostr, port)
db = client["Taller2"]
collection = db["Tweets1"]
UsuariosC = db["Usuarios"]

# Conexión a la base de datos MongoDB
# client = MongoClient('mongodb://localhost:27017')
# db = client["taller_2"]
# collection = db["tweets_2"]

# Carga el léxico de emociones desde el archivo
emotion_lexicon = defaultdict(list)
emotions = ['anger', 'anticipation', 'disgust', 'fear', 'joy',
            'negative', 'positive', 'sadness', 'surprise', 'trust']

with open('taller_2/Spanish-NRC-EmoLex.txt', 'r', encoding='utf-8') as f:
    next(f)  # skip the header line
    for line in f:
        fields = line.strip().split('\t')
        spanish_word = fields[-1]
        emotion_scores = fields[1:-1]
        for emotion, score in zip(emotions, emotion_scores):
            if int(score) == 1:
                emotion_lexicon[emotion].append(spanish_word)

# Endpoint para analizar las emociones de los tweets filtrados por usuario y rango de fechas


@router.get("/taller_2/analyze_emotions/")
def analyze_emotions(usuarios: str, temas: str, fecha_inicio: str, fecha_fin: str):
    usuarios_list = usuarios.split(",")
    temas_list = temas.split(",")
    fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
    fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
    stop_words = set(stopwords.words('spanish'))

    query = {
        "Usuario": {"$in": usuarios_list},
        "Tema": {"$in": temas_list},
        "Fecha": {"$gte": fecha_inicio, "$lte": fecha_fin}
    }

    tweets = list(collection.find(query))  # Convert cursor to a list

    # Initialize data structures to store results
    emotion_counts_per_tweet = []
    emotion_counts_total = defaultdict(int)
    usuario_tema_emotion_counts = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int)))
    usuario_emotion_counts = defaultdict(lambda: defaultdict(int))
    usuario_tema_tweet_counts = defaultdict(lambda: defaultdict(int))
    usuario_tweet_counts = defaultdict(int)

    vectorizer = TfidfVectorizer()
    processed_tweets = []

    for tweet in tweets:
        processed_tweet = [w.lower() for w in nltk.word_tokenize(
            tweet['Tweet']) if w not in stop_words and w.isalpha()]
        processed_tweets.append(' '.join(processed_tweet))

    tfidf_matrix = vectorizer.fit_transform(processed_tweets)
    feature_names = vectorizer.get_feature_names_out()

    for tweet_index, tfidf_tweet in enumerate(tfidf_matrix):
        feature_index = tfidf_tweet.nonzero()[1]
        tfidf_scores = zip(
            feature_index, [tfidf_tweet[0, x] for x in feature_index])
        emotion_counts = defaultdict(int)

        for word, score in [(feature_names[i], score) for (i, score) in tfidf_scores]:
            for emotion, emotion_words in emotion_lexicon.items():
                if word in emotion_words:
                    emotion_counts[emotion] += score
                    emotion_counts_total[emotion] += score
                    usuario = tweets[tweet_index]['Usuario']
                    tema = tweets[tweet_index]['Tema']
                    usuario_tema_emotion_counts[usuario][tema][emotion] += score
                    usuario_emotion_counts[usuario][emotion] += score

        usuario_tema_tweet_counts[usuario][tema] += 1
        usuario_tweet_counts[usuario] += 1

        emotion_counts_per_tweet.append(dict(emotion_counts))

    polarity_indices_spanish = {
        'positive': 'Positivo',
        'fear': 'Miedo',
        'negative': 'Negativo',
        'joy': 'Alegría',
        'anticipation': 'Anticipación',
        'trust': 'Confianza',
        'anger': 'Ira',
        'sadness': 'Tristeza',
        'surprise': 'Sorpresa',
        'disgust': 'Asco'
    }

    # Convert defaultdicts to regular dicts for JSON serialization
    usuario_tema_emotion_counts = {usuario: {tema: dict(emotion_counts) for tema, emotion_counts in tema_emotion_counts.items(
    )} for usuario, tema_emotion_counts in usuario_tema_emotion_counts.items()}
    usuario_emotion_counts = {usuario: dict(
        emotion_counts) for usuario, emotion_counts in usuario_emotion_counts.items()}

    # Normalize counts by the number of tweets and translate emotion names to Spanish
    for usuario, tema_emotion_counts in usuario_tema_emotion_counts.items():
        for tema, emotion_counts in tema_emotion_counts.items():
            usuario_tema_emotion_counts[usuario][tema] = {polarity_indices_spanish[emotion]: round(
                value / usuario_tema_tweet_counts[usuario][tema], 2) for emotion, value in sorted(emotion_counts.items(), key=lambda x: x[1], reverse=True)}
        usuario_emotion_counts[usuario] = {polarity_indices_spanish[emotion]: round(
            value / usuario_tweet_counts[usuario], 2) for emotion, value in sorted(usuario_emotion_counts[usuario].items(), key=lambda x: x[1], reverse=True)}

    emotion_results = []
    for tweet, emotion_counts in zip(tweets, emotion_counts_per_tweet):
        tweet["_id"] = str(tweet["_id"])  # Convert ObjectId to string
        polarity_sorted = dict(
            sorted(emotion_counts.items(), key=lambda x: x[1], reverse=True))
        tweet["Polaridad"] = {polarity_indices_spanish[emotion]: round(
            value, 2) for emotion, value in sorted(polarity_sorted.items(), key=lambda x: x[1], reverse=True)}
        emotion_results.append(tweet)

    polarity_sorted_total = {polarity_indices_spanish[emotion]: round(
        value / len(tweets), 2) for emotion, value in sorted(emotion_counts_total.items(), key=lambda x: x[1], reverse=True)}

    return {
        "usuario": usuarios,
        "analisis_por_tema": usuario_tema_emotion_counts,
        "analisis_por_usuario": usuario_emotion_counts,
        "analisis_por_tweet": emotion_results,
        "analisis_agregado": polarity_sorted_total
    }


# Servicio que obtiene el listado total de tweets en la base de datos


@router.get("/taller_2/all_tweets")
async def all_tweets() -> List:
    try:
        # Realiza la consulta para obtener todos los documentos
        documentos = await asyncio.to_thread(list, collection.find({}))
        for doc in documentos:
            # Convertir los ObjectId en cadenas de texto
            doc['_id'] = str(doc['_id'])
        return documentos
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene el listado de todos los usuarios


@router.get("/taller_2/all_users")
async def all_users() -> List:
    try:
        pipeline = [
            {'$group': {'_id': '$Usuario'}},
            {"$project": {"_id": 0, "Usuario": "$_id"}}
        ]
        result = await asyncio.to_thread(list, collection.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene el detalle de ese Usuario


@router.get("/taller_2/detail_users")
async def detail_users(Username) -> List:
    try:
        query = {"Usuario": Username}
        result = await asyncio.to_thread(list, UsuariosC.find(query))
        for Usuario in result:
            Usuario["_id"] = str(Usuario["_id"])
        return result
    except Exception as e:
        return {"error": str(e)}


# Servicio que obtiene el listado de todos los temas


@router.get("/taller_2/all_subjects")
async def all_subjects() -> List:
    try:
        pipeline = [
            {'$group': {'_id': '$Tema'}},
            {"$project": {"_id": 0, "Tema": "$_id"}}
        ]
        result = await asyncio.to_thread(list, collection.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}


# Servicio que obtiene el listado de todos los Jobs de los usuarios
@router.get("/taller_2/all_jobs")
async def all_jobs() -> List:
    try:
        pipeline = [
            {'$group': {'_id': '$Profesion'}},
            {"$project": {"_id": 0, "Profesion": "$_id"}}
        ]
        result = await asyncio.to_thread(list, UsuariosC.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene el listado de todos los Generos de los usuarios


@router.get("/taller_2/all_gender")
async def all_gender() -> List:
    try:
        pipeline = [
            {'$group': {'_id': '$Genero'}},
            {"$project": {"_id": 0, "Genero": "$_id"}}
        ]
        result = await asyncio.to_thread(list, UsuariosC.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene el listado de todos los Sectores de los usuarios


@router.get("/taller_2/all_sectors")
async def all_sectors() -> List:
    try:
        pipeline = [
            {'$group': {'_id': '$Sector'}},
            {"$project": {"_id": 0, "Sector": "$_id"}}
        ]
        result = await asyncio.to_thread(list, UsuariosC.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene un arreglo de polaridades por usuarios en un intervalo de tiempo


@router.get("/taller_2/tweets_user_polaridad")
async def tweets_user_polaridad(fecha_inicio, fecha_fin) -> List:
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
        fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
        pipeline = [
            {
                '$match': {
                    'Fecha': {
                        '$gte': datetime(fecha_inicio.year, fecha_inicio.month, fecha_inicio.day, 0, 0, 0, tzinfo=timezone.utc),
                        '$lt': datetime(fecha_fin.year, fecha_fin.month, fecha_fin.day, 0, 0, 0, tzinfo=timezone.utc)
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'Usuario': '$Usuario',
                        'Polaridad': '$Polaridad'
                    },
                    'total_tweets': {
                        '$sum': 1
                    }
                }
            },
            {
                '$group': {
                    '_id': '$_id.Usuario',
                    'polaridades': {
                        '$push': {
                            'polaridad': '$_id.Polaridad',
                            'total_tweets': '$total_tweets'
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'polaridades': 1,
                    'Usuario': '$_id',
                }
            },
            {
                '$sort': {
                    'Usuario': 1
                }
            }
        ]
        result = await asyncio.to_thread(list, collection.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene un arreglo de polaridades por Temas en un intervalo de tiempo


@router.get("/taller_2/tweets_theme_polaridad")
async def tweets_theme_polaridad(fecha_inicio, fecha_fin) -> List:
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
        fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
        pipeline = [
            {
                '$match': {
                    'Fecha': {
                        '$gte': datetime(fecha_inicio.year, fecha_inicio.month, fecha_inicio.day, 0, 0, 0, tzinfo=timezone.utc),
                        '$lt': datetime(fecha_fin.year, fecha_fin.month, fecha_fin.day, 0, 0, 0, tzinfo=timezone.utc)
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'Tema': '$Tema',
                        'Polaridad': '$Polaridad'
                    },
                    'total_tweets': {
                        '$sum': 1
                    }
                }
            },
            {
                '$group': {
                    '_id': '$_id.Tema',
                    'polaridades': {
                        '$push': {
                            'polaridad': '$_id.Polaridad',
                            'total_tweets': '$total_tweets'
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'Tema': '$_id',
                    'polaridades': 1
                }
            },
            {
                '$sort': {
                    'Tema': 1
                }
            }
        ]
        result = await asyncio.to_thread(list, collection.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene un arreglo de polaridades por Ubicacion en un intervalo de tiempo


@router.get("/taller_2/tweets_ubicacion_polaridad")
async def tweets_ubicacion_polaridad(fecha_inicio, fecha_fin) -> List:
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
        fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
        pipeline = [
            {
                '$match': {
                    'Fecha': {
                        '$gte': datetime(fecha_inicio.year, fecha_inicio.month, fecha_inicio.day, 0, 0, 0, tzinfo=timezone.utc),
                        '$lt': datetime(fecha_fin.year, fecha_fin.month, fecha_fin.day, 0, 0, 0, tzinfo=timezone.utc)
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'Ubicacion': '$Ubicación',
                        'Polaridad': '$Polaridad'
                    },
                    'total_tweets': {
                        '$sum': 1
                    }
                }
            },
            {
                '$group': {
                    '_id': '$_id.Ubicacion',
                    'polaridades': {
                        '$push': {
                            'polaridad': '$_id.Polaridad',
                            'total_tweets': '$total_tweets'
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'polaridades': 1,
                    'Ubicacion': '$_id',
                }
            },
            {
                '$sort': {
                    'Ubicacion': 1
                }
            }
        ]
        result = await asyncio.to_thread(list, collection.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene por Polaridad cuantos tweets se han publicado en un intervalo de tiempo


@router.get("/taller_2/tweets_polaridad")
async def tweets_polaridad(fecha_inicio, fecha_fin) -> List:
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
        fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
        pipeline = [
            {
                '$match': {
                    'Fecha': {
                        '$gte': datetime(fecha_inicio.year, fecha_inicio.month, fecha_inicio.day, 0, 0, 0, tzinfo=timezone.utc),
                        '$lt': datetime(fecha_fin.year, fecha_fin.month, fecha_fin.day, 0, 0, 0, tzinfo=timezone.utc)
                    }
                }
            },
            {
                '$group': {
                    '_id': '$Polaridad',
                    'total_tweets': {
                        '$sum': 1
                    }
                }
            }
        ]
        result = await asyncio.to_thread(list, collection.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene por fecha la polaridad de los tweets


@router.get("/taller_2/tweets_fechas_polaridad")
async def tweets_fechas_polaridad() -> List:
    try:
        pipeline = [
            {
                '$project': {
                    'fecha': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$Fecha'
                        }
                    },
                    'Polaridad': 1
                }
            },
            {
                '$group': {
                    '_id': {
                        'fecha': '$fecha',
                        'Polaridad': '$Polaridad'
                    },
                    'total_tweets': {
                        '$sum': 1
                    }
                }
            },
            {
                '$group': {
                    '_id': '$_id.fecha',
                    'polaridades': {
                        '$push': {
                            'polaridad': '$_id.Polaridad',
                            'total_tweets': '$total_tweets'
                        }
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'polaridades': 1,
                    'Fecha': '$_id'
                }
            },
            {
                '$sort': {
                    'Fecha': 1
                }
            }
        ]
        result = await asyncio.to_thread(list, collection.aggregate(pipeline))
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene los Tweets por Usuario


@router.get("/taller_2/tweets_by_user")
async def tweets_by_user(Username) -> List:
    try:
        query = {"Usuario": Username}
        result = await asyncio.to_thread(list, collection.find(query))
        for tweet in result:
            tweet["_id"] = str(tweet["_id"])
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene el Tweet por Id


@router.get("/taller_2/tweet_by_Id")
def tweet_by_Id(Id):
    try:
        _id = ObjectId(Id)
        query = {"_id": _id}
        tweet = collection.find_one(query)
        tweet["_id"] = str(tweet["_id"])
        mensaje = tweet["Tweet"]
        return tweet
    except Exception as e:
        return {"error": str(e)}

# Servicio que realiza la obtencion de entidades de un tweet utilizando spacy


@router.get("/taller_2/semantic_analysis_by_tweet_id")
async def semantic_analysis_by_tweet_id(Id):
    try:
        _id = ObjectId(Id)
        query = {"_id": _id}
        tweet = collection.find_one(query)
        tweet["_id"] = str(tweet["_id"])
        mensaje = tweet["Tweet"]
        usuario = tweet["Usuario"]
        doc = nlp(mensaje)
        resultados = []
        for token in doc:
            resultado = {
                "texto": token.text,
                "etiquetado_gramatical": token.pos_,
                "dependencia_sintactica": token.dep_,
                "tipo_entidad_reconocida": token.ent_type_
            }
            resultados.append(resultado)
        respuesta = {
            "texto_original": mensaje,
            "tokens": resultados,
            "Usuario": usuario,
        }
        return respuesta
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene todos los tweets escritos por un genero


@router.get("/taller_2/tweets_by_gender")
async def tweets_by_gender(gender: str, fecha_inicio: str, fecha_fin: str):
    try:
        fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
        fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")

        pipeline = [
            {
                '$match': {
                    'Genero': gender
                }
            },
            {
                '$lookup': {
                    'from': collection.name,
                    'let': {'usuario': '$Usuario', 'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin},
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {'$eq': ['$Usuario', '$$usuario']},
                                        {'$gte': ['$Fecha', '$$fecha_inicio']},
                                        {'$lte': ['$Fecha', '$$fecha_fin']}
                                    ]
                                }
                            }
                        },
                        {
                            '$project': {'Tweet': 1}
                        }
                    ],
                    'as': 'tweets_usuario'
                }
            },
            {
                '$project': {
                    'Usuario': 1,
                    'Genero': 1,
                    'Sector': 1,
                    'Profesion': 1,
                    'Edad': 1,
                    'tweets_usuario': {
                        '$map': {
                            'input': '$tweets_usuario',
                            'as': 'tweet',
                            'in': {
                                'Tweet': '$$tweet.Tweet'
                            }
                        }
                    }
                }
            }
        ]
        result = await asyncio.to_thread(list, UsuariosC.aggregate(pipeline))
        for usuario in result:
            usuario["_id"] = str(usuario["_id"])
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene todos los tweets escritos por una profesion


@router.get("/taller_2/tweets_by_job")
async def tweets_by_job(Job):
    try:
        pipeline = [
            {
                '$match': {
                    'Profesion': Job
                }
            }, {
                '$lookup': {
                    'from': collection.name,
                    'localField': 'Usuario',
                    'foreignField': 'Usuario',
                    'as': 'tweets_usuario'
                }
            }, {
                '$project': {
                    'Usuario': 1,
                    'Genero': 1,
                    'Sector': 1,
                    'Profesion': 1,
                    'Edad': 1,
                    'tweets_usuario': {
                        '$map': {
                            'input': '$tweets_usuario',
                            'as': 'tweet',
                            'in': {
                                'Tweet': '$$tweet.Tweet'
                            }
                        }
                    }
                }
            }
        ]
        result = await asyncio.to_thread(list, UsuariosC.aggregate(pipeline))
        for usuario in result:
            usuario["_id"] = str(usuario["_id"])
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio que obtiene todos los tweets escritos por un sector en particular.


@router.get("/taller_2/tweets_by_sector")
async def tweets_by_sector(sector: str, fecha_inicio: str, fecha_fin: str):
    try:
        # Convertir las cadenas de fecha en objetos de fecha
        fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
        fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")

        pipeline = [
            {
                '$match': {
                    'Sector': sector
                }
            },
            {
                '$lookup': {
                    'from': collection.name,
                    'let': {'usuario': '$Usuario', 'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin},
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {'$eq': ['$Usuario', '$$usuario']},
                                        {'$gte': ['$Fecha', '$$fecha_inicio']},
                                        {'$lte': ['$Fecha', '$$fecha_fin']}
                                    ]
                                }
                            }
                        },
                        {
                            '$project': {'Tweet': 1}
                        }
                    ],
                    'as': 'tweets_usuario'
                }
            },
            {
                '$project': {
                    'Usuario': 1,
                    'Genero': 1,
                    'Sector': 1,
                    'Profesion': 1,
                    'Edad': 1,
                    'tweets_usuario': {
                        '$map': {
                            'input': '$tweets_usuario',
                            'as': 'tweet',
                            'in': {
                                'Tweet': '$$tweet.Tweet'
                            }
                        }
                    }
                }
            }
        ]
        result = await asyncio.to_thread(list, UsuariosC.aggregate(pipeline))
        for usuario in result:
            usuario["_id"] = str(usuario["_id"])
        return result
    except Exception as e:
        return {"error": str(e)}

# Servicio de enriquecimiento de contenido con base en los resultados del api de DBPedia


@router.get("/taller_2/dbpedia_by_tweet_id")
async def dbpedia_by_tweet_id(Id):
    try:
        _id = ObjectId(Id)
        query = {"_id": _id}
        tweet = collection.find_one(query)
        tweet["_id"] = str(tweet["_id"])
        mensaje = tweet["Tweet Normalizado"]
        ubicacion = tweet["Ubicación"]
        usuario = tweet["Usuario"]
        data = consultar_dbpedia_spotlight(mensaje)
        data_ubicacion = consultar_dbpedia_spotlight(ubicacion)

        if data_ubicacion is not None and "Resources" in data_ubicacion:
            data_ubicacion = data_ubicacion["Resources"]
        else:
            data_ubicacion = []

        new_data_u = []
        surfaceForms_u = set()  # Conjunto para almacenar las surfaceForms únicas
        for item in data_ubicacion:
            surfaceForm = item["@surfaceForm"]
            if surfaceForm not in surfaceForms_u:
                new_item = {
                    "@surfaceForm": surfaceForm,
                    "@URI": item["@URI"]
                }
                new_data_u.append(new_item)
                surfaceForms_u.add(surfaceForm)

        if data is not None and "Resources" in data:
            data = data["Resources"]
        else:
            data = []

        new_data = []
        surfaceForms = set()  # Conjunto para almacenar las surfaceForms únicas
        for item in data:
            surfaceForm = item["@surfaceForm"]
            if surfaceForm not in surfaceForms:
                new_item = {
                    "@surfaceForm": surfaceForm,
                    "@URI": item["@URI"]
                }
                new_data.append(new_item)
                surfaceForms.add(surfaceForm)

        respuesta = {
            "texto_original": mensaje,
            "ubicacion": ubicacion,
            "Usuario": usuario,
            "DbPediaUbicacion": new_data_u,
            "DbPedia": new_data
        }
        return respuesta
    except Exception as e:
        return {"error": str(e)}

@router.on_event("shutdown")
def shutdown_event():
    client.close()

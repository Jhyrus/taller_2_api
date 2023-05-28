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

router = APIRouter()

nltk.download('punkt')
nltk.download('stopwords')
nlp = spacy.load('es_core_news_sm')

# Conecta al servidor remoto de MongoDB
mongostr = "mongodb+srv://ffserrano42:WB5I8PRECKh6rTEv@cluster0.ul9f2rr.mongodb.net/test"
port = 8000
client = MongoClient(mongostr, port)
db = client["Taller2"]
collection = db["Tweets"]

# Conexión a la base de datos MongoDB
#client = MongoClient('mongodb://localhost:27017')
#db = client["taller_2"]
#collection = db["tweets_2"]

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
def analyze_emotions(usuarios: str, fecha_inicio: str, fecha_fin: str):
    usuarios_list = usuarios.split(",")
    fecha_inicio = datetime.strptime(fecha_inicio, "%d-%m-%Y")
    fecha_fin = datetime.strptime(fecha_fin, "%d-%m-%Y")
    stop_words = set(stopwords.words('spanish'))
    processed_tweets = []

    query = {
        "Usuario": {"$in": usuarios_list},
        "Fecha": {"$gte": fecha_inicio, "$lte": fecha_fin}
    }

    tweets = list(collection.find(query))  # Convert cursor to a list

    for tweet in tweets:
        processed_tweet = [w.lower() for w in nltk.word_tokenize(
            tweet['Tweet']) if w not in stop_words and w.isalpha()]
        processed_tweets.append(' '.join(processed_tweet))

    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(processed_tweets)

    feature_names = vectorizer.get_feature_names_out()
    emotion_counts_per_tweet = []

    for tweet_index, tfidf_tweet in enumerate(tfidf_matrix):
        feature_index = tfidf_tweet.nonzero()[1]
        tfidf_scores = zip(
            feature_index, [tfidf_tweet[0, x] for x in feature_index])
        emotion_counts = defaultdict(int)

        for word, score in [(feature_names[i], score) for (i, score) in tfidf_scores]:
            for emotion, emotion_words in emotion_lexicon.items():
                if word in emotion_words:
                    emotion_counts[emotion] += score

        emotion_counts_per_tweet.append(dict(emotion_counts))

    polarity_indices_spanish = {
        'positive': 'positivo',
        'fear': 'miedo',
        'negative': 'negativo',
        'joy': 'alegría',
        'anticipation': 'anticipación',
        'trust': 'confianza',
        'anger': 'ira',
        'sadness': 'tristeza',
        'surprise': 'sorpresa',
        'disgust': 'asco'
    }

    emotion_results = []
    for tweet, emotion_counts in zip(tweets, emotion_counts_per_tweet):
        tweet["_id"] = str(tweet["_id"])  # Convert ObjectId to string
        polarity_sorted = dict(
            sorted(emotion_counts.items(), key=lambda x: x[1], reverse=True))
        tweet["Polaridad"] = {polarity_indices_spanish[emotion]: value for emotion, value in polarity_sorted.items()}
        emotion_results.append(tweet)

    return emotion_results

# Servicio que obtiene el listado total de tweets en la base de datos
@router.get("/taller_2/all_tweets")
async def all_tweets() -> List:
    try:
        documentos = await asyncio.to_thread(list, collection.find({}))  # Realiza la consulta para obtener todos los documentos
        for doc in documentos:
            doc['_id'] = str(doc['_id'])  # Convertir los ObjectId en cadenas de texto
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

@router.get("/taller_2/semantic_analysis_by_tweet_id")
async def semantic_analysis_by_tweet_id(Id):
    try:
        _id = ObjectId(Id)
        query = {"_id": _id}
        tweet = collection.find_one(query)
        tweet["_id"] = str(tweet["_id"])
        mensaje = tweet["Tweet"]
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
            "tokens": resultados
        }
        return respuesta
    except Exception as e:
        return {"error": str(e)}


@router.get("/taller_2/dbpedia_by_tweet_id")
async def dbpedia_by_tweet_id(Id):
    try:
        _id = ObjectId(Id)
        query = {"_id": _id}
        tweet = collection.find_one(query)
        tweet["_id"] = str(tweet["_id"])
        mensaje = tweet["Tweet"]
        ubicacion = tweet["Ubicación"]
        data = consultar_dbpedia_spotlight(mensaje)
        data_ubicacion = consultar_dbpedia_spotlight(ubicacion)
        if "Resources" in data_ubicacion:
            data_ubicacion = data_ubicacion["Resources"]
            new_data_u = []
            for item in data_ubicacion:
                new_item = {
                    "@surfaceForm": item["@surfaceForm"],
                    "@URI": item["@URI"]
                }
                new_data_u.append(new_item)
        else:
            new_data_u = []

        if "Resources" in data:
            data = data["Resources"]
            new_data = []
            for item in data:
                new_item = {
                    "@surfaceForm": item["@surfaceForm"],
                    "@URI": item["@URI"]
                }
                new_data.append(new_item)
        else:
            new_data = []
        respuesta = {
            "texto_original": mensaje,
            "ubicacion": ubicacion,
            "DbPediaUbicacion": new_data_u,
            "DbPedia": new_data
        }
        return respuesta
    except Exception as e:
        return {"error": str(e)}

@router.on_event("shutdown")
def shutdown_event():
    client.close()

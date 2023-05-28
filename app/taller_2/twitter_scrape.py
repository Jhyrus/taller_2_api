import snscrape.modules.twitter as sntwitter
import pandas as pd
import re
import numpy as np
from pymongo import MongoClient
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
from scipy.special import softmax
# from app.Tweets import obtenerTweets,registrarSentiment
# from app.dbPedia import consultar_dbpedia_spotlight


# Conecta al servidor remoto de MongoDB
# mongostr = "mongodb+srv://ffserrano42:WB5I8PRECKh6rTEv@cluster0.ul9f2rr.mongodb.net/test"
# port = 8000
# client = MongoClient(mongostr, port)

# Conexión al servidor local de MongoDB
client = MongoClient('mongodb://localhost:27017')

db = client["taller_2"]
collection = db["tweets_2"]

usuarios = [
    # "RevistaSemana",
    # "CaracolTV",
    # "NoticiasRCN",
    # "lasillavacia",
    # "City_Noticias",
    # "MinSaludCol",
    # "MintrabajoCol",
    # "infopresidencia",
    "petrogustavo",
    "DanielSamperO",
    "IvanDuque",
    "DCoronell",
    "VickyDavilaH",
    "AlvaroUribeVel",
    "FranciaMarquezM",
    "ANDI_Colombia",
    "FenalcoNacional",
    "ACOPIAntioquia",
    "ACHC_Col",
    "JuanManSantos",
    "ClaudiaLopez",
    "VickyDavilaH",
    "MJDuzan",
    "IvanCepedaCast",
    "ZuluagaCamila",
    "MdeFrancisco12",
    "EnriquePenalosa",
    "sergio_fajardo",
    "cesaralo",
    "FernanMartinez",
    "YolandaRuizCe",
    "patriciajaniot"
    "ELTIEMPO",
    "elespectador",
]

temas = {
    "reforma laboral": ["reforma laboral", "legislación laboral"],
    "reforma tributaria": ["reforma tributaria"],
    "reforma salud": ["reforma salud", "reforma de salud"],
    "reforma pensional": ["reforma pensional"],
    "niños del guaviare": ["niños del guaviare", "selvas del Guaviare", "niños desaparecidos en la selva", "selva del Guaviare", "niños perdidos en la selva"],
    "sin gas": ["sin gas", "problema del gas"],
    "nutresa": ["nutresa"],
    "mundial sub 20": ["mundial sub 20"],
    "fiscal barbosa": ["fiscal barbosa", "Francisco Barbosa"],
}

tweets = []
limit = 10


def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)


# # Obtener todas las frases clave de los temas
# all_keywords = []
# for frases_clave in temas.values():
#     all_keywords.extend(frases_clave)

# for tema, frases_clave in temas.items():
#     combined_query = " OR ".join(frases_clave)
#     for usuario in usuarios:
#         query = f'from:{usuario} ({combined_query}) lang:es since:2023-05-20 until:2023-05-30'
#         for tweet in sntwitter.TwitterSearchScraper(query).get_items():
#             if len(tweets) == limit:
#                 break
#             else:
#                 for frase_clave in frases_clave:
#                     # Normalizamos el texto eliminando las mayúsculas y los signos de puntuación
#                     normalized_tweet = re.sub(
#                         r'[^\w\s]', '', tweet.rawContent.lower())
#                     normalized_phrase = re.sub(
#                         r'[^\w\s]', '', frase_clave.lower())

#                     if normalized_phrase in normalized_tweet:
#                         user_location = tweet.user.location if hasattr(
#                             tweet.user, 'location') else ''
#                         model_path = "daveni/twitter-xlm-roberta-emotion-es"
#                         tokenizer = AutoTokenizer.from_pretrained(model_path)
#                         config = AutoConfig.from_pretrained(model_path)
#                         # PT
#                         model = AutoModelForSequenceClassification.from_pretrained(
#                             model_path)
#                         Analysis = []
#                         normalized_tweet = preprocess(normalized_tweet)
#                         encoded_input = tokenizer(normalized_tweet, return_tensors='pt')
#                         output = model(**encoded_input)
#                         scores = output[0][0].detach().numpy()
#                         scores = softmax(scores)
#                         # Print labels and scores
#                         ranking = np.argsort(scores)
#                         ranking = ranking[::-1]
#                         max_score = 0
#                         max_label = ""
#                         # print(normalized_tweet)
#                         # dbpedia_result = consultar_dbpedia_spotlight(normalized_tweet)
#                         for i in range(scores.shape[0]):
#                             l = config.id2label[ranking[i]]
#                             s = scores[ranking[i]]
#                             if s > max_score:
#                                 max_score = np.round(float(s), 2)
#                                 max_label = l
#                         tweets.append(
#                             [tweet.date, tweet.user.username, tweet.content, user_location, tema, max_label, max_score])
#                         break

# for usuario in usuarios:
#     combined_query = f'from:{usuario} ({" OR ".join(all_keywords)}) lang:es since:2023-05-20 until:2023-05-30'
#     print(combined_query)
#     for tweet in sntwitter.TwitterSearchScraper(combined_query).get_items():
#         if len(tweets) == limit:
#             break
#         else:
#             for tema, frases_clave in temas.items():
#                 for frase_clave in frases_clave:
#                     normalized_tweet = re.sub(
#                         r'[^\w\s]', '', tweet.rawContent.lower())
#                     normalized_phrase = re.sub(
#                         r'[^\w\s]', '', frase_clave.lower())

#                     if normalized_phrase in normalized_tweet:
#                         user_location = tweet.user.location if hasattr(
#                             tweet.user, 'location') else ''
#                         model_path = "daveni/twitter-xlm-roberta-emotion-es"
#                         tokenizer = AutoTokenizer.from_pretrained(model_path)
#                         config = AutoConfig.from_pretrained(model_path)
#                         model = AutoModelForSequenceClassification.from_pretrained(
#                             model_path)
#                         Analysis = []
#                         normalized_tweet = preprocess(normalized_tweet)
#                         encoded_input = tokenizer(
#                             normalized_tweet, return_tensors='pt')
#                         output = model(**encoded_input)
#                         scores = output[0][0].detach().numpy()
#                         scores = softmax(scores)
#                         ranking = np.argsort(scores)
#                         ranking = ranking[::-1]
#                         max_score = 0
#                         max_label = ""
#                         for i in range(scores.shape[0]):
#                             l = config.id2label[ranking[i]]
#                             s = scores[ranking[i]]
#                             if s > max_score:
#                                 max_score = np.round(float(s), 2)
#                                 max_label = l
#                         tweets.append([tweet.date, tweet.user.username, tweet.rawContent,
#                                       user_location, tema, max_label, max_score])
#                         break

for usuario in usuarios:
    for tema, frases_clave in temas.items():
        combined_query = f'from:{usuario} ({" OR ".join(frases_clave)}) lang:es since:2023-04-20 until:2023-05-30'
        for tweet in sntwitter.TwitterSearchScraper(combined_query).get_items():
            # print(tweet.rawContent)
            print(len(tweets))
            # if len(tweets) == limit:
            #     break
            # else:
            for frase_clave in frases_clave:
                # Normalizamos el texto eliminando las mayúsculas y los signos de puntuación
                normalized_tweet = re.sub(
                    r'[^\w\s]', '', tweet.rawContent.lower())
                normalized_phrase = re.sub(
                    r'[^\w\s]', '', frase_clave.lower())

                if normalized_phrase in normalized_tweet:
                    user_location = tweet.user.location if hasattr(
                        tweet.user, 'location') else ''
                    model_path = "daveni/twitter-xlm-roberta-emotion-es"
                    tokenizer = AutoTokenizer.from_pretrained(model_path)
                    config = AutoConfig.from_pretrained(model_path)
                    model = AutoModelForSequenceClassification.from_pretrained(
                        model_path)
                    Analysis = []
                    normalized_tweet = preprocess(normalized_tweet)
                    encoded_input = tokenizer(
                        normalized_tweet, return_tensors='pt')
                    output = model(**encoded_input)
                    scores = output[0][0].detach().numpy()
                    scores = softmax(scores)
                    ranking = np.argsort(scores)
                    ranking = ranking[::-1]
                    max_score = 0
                    max_label = ""
                    for i in range(scores.shape[0]):
                        l = config.id2label[ranking[i]]
                        s = scores[ranking[i]]
                        if s > max_score:
                            max_score = np.round(float(s), 2)
                            max_label = l
                    tweets.append([tweet.date, tweet.user.username, tweet.rawContent,
                                  user_location, tema, max_label, max_score])
                    tweet_data = {
                        'Fecha': tweet.date,
                        'Usuario': tweet.user.username,
                        'Tweet': tweet.rawContent,
                        'Tweet Normalizado': normalized_tweet,
                        'Ubicación': user_location,
                        'Tema': tema,
                        'Polaridad': max_label,
                        'Score': max_score
                    }
                    collection.insert_one(tweet_data)
                    break

# columnas = ['Fecha', 'Usuario', 'Tweet', 'Ubicación', 'Tema', "Label", "Score"]
# df = pd.DataFrame(tweets, columns=columnas)
# documentos = df.to_dict("records")

# try:
#     resultado = collection.insert_many(documentos)
#     print("Insertion successful. Document IDs:", resultado.inserted_ids)
# except Exception as e:
#     print("An error occurred:", e)

# print(df)

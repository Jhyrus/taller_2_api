import tweepy
from fastapi import APIRouter

router = APIRouter()

# Autenticación con la API de Twitter
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''
bearer_token = ''

# Crear un objeto API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# auth = tweepy.Client(bearer_token)
api = tweepy.API(auth)


@router.get("/tweets/{tweet_id}")
async def get_tweet(tweet_id: str):
    # obtener un tweet específico
    tweet = api.get_status(tweet_id)

    # imprimir el tweet obtenido
    print(tweet.text)
    return {"message": "Tweet obtained successfully"}

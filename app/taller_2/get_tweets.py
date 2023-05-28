import tweepy
from fastapi import APIRouter

router = APIRouter()

# Autenticación con la API de Twitter
consumer_key = '6rspBEF6G2rPms0S3z6oeAnCf'
consumer_secret = '6XX1bBs1C2maB9Rss5bSz7SMOyFVNZ1LJJelgkePRqQ5f4Vnts'
access_token = '1655223340657721347-BSBqDgk5PrlbR4LSwpw0TR9Z3gDUDz'
access_token_secret = 'J12sHEFyfsE6OKqDR74fjwCL0sDzZYK6yOlTjBR71s7EV'
bearer_token = 'aVRjODJmaDZVeFZPRDhrUHRoUENoN2ExcHBOaWxaUEhPSmxneTJUc2pnWXBJOjE2ODM0ODc4OTExODg6MToxOmF0OjE'

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

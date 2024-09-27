# Shopify API
import shopify

def extract_shopify_data(api_key, password, shop_name):
    shop_url = f"https://{api_key}:{password}@{shop_name}.myshopify.com/admin"
    shopify.ShopifyResource.set_site(shop_url)
    
    # Extraire les commandes des 30 derniers jours
    orders = shopify.Order.find(limit=250, status="any", created_at_min="2024-08-27")
    
    # Extraire les produits
    products = shopify.Product.find(limit=250)
    
    return {'orders': orders, 'products': products}

# Twitter API
import tweepy

def extract_twitter_data(consumer_key, consumer_secret, access_token, access_token_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    # Rechercher des tweets contenant des mots-clés spécifiques
    tweets = api.search_tweets(q="ecommerce OR online shopping", count=100)
    
    return tweets

# Facebook Graph API
import facebook

def extract_facebook_data(access_token, page_id):
    graph = facebook.GraphAPI(access_token)
    
    # Extraire les posts de la page
    posts = graph.get_connections(page_id, 'posts')
    
    # Extraire les insights de la page
    insights = graph.get_connections(page_id, 'insights')
    
    return {'posts': posts, 'insights': insights}

# Google Analytics API
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def extract_ga_data(view_id, credentials):
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    
    response = analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': view_id,
                    'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
                    'metrics': [{'expression': 'ga:sessions'}, {'expression': 'ga:pageviews'}],
                    'dimensions': [{'name': 'ga:date'}]
                }
            ]
        }
    ).execute()
    
    return response

# Web Server Logs
import re
from datetime import datetime

def parse_apache_log(log_file_path):
    log_pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d+) (\d+)'
    parsed_logs = []

    with open(log_file_path, 'r') as file:
        for line in file:
            match = re.match(log_pattern, line)
            if match:
                ip, _, _, timestamp, request, status, size = match.groups()
                parsed_logs.append({
                    'ip': ip,
                    'timestamp': datetime.strptime(timestamp, '%d/%b/%Y:%H:%M:%S %z'),
                    'request': request,
                    'status': int(status),
                    'size': int(size)
                })
    
    return parsed_logs

# OpenWeatherMap API
import requests

def get_weather_data(api_key, city):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }
    response = requests.get(base_url, params=params)
    return response.json()

# Alpha Vantage API (Market Data)
def get_stock_data(api_key, symbol):
    base_url = "https://www.alphavantage.co/query"
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': api_key
    }
    response = requests.get(base_url, params=params)
    return response.json()

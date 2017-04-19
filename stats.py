import requests
import json
import pymongo
import asyncio
import concurrent.futures
from pymongo import MongoClient

client = MongoClient()
db = client.jet

products = db.products
nutrition = db.nutrition
nutritionGrocer = db.nutritionGrocer

products = len(products.distinct('upc'))
nutrition = (len(nutrition.distinct('upc')) + len(nutritionGrocer.distinct('upc')))
print('Unique products:', products)
print('Nutrition found:', nutrition)
print('% found:', round(nutrition / products * 100.0, 1))

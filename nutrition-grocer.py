import requests
import json
import pymongo
import asyncio
import re
import concurrent.futures
from pymongo import MongoClient

options = [
    'http://www.shoprite.com/pd/',
    'http://www.keyfood.com/pd/',
    'http://www.acmemarkets.com/pd/',
    'http://grocery.peapod.com/pd/',
    'http://stopandshop.peapod.com/pd/',
    'http://grocery.thepig.net/pd/'
]

current = options[0]

client = MongoClient()
db = client.jet

products = db.products
nutrition = db.nutrition
nutritionGrocer = db.nutritionGrocer
nutritionGrocer.create_index([("upc", pymongo.DESCENDING)], unique=True)

def findNutrition(upc):
    if not nutritionGrocer.find_one({'upc': upc}):
        print('finding nutrition for: ' + upc)
        
        response = requests.request('GET', current + '-/-/-/' + upc)

        if "<h1>Nutrition</h1>" in response.text:
            search = {
                'serving_size': r"Serving Size (.*?)<\/b>",
                'serving_count': r"Servings Per Container ([\d.]+)",
                'calories': r"Calories<\/b> ([\d.]+)",
                'calories_from_fat': r"Calories from Fat ([\d.]+)",
                'fat': r"Total Fat<\/b> ([\d.]+)",
                'saturated_fat': r"Saturated Fat ([\d.]+)",
                'polyunsaturated_fat': r"Polyunsaturated Fat ([\d.]+)",
                'monounsaturated_fat': r"Monounsaturated Fat ([\d.]+)",
                'trans_fat': r"Trans<\/i> Fat ([\d.]+)",
                'cholesterol': r"Cholesterol<\/b> ([\d.]+)",
                'sodium': r"Sodium<\/b> ([\d.]+)m",
                'carb': r"Carbohydrate<\/b> ([\d.]+)",
                'dietary_fiber': r"Dietary Fiber ([\d.]+)",
                'sugar': r"Sugars ([\d.]+)",
                'protein': r"Protein<\/b> ([\d.]+)",
                'vitamin_a': r"Vitamin A.*?([\d.]+)%",
                'vitamin_c': r"Vitamin C.*?([\d.]+)%",
                'vitamin_d': r"Vitamin D.*?([\d.]+)%",
                'calcium': r"Calcium.*?([\d.]+)%",
                'iron': r"Iron.*?([\d.]+)%",
                'thiamin': r"Thiamin.*?([\d.]+)%",
                'riboflavin': r"Riboflavin.*?([\d.]+)%",
                'niacin': r"Niacin.*?([\d.]+)%",
                'folic_acid': r"Folic Acid.*?([\d.]+)%",
                'ingredients': r"Ingredients<\/h1>[\s]*<p>([\s\S]*?)<",
                'directions': r"Directions<\/h1>[\s]*<p>([\s\S]*?)<"
            }
            
            obj = {'upc': upc, 'grocer': current}
            
            for key in search.keys():
                match = re.search(search[key], response.text)
                if match:
                    obj[key] = match.group(1)
                    
                    if key not in ['serving_size', 'ingredients', 'directions']:
                        obj[key] = float(obj[key])
                
            
            if 'ingredients' in obj: 
                obj['ingredients'] = list(filter(lambda x: x != '', [i.strip().lower() for i in re.split(r"[,\.]",obj['ingredients'])]))
                
            try:
                nutritionGrocer.insert_one(obj)
            except pymongo.errors.DuplicateKeyError:
                return 'already inserted'

async def main():
    toFind = list(set(products.distinct('upc')) - set(nutrition.distinct('upc')) - set(nutritionGrocer.distinct('upc')))
    toFind.reverse()
    print('finding ' + str(len(toFind)) + ' nutritions:')
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor, 
                findNutrition, 
                i
            )
            for i in toFind
        ]
        for response in await asyncio.gather(*futures):
            pass

loop = asyncio.get_event_loop()
future = asyncio.ensure_future(main())
loop.run_until_complete(future)

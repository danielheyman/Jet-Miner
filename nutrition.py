import requests
import json
import pymongo
import asyncio
import re
import concurrent.futures
from pymongo import MongoClient

client = MongoClient()
db = client.jet

products = db.products
nutrition = db.nutrition
nutritionGrocer = db.nutritionGrocer
nutrition.create_index([("upc", pymongo.DESCENDING)], unique=True)

def findNutrition(upc):
    if not nutrition.find_one({'upc': upc}):
        print('finding nutrition for: ' + upc)
        
        response = requests.request('GET', 'https://ndb.nal.usda.gov/ndb/search/list?qlookup=' + upc)
        
        if "Click to view reports for this food" in response.text:
            id = response.text.split("/ndb/foods/show/")[1].split("?")[0]
            response = requests.request('GET', 'https://ndb.nal.usda.gov/ndb/foods/show/' + id + '?format=Abridged&reportfmt=csv&Qv=1')
            obj = {
                'upc': upc,
                'ndb_url': int(id)
            }

            matches = re.search(r"for: (\d+), ([\S ]+), UPC[\S\s]+?,\"([.\d]+) ([\S ]+) = ([.\d]+)g", response.text)
            if matches:
                obj['ndb_id'] = int(matches.group(1))
                obj['name'] = matches.group(2).lower()
                obj['unit'] = float(matches.group(3))
                obj['unit_type'] = matches.group(4)
                obj['eq_gram'] = float(matches.group(5))
                
                matchIngredients = re.search(r"Ingredients\s\"([\S\s]+?)\.{0,1}\"", response.text)
                if matchIngredients:
                    obj['ingredients'] = matchIngredients.group(1).lower().split(", ")

                matchall = re.findall(r"\"([\S ]+)\",\w+(?:\S+)?,([\d.]+),[\d.]+", response.text)
                for i in matchall:
                    obj[i[0]] = float(i[1]);
            
            try:
                nutrition.insert_one(obj)
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

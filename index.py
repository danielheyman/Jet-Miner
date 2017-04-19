import requests
import json
import pymongo
import asyncio
import concurrent.futures
from pymongo import MongoClient

client = MongoClient()
db = client.jet

products = db.products
products.create_index([("req", pymongo.DESCENDING)], unique=True)



headers = {
  'cookie': "jet.csrf=0uzZWZ4wZYT3ES-YNgz_EIMk",
  'x-csrf-token': "IGyasXCd-qJ8-OLAr6d3WdHN3kKET4ARNNMc",
  'content-type': "application/json"
}

zipcode = '07624'
startAtCat = '15900412'


async def getCats():
  url = "https://jet.com/api/nav/category"
  payload = '{"zipcode": "' + zipcode + '"}'
  
  response = requests.request("POST", url, data=payload, headers=headers)
  res = json.loads(response.text)
  
  for i in res['result']:
    if i['name'] != 'Grocery': continue
    
    await getCatChildren(i['children'])
    
  
async def getCatChildren(children):
  for i in children:
    if 'children' in i and len(i['children']) != 0:
      await getCatChildren(i['children'])
    else:
      await getBrands(i['id'])


async def getBrands(cat):
  print("finding cat: " + cat)
  url = "https://jet.com/api/search"
  payload = '{"zipcode": "' + zipcode + '", "categories": "' + cat + '", "origination": "PLP"}'

  response = requests.request("POST", url, data=payload, headers=headers)
  res = json.loads(response.text)
  
  global startAtCat
  if startAtCat and startAtCat != cat: 
    return
  startAtCat = False
    
  with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    loop = asyncio.get_event_loop()
    futures = [
      loop.run_in_executor(
        executor, 
        getProducts, 
        cat, 
        i['name'], 
        1
      )
      for i in res['result']['brandFacets']
    ]
    for response in await asyncio.gather(*futures):
      pass


def getProducts(cat, brand, page):
  print("finding cat>brand: " + cat + '>' + brand)
  url = "https://jet.com/api/search"
  payload = '{"zipcode": "' + zipcode + '", "categories": "' + cat + '", "page": "' + str(page) + '", "brands": "' + brand + '", "origination": "PLP"}'

  response = requests.request("POST", url, data=payload.encode('utf-8'), headers=headers)
  res = json.loads(response.text)
  
  for i in res['result']['products']:
    getProduct(i['id'])
  
  if page < res['result']['total'] / 24.0:
    getProducts(cat, brand, page + 1)
  
  
def getProduct(sku):
  if not products.find_one({'req.body': {'skuId': sku, 'zipCode': zipcode}}):
    print("finding product: " + sku)
    url = "https://jet.com/api/product/v2"
    payload = '{"zipcode":"' + zipcode + '","sku":"' + sku + '","origination":"PLP"}'

    response = requests.request("POST", url, data=payload, headers=headers)
    try:
      res = json.loads(response.text)
      products.insert_one(res['result'])
    except pymongo.errors.DuplicateKeyError:
      print('already inserted')
    except ValueError:
      print('an error occured: ' + sku)
  

async def main():
  v = await getCats()
  print('done')


loop = asyncio.get_event_loop()  
loop.run_until_complete(main())  
loop.close()  

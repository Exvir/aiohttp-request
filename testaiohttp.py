import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import lxml
import pprint
import time
import requests

'''
async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

async def run(l):
    url = "http://localhost:8000/category/{}"
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with ClientSession() as session:
        for item in items:
            task = asyncio.ensure_future(fetch(url.format(item), session))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        # you now have all response bodies in this variable
        print(responses)

def print_responses(result):
    print(result)

items=['watchs-of-man', 'watch-of-woman']
loop = asyncio.get_event_loop()
future = asyncio.ensure_future(run(items))
loop.run_until_complete(future)

'''
async def get_html(url, session):
        async with session.get(url) as response:
            return await response.text()
            

async def series_get_html(list_url):

    tasks = []
    async with ClientSession() as session:
        for url in list_url:
            task = asyncio.ensure_future(get_html(url, session))
            tasks.append(task)

        return await asyncio.gather(*tasks)

def parser_href(list_a_tag):
    list_href = []
    for a in list_a_tag:
        list_href.append(a.get('href'))
    return list_href


def parser_html(list_html):
    for html in list_html:
        soup = BeautifulSoup(html, 'html.parser')
        list_a_tag = soup.findAll('a')
        list_href = parser_href(list_a_tag)
        
    print(list_href)


if __name__ == "__main__":
    list_url = ['http://127.0.0.1:8000/']
    loop = asyncio.get_event_loop() #инициализация
    future = asyncio.ensure_future(series_get_html(list_url))
    list_html = loop.run_until_complete(future) #возвращает список всех полученных html
    #print(list_html)
    parser_html(list_html)


    #Так последоательно можно получить html
    response = requests.get('http://127.0.0.1:8000/').text
    #так получается объект суп
    soup = BeautifulSoup(response, 'html.parser')
    #Так можно найти значение ссылки первой a
    soup.find('a').get('href')
    #перебрать все ссылки
    aa = soup.findAll('a')
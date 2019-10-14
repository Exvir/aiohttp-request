from datetime import datetime
import asyncio
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup
import pprint
import time
import requests
import re
from sqlalchemy.orm import sessionmaker

from aiopg.sa import create_engine
import sqlalchemy as sa

#Таблица
metadata = sa.MetaData()

tbl = sa.Table('tbl', metadata,
               sa.Column('id', sa.Integer, primary_key=True),
               sa.Column('val', sa.String(255)))


async def create_table(engine):
    async with engine.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS tbl')
        await conn.execute('''CREATE TABLE tbl (
                                  id serial PRIMARY KEY,
                                  val varchar(500))''')


async def insert_to_table(url, engine):
    #Нужно для каждой транзакции своё соединение открывать!
    async with engine.acquire() as connect:
        await connect.execute(tbl.insert().values(val=f'{url}'))
        print(f'Запись: {datetime.now().isoformat()}')


async def get_html(url, session):
        async with session.get(url) as response:
            #Вызов сопрограммы (генератора)
            result = await response.text()
            print(f'Запрос: {datetime.now().isoformat()}')
            return result

async def series_get_html(list_url):
    tasks1 = []
    async with create_engine(user='exvir',
                             database='webproject',
                             host='127.0.0.1',
                             password='qscft813813'
                             ) as engine:
        await create_table(engine)                     
        async with ClientSession(timeout=ClientTimeout(total=120)) as session:
            N = range(len(list_url))
            for url in list_url:
                await insert_to_tab(f'{url}', engine)
                task1 = asyncio.create_task(get_html(f'{url}', session))
                #print(f'Зашёл в {url}')
                tasks1.append(task1)

            #Ожидает завершения всех фьючерсов, нужно чтобы синхронизировать все результаты и вывести их имено в этом порядке
            list_html = await asyncio.gather(*tasks1)
            return list_html

'''
async def get_html(url, session):
    async with session.get(url) as response:
        
        #Вызов сопрограммы (генератора)
        result = await response.text()
        print(datetime.now().isoformat())
        return result
            

async def series_get_html(list_url):

    tasks = []
    async with ClientSession(timeout=ClientTimeout(total=600)) as session:
        N = range(len(list_url))
        for url in list_url:
            #Создаётся список задач, тут можно вставить цикл for
            task = asyncio.create_task(get_html(f'{url}', session))
            #print(f'Зашёл в {url}')
            tasks.append(task)
        #Ожидает завершения всех фьючерсов, нужно чтобы синхронизировать все результаты и вывести их имено в этом порядке
        return await asyncio.gather(*tasks)
'''

#Принимает список тегов, возвращает список ссылок
def parser_href(list_a_tag):
    list_href = []
    for a in list_a_tag:
        href1 = 'https://ru.wikipedia.org'+a.get('href')
        list_href.append(href1)
    return list_href

#принимает список html, возвращает список href
def parser_html(list_html):
    list_href = []
    for html in list_html:
        soup = BeautifulSoup(html, 'html.parser')
        #print(soup)
        list_a_tag = soup.findAll('a', href=re.compile('^/wiki/*'))
        #print(f'Список а тегов {list_a_tag}')
        list_href += parser_href(list_a_tag)
    return list_href

def recursion(list_url, steps=1):

    if steps == 0:
        print('Последний шаг сделан')
        print(datetime.now().isoformat())
    else:
        #Создаётся асинхронная петля
        loop = asyncio.get_event_loop()
        #создаётся фьючерс, который передставляет собой асинхронного клиента, которому передаётся список url
        future = asyncio.ensure_future(series_get_html(list_url))
        #Почитать про метод
        list_html = loop.run_until_complete(future)
        list_href = parser_html(list_html)
        #print(f'Нужно зайти {list_href}')
        print(f'Сделан шаг 1, осталось шагов {steps-1}')
        print(datetime.now().isoformat())
        recursion(list_href, steps-1)

def sinhron(list_url):
    for url in list_url:
        list_html = []
        html = requests.get(url).text
        list_html.append(html)
        list_href = parser_html(list_html)
        print(datetime.now().isoformat())


if __name__ == "__main__":
    list_url = ['https://ru.wikipedia.org/wiki/Анантнаг_(округ)']
    '''
    response = requests.get('https://ru.wikipedia.org/wiki/%D0%94%D0%B6%D0%BE%D0%BA%D0%B5%D1%80_(%D1%84%D0%B8%D0%BB%D1%8C%D0%BC,_2019)').text
    #так получается объект суп
    soup = BeautifulSoup(response, 'html.parser')
    #Так можно найти значение ссылки первой a
    list_a_tag = soup.findAll('a', href=re.compile('^/wiki/*'))
    list_url = parser_href(list_a_tag)

    #перебрать все ссылки

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(series_get_html(list_url))
    loop.run_until_complete(future)
    print(datetime.now().isoformat())'''
    recursion(list_url, steps=2)
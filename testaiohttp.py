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

page = sa.Table('page', metadata,
               sa.Column('id_page', sa.Integer, primary_key=True),
               sa.Column('url', sa.String(255)),
               )


async def create_table_page(engine):
    async with engine.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS page')
        await conn.execute('''CREATE TABLE page (
                                id_page serial PRIMARY KEY,
                                url varchar(500)
                                )''')


relation = sa.Table('relation', metadata,
               sa.Column('id_link', sa.Integer, primary_key=True),
               sa.Column('id_page', sa.Integer)
                    )


async def create_table_relation(engine):
    async with engine.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS relation')
        await conn.execute('''CREATE TABLE relation (
                                id_link serial PRIMARY KEY,
                                id_page Integer)''')                                 


async def create_postgres_tables():
    async with create_engine(user='testtaskuser',
                            database='testtasks',
                            host='127.0.0.1',
                            password='qazwsx134134'
                            ) as engine:
        #Нужно для каждой транзакции своё соед
        await create_table_page(engine)
        await create_table_relation(engine)


async def request(url, session):
        async with session.get(url) as response:
            #Вызов сопрограммы (генератора)
            html = await response.text()
            print(f'Запрос: {datetime.now().isoformat()}')
            return html


async def insert_to_table_page(url):
    async with create_engine(user='testtaskuser',
                            database='testtasks',
                            host='127.0.0.1',
                            password='qazwsx134134'
                            ) as engine:
        #Нужно для каждой транзакции своё соединение открывать!
        async with engine.acquire() as connect:
            await connect.execute(page.insert().values(url=f'{url}'))
            print(f'Запись: {datetime.now().isoformat()}')


async def series_requests(list_url):
    tasks_request = []
    tasks_create_page = []                 
    async with ClientSession(timeout=ClientTimeout(total=120)) as session:
        for url in list_url:
            task_request = asyncio.create_task(request(f'{url}', session))
            task_create_page = asyncio.create_task(insert_to_table_page(f'{url}'))
            tasks_request.append(task_request)
            tasks_create_page.append(task_create_page)
        #Ожидает завершения всех фьючерсов, нужно чтобы синхронизировать все результаты и вывести их имено в этом порядке
        list_html = await asyncio.gather(*tasks_request)
        await asyncio.gather(*tasks_create_page)
        return list_html


#Принимает список тегов, возвращает список ссылок
def parser_href(list_a_tag):
    list_href = []
    for a in list_a_tag:
        href_long = 'https://ru.wikipedia.org'+a.get('href')
        list_href.append(href_long)
    return list_href

#принимает список html, возвращает список href
def parser_html(list_html):
    list_href = []
    for html in list_html:
        soup = BeautifulSoup(html, 'html.parser')
        list_a_tag = soup.findAll('a', href=re.compile('^/wiki/*'))
        #Создать базу данных и записывать значения
        list_href += parser_href(list_a_tag)       
    return list_href


async def recursion(list_url, steps=1):
    if steps == 0:
        print('Последний шаг сделан')
        print(datetime.now().isoformat())
    else:
        list_html = await series_requests(list_url)
        list_href = parser_html(list_html)
        print(f'Сделан шаг 1, осталось шагов {steps-1}')
        print(datetime.now().isoformat())
        await recursion(list_href, steps-1)


async def run():
    test_list_url = ['http://127.0.0.1:8000/test1/']
    list_url = ['https://ru.wikipedia.org/wiki/Анантнаг_(округ)']
    await create_postgres_tables()
    await recursion(list_url, steps=2)


if __name__ == "__main__":


    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run())
    loop.run_until_complete(future)
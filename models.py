'''
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine 

engine = create_engine('postgresql+psycopg2://exvir::qscft813813@127.0.0.1:5432/webproject')
Base = declarative_base()
qscft813813
class Page(Base):
    __tablename__ = 'pages'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    request_deph = Column(Integer)

    def __init__(self, id, url, request_deph):
        self.id = id
        self.url = url
        self.request_deph = request_deph
    def __repr__(self):
        return f"<Page('{self.id}', '{self.url}', '{self.request_deph}')>"

# Создание сессии
Session = sessionmaker(bind=engine)
#Создание таблицы
Base.metadata.create_all(engine)
#Соединение сессии с подключением
Session.configure(bind=engine)
session = Session()

#Создание страницы
url = 'http://127.0.0.1:8000/'
FirstPage = Page(1, f"{url}", 1)
session.add(FirstPage)
#Внести изменения в БД
session.commit()
'''
from datetime import datetime

import asyncio
from aiopg.sa import create_engine
import sqlalchemy as sa
from aiohttp import ClientSession, ClientTimeout

metadata = sa.MetaData()

page = sa.Table('page', metadata,
               sa.Column('id', sa.Integer, primary_key=True),
               sa.Column('val', sa.String(255)))


async def create_table_page(engine):
    async with engine.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS page')
        await conn.execute('''CREATE TABLE page (
                                  id serial PRIMARY KEY,
                                  val varchar(255))''')


async def insert_to_table_page(url, engine):
    #Нужно для каждой транзакции своё соединение открывать!
    async with engine.acquire() as connect:
        await connect.execute(
            f"INSERT INTO page (val) VALUES ('{url}') RETURNING id"
                            )
        #await connect.execute(page.insert().values(val=f'{url}'))
        #return await connect.execute(page.select(val=f'{url}').id)


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


async def insert_to_table_relation(i, engine):
    # Нужно для каждой транзакции своё соединение открывать!
    async with engine.acquire() as connect:
        await connect.execute(relation.insert().values(id_page=1))


async def request(url, session):
        async with session.get(url) as response:
            #Вызов сопрограммы (генератора)
            html = await response.text()
            print(f'Запрос: {datetime.now().isoformat()}')
            return html


async def go(urls: list):
    async with create_engine(user='exvir',
                             database='webproject',
                             host='127.0.0.1',
                             password='qazwsx') as engine:

        await create_table_page(engine)
        await create_table_relation(engine)

        # tasks = []
        # tasks2 = []
        #
        # for url in urls:
        #     task = asyncio.create_task(insert_to_table_page(url, engine))
        #     task2 = asyncio.create_task(insert_to_table_relation(engine))
        #     tasks.append(task)
        #     tasks2.append(task2)

        tasks = [asyncio.create_task(insert_to_table_page(url, engine)) for url in urls]
        tasks2 = [asyncio.create_task(insert_to_table_page(url, engine)) for url in urls]
        await asyncio.gather(*tasks2)
        await asyncio.gather(*tasks)
        async with engine.acquire() as connect:
            async for row in connect.execute(page.select()):
                print(row.id, row.val)


urls = ['http://127.0.0.1:8000/', 'http://127.0.0.1:8000/test1', 'http://127.0.0.1:8000/test2']
# loop = asyncio.get_event_loop()
# loop.run_until_complete(go(urls))
asyncio.run(go(urls))
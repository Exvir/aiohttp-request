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

import asyncio
from aiopg.sa import create_engine
import sqlalchemy as sa


metadata = sa.MetaData()

tbl = sa.Table('tbl', metadata,
               sa.Column('id', sa.Integer, primary_key=True),
               sa.Column('val', sa.String(255)))


async def create_table(engine):
    async with engine.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS tbl')
        await conn.execute('''CREATE TABLE tbl (
                                  id serial PRIMARY KEY,
                                  val varchar(255))''')


async def insert_to_table(url, engine):
    #Нужно для каждой транзакции своё соединение открывать!
    async with engine.acquire() as connect:
        await connect.execute(tbl.insert().values(val=f'{url}'))


async def go(list_url):
    tasks = []
    async with create_engine(user='exvir',
                             database='webproject',
                             host='127.0.0.1',
                             password='qscft813813') as engine:

        await create_table(engine)
        for url in list_url:
            task = asyncio.create_task(insert_to_table(url, engine))
            tasks.append(task)
        await asyncio.gather(*tasks)
        async with engine.acquire() as connect:
            async for row in connect.execute(tbl.select()):
                print(row.id, row.val)


list_url = ['http://127.0.0.1:8000/', 'http://127.0.0.1:8000/test1', 'http://127.0.0.1:8000/test2']
loop = asyncio.get_event_loop()
loop.run_until_complete(go(list_url))
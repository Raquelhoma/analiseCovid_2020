import os
from os import path
from sqlite3 import dbapi2 as sqlite

file_path = path.join(path.dirname(path.abspath(__file__)), 'dataset_covid.csv')
db_file = path.join(path.dirname(path.abspath(__file__)), 'covid_db.db')

def criarBD():
    try:
        cx = sqlite.connect(db_file)
        cursor = cx.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS dadoscovid2020 (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            regiao VARCHAR(100),
                            estado varchar(2),
                            data date,
                            casos_novos int,
                            casos_acumulados int,
                            obitos_novos int,
                            obitos_acumulados int);''')
    except err:
        print('Erro de banco de dados', err)        
    finally:       
        cursor.close()
        cx.close()

def preencherDados():
    try:
        with open(file_path) as file:
            cx = sqlite.connect(db_file)
            cursor = cx.cursor()
            listaDataset = file.readlines()
            for data in listaDataset:
                cols = data.split(';')
                cursor.execute('''INSERT INTO dadoscovid2020 (
                                        regiao,estado,data,casos_novos,casos_acumulados,obitos_novos,obitos_acumulados)
                                        values (?,?,?,?,?,?,?);''', cols)    
                cx.commit()
    except err:
        print('Erro de banco de dados', err)        
    finally:       
        cursor.close()
        cx.close()

def casosConfirmados():
    try:
        with open(db_file) as file:
            cx = sqlite.connect(db_file)
            cursor = cx.cursor()
            result = cursor.execute('''SELECT estado, sum(casos_novos) as total from dadoscovid2020 group by estado;''' )
            for line in result:
                print(line)
    except err:
        print('Erro de banco de dados', err)        
    finally:       
        cursor.close()
        cx.close()

def mortesporEstado():
    try:
        with open(db_file) as file:
            cx = sqlite.connect(db_file)
            cursor = cx.cursor()
            result = cursor.execute('''SELECT estado, sum(obitos_novos) as total from dadoscovid2020 group by estado;''' )
            for line in result:
                print(line)
    except err:
        print('Erro de banco de dados', err)        
    finally:       
        cursor.close()
        cx.close()

def mortesTotal():
    try:
        with open(db_file) as file:
            cx = sqlite.connect(db_file)
            cursor = cx.cursor()
            result = cursor.execute('''SELECT  sum(obitos_novos) as total from dadoscovid2020;''' )
            for line in result:
                print(line[0])
    except err:
        print('Erro de banco de dados', err)        
    finally:       
        cursor.close()
        cx.close()

def casosConfirmadosTotal():
    try:
        with open(db_file) as file:
            cx = sqlite.connect(db_file)
            cursor = cx.cursor()
            result = cursor.execute('''SELECT sum(casos_novos) as total from dadoscovid2020;''' )
            for line in result:
                print(line[0])
    except err:
        print('Erro de banco de dados', err)        
    finally:       
        cursor.close()
        cx.close()

# criarBD()
# preencherDados()
print('************************************************')
print('Casos Confirmados por Estado ')
casosConfirmados()
print('\nMortes por Estado ')
mortesporEstado()
print('\nTotal de Casos Confirmados ')
casosConfirmadosTotal()
print('\nTotal de Mortes')
mortesTotal()



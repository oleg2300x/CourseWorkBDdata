import sqlite3

import requests
import json
import psycopg2
import config


def get_hh_data(companies) -> list:
    '''Получение данных компаний с сайта'''
    date = []
    for company in companies:
        url = f'https://api.hh.ru/employers/{company}'
        company_response = requests.get(url).json()
        vacancy_response = requests.get(company_response['vacancies_url']).json()
        date.append({
            'company': company_response,
            'vacancies': vacancy_response['items']
        })
    print('Данные получены')
    return date


def filter_strings(string: str) -> str:
    symbols = ['\n', '<strong>', '\r', '</strong>', '&mdash', '</p>', '<p>', '</li>', '<li>',
               '<b>', '</b>', '<ul>', '<li>', '</li>', '<br />', '</ul>']
    if string is not None:
        for symbol in symbols:
            string = string.replace(symbol, '')
        return string


def filter_salary(salary):
    if salary is not None:
        if salary['from'] is not None and salary['to'] is not None:
            return round((salary['from'] + salary['to']) / 2)
        elif salary['from'] is not None:
            return salary['from']
        elif salary['to'] is not None:
            return salary['to']
    return None


def create_database(database_name: str, params: list):
    '''Сохдание базы данных и таблиц'''
    connection = psycopg2.connect(dbname='postgres', **params)
    connection.autocommit = True

    with connection.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
        cur.execute(f'CREATE DATABASE {database_name}')

    connection.close()
    print('БД создана')

    with psycopg2.connect(dbname=database_name, **params) as connection:
        with connection.cursor() as cur:
            cur.execute('''CREATE TABLE companies (
                company_id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                description TEXT,
                link VARCHAR(255) NOT NULL,
                url_vacancies VARCHAR(255) NOT NULL
                )
            ''')

            cur.execute('''CREATE TABLE vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                vacancy_name VARCHAR(255) NOT NULL,
                salary NUMERIC,
                requirement TEXT,
                responsibilities TEXT,
                link VARCHAR(255) NOT NULL,
                company_id SERIAL REFERENCES companies(company_id) NOT NULL,
                published_date TIMESTAMP NOT NULL
                )
            ''')
    connection.close()
    print('Таблицы созданы')

def fill_tables(data:list, database_name: str, params: dict) -> None:
    with psycopg2.connect(dbname=database_name, **params) as connection:
        with connection.cursor() as cur:
            for company_data in data:
                company = company_data['company']
                vacancies = company_data['vacancies']

                cur.execute('''INSERT INTO companies (company_name, description, link, url_vacancies) 
                                VALUES (%s, %s, %s, %s) 
                                RETURNING company_id''',
                            (company['name'], filter_strings(company['description']), company['site_url'], company['vacancies_url']))
                company_id = cur.fetchone()[0]

                for vacancy in vacancies:
                    cur.execute('''INSERT INTO vacancies (vacancy_name, salary, requirement, responsibilities, link, company_id, published_date) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                                (vacancy['name'], filter_salary(vacancy['salary']),
                                 filter_strings(vacancy['snippet']['requirement']),
                                 filter_strings(vacancy['snippet']['responsibility']),
                                 vacancy['alternate_url'], company_id, vacancy['published_at']))

    connection.commit()
    connection.close()

    print('Данные добавлены в таблицы')


def close_all_database_connections():
    conn = sqlite3.connect('sqlite_master.db')
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='database';")
    databases = cursor.fetchall()

    for database in databases:
        try:
            current_conn = sqlite3.connect(database[0])
            current_conn.close()
            print(f"Соединение с базой данных {database[0]} успешно закрыто.")
        except Exception as e:
            print(f"Ошибка при закрытии соединения с базой данных {database[0]}: {str(e)}")

    conn.close()
    print("Все соединения с базами данных успешно закрыты.")
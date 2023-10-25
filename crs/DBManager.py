import psycopg2

from config import config


class DBManager:
    def __init__(self, bd_name, params=config()):
        self.bd_name = bd_name
        self.params = params

    def get_companies_and_vacancies_count(self):
        try:
            conn = psycopg2.connect(database=self.bd_name, **self.params)
            print('Успешное подключение в БД')
            with conn.cursor() as cur:
                cur.execute('SELECT company_name, COUNT(vacancy_id)\n'
                            'FROM companies\n'
                            'JOIN vacancies USING(company_id)\n'
                            'GROUP BY company_name')

                data = cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            return f'[INFO] {error}'
        conn.close()
        return data

    @property
    def get_all_vacancies(self):
        try:
            conn = psycopg2.connect(database=self.bd_name, **self.params)
            print('Успешное подключение в БД')
            with conn.cursor() as cur:
                cur.execute('SELECT company_name, vacancy_name, salary, vacancies.link\n'
                            'FROM vacancies\n'
                            'JOIN companies USING(company_id)')

                data = cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            return f'[INFO] {error}'
        conn.close()
        return data

    def get_avg_salary(self):
        try:
            conn = psycopg2.connect(database=self.bd_name, **self.params)
            print('Успешное подключение в БД')
            with conn.cursor() as cur:
                cur.execute('SELECT company_name, round(AVG(salary)) AS avg_selary\n'
                            'FROM companies\n'
                            'JOIN vacancies USING(company_id)\n'
                            'GROUP BY company_name')

                data = cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            return f'[INFO] {error}'
        conn.close()
        return data

    def get_vacancies_with_higher_salary(selary):
        try:
            conn = psycopg2.connect(database=self.bd_name, **self.params)
            print('Успешное подключение в БД')
            with conn.cursor() as cur:
                cur.execute(' SELECT * FROM vacancies\n'
                            ' WHERE salary > (SELECT AVG(salary) FROM vacancies)\n'
                            ' ORDER BY salary DESC')

                data = cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            return f'[INFO] {error}'
        conn.close()
        return data

    def get_vacancies_with_keyword(self, keyword):
        try:
            conn = psycopg2.connect(database=self.bd_name, **self.params)
            print('Успешное подключение в БД')
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM vacancies WHERE lower(vacancy_name) LIKE '%{keyword}%' ")
                data = cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            return f'[INFO] {error}'
        conn.close()
        return data

import psycopg2
import requests


class FullerBase:

    def __init__(self, password):
        self.password = password
        self.base_url = f'https://api.hh.ru/vacancies?employer_id='
        self.list_id_companies = ['3529', '1740', '78638', '2526975', '2180', '15478',
                                  '8940054', '1942330', '2748', '87021']
        self.con = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                                    host="127.0.0.1")

    @staticmethod
    def date_company(id):
        url_vacancies = f'https://api.hh.ru/employers/' + id

        response = requests.get(url_vacancies)
        all_data = response.json()

        id_company = all_data['id']
        name_company = all_data['name']
        vacancies_count = all_data['open_vacancies']

        return id_company, name_company, vacancies_count

    def get_pages(self, id):
        vacancies_link = self.base_url + id
        response = requests.get(vacancies_link, params={'per_page': 100})
        all_data_vacancies = response.json()

        all_pages = all_data_vacancies['pages']
        return all_pages

    def get_vacancies_on_page(self, id, page):
        page_url_vacancy = self.base_url + id
        response = requests.get(page_url_vacancy, params={'page': page, 'per_page': 100})
        all_data_on_page = response.json()
        return all_data_on_page

    @staticmethod
    def get_data_vacancies(vacancy):
        id_vacancy = int(vacancy['id'])
        id_company = int(vacancy['employer']['id'])
        link_vacancy = vacancy['alternate_url']
        name_vacancy = vacancy['name']
        if vacancy['salary']:
            data_salary = vacancy['salary']
            if data_salary['to']:
                salary = int(data_salary['to'])
            elif data_salary['from']:
                salary = int(data_salary['from'])
        else:
            salary = None
        return id_company, name_vacancy, link_vacancy, salary, id_vacancy

    def list_data_vacancies(self, id):
        id_vacancies = []
        list_vacancies = []

        all_data_on_page = self.get_vacancies_on_page(id, 1)

        for one_vacancy in all_data_on_page['items']:
            id_vacancy = one_vacancy['id']

            if id_vacancy in id_vacancies:
                continue

            id_vacancies.append(id_vacancy)
            data_vacancy = self.get_data_vacancies(one_vacancy)
            if data_vacancy is not None:
                list_vacancies.append(data_vacancy)
        return list_vacancies

    def create_tables(self):

        con = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                               host="127.0.0.1")
        try:
            with con:
                with con.cursor() as cursor:
                    cursor.execute("""CREATE TABLE IF NOT EXISTS Companies (
                                    id integer PRIMARY KEY,
                                    name TEXT NOT NULL,
                                    count_vacancy INTEGER)""")
                    cursor.execute("""CREATE TABLE IF NOT EXISTS Vacancies (
                                   id_company INTEGER,
                                   name_vacancy TEXT NOT NULL,
                                   link_vacancy TEXT,
                                   salary integer,
                                   id_vacancy integer PRIMARY KEY,
                                   FOREIGN KEY (id_company) REFERENCES Companies (id)
                                   )""")
        finally:
            con.close()

    def delete_tables(self):
        connection = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                                      host="127.0.0.1")
        with connection:
            with connection.cursor() as curs:
                curs.execute("TRUNCATE TABLE Companies CASCADE")
                curs.execute("TRUNCATE TABLE Vacancies")
        print('Произведено удаление  Companies, Vacancies')

    def loading_data(self):
        conn = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                                host="127.0.0.1")
        try:
            with conn:
                with conn.cursor() as cur:

                    for company in self.list_id_companies:
                        cur.execute(f"""INSERT INTO Companies (id, name, count_vacancy)
                                    VALUES {self.date_company(company)};""")

                    for company in self.list_id_companies:
                        for vacancy in self.list_data_vacancies(company):
                            cur.execute(f"""INSERT INTO Vacancies 
                            (id_company, name_vacancy, link_vacancy, salary, id_vacancy)
                            VALUES (%s, %s, %s, %s, %s)""", vacancy)
        finally:
            conn.close()

    def run(self):
        self.delete_tables()
        self.create_tables()
        print('Произведено создание таблиц')
        self.loading_data()
        print('Произведена загрузка данных')

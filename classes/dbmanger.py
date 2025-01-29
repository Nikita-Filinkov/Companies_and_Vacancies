import psycopg2


class DBManager:

    def __init__(self, password):
        self.password = password

    def get_companies_and_vacancies_count(self):
        connection = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                                      host='127.0.0.1')
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT name, count_vacancy FROM Companies")
                    result = cursor.fetchall()
                    return result
        finally:
            connection.close()

    def get_all_vacancies(self):
        con = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                               host='127.0.0.1')
        try:
            with con:
                with con.cursor() as cur:
                    cur.execute("""SELECT Companies.name, 
                                    Vacancies.name_vacancy, 
                                    Vacancies.salary, 
                                    Vacancies.link_vacancy 
                                    FROM Companies
                                    JOIN Vacancies 
                                    ON Companies.id = Vacancies.id_company 
                                    LIMIT 10""")
                    return cur.fetchall()
        finally:
            con.close()

    def get_avg_salary(self):
        con = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                               host='127.0.0.1')
        try:
            with con:
                with con.cursor() as cur:
                    cur.execute("""SELECT Companies.name,  
                                    ROUND(AVG(Vacancies.salary), 2)
                                    FROM Companies
                                    JOIN Vacancies 
                                    ON Companies.id = Vacancies.id_company
                                    GROUP BY Companies.name 
                                    LIMIT 10""")
                    return cur.fetchall()
        finally:
            con.close()

    def get_vacancies_with_higher_salary(self):
        con = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                               host='127.0.0.1')
        try:
            with con:
                with con.cursor() as cur:
                    cur.execute("""SELECT Vacancies.name_vacancy, Vacancies.salary FROM Vacancies
                                    WHERE Vacancies.salary > (SELECT AVG(Vacancies.salary) FROM Vacancies
							                                  WHERE Vacancies.salary IS NOT NULL)
							        ORDER BY Vacancies.salary DESC
							        LIMIT 10""")
                    return cur.fetchall()
        finally:
            con.close()

    def get_vacancies_with_keyword(self, word):

        con = psycopg2.connect(database="Companies_and_Vacancies", user="postgres", password=self.password,
                               host='127.0.0.1')
        try:
            with con:
                with con.cursor() as cur:
                    cur.execute(f"""SELECT Companies.name, Vacancies.name_vacancy, Vacancies.salary
                                            FROM Companies
                                            JOIN Vacancies 
                                            ON Companies.id = Vacancies.id_company
                                            WHERE Vacancies.name_vacancy LIKE '%{word}%' 

                                            LIMIT 100""")
                    return cur.fetchall()
        finally:
            con.close()

from decouple import config
from classes.loads import FullerBase
from classes.dbmanger import DBManager

password = config('password')
fuller = FullerBase(password)
dbmanager = DBManager(password)
fuller.run()

while True:
    print('ИНТЕРФЕЙС')
    print('Доступны следующие функции:')
    print('1. Получает список всех компаний и количество вакансий у каждой компании')
    print('2. Получает список всех вакансий с указанием названия компании, названия вакансии '
          'и зарплаты и ссылки на вакансию')
    print('3. Получает среднюю зарплату по вакансиям')
    print('4. Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям')
    print('5. Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python')

    num_fun = input("Введите номер функции для ее запуска и 0 для завершения: ")

    if num_fun == '0':
        break
    if num_fun not in ('1', '2', '3', '4', '5'):
        print('Такой функции нет')
    if num_fun == '1':
        data = dbmanager.get_companies_and_vacancies_count()
        print(data)
        for value in data:
            formatted_results = (" | ".join(map(str, value)))
            print(formatted_results)
    elif num_fun == '2':
        data = dbmanager.get_all_vacancies()
        for value in data:
            formatted_results = (" | ".join(map(str, value)))
            print(formatted_results)
    elif num_fun == '3':
        data = dbmanager.get_avg_salary()
        for value in data:
            formatted_results = (" | ".join(map(str, value)))
            print(formatted_results)
    elif num_fun == '4':
        data = dbmanager.get_vacancies_with_higher_salary()
        for value in data:
            formatted_results = (" | ".join(map(str, value)))
            print(formatted_results)
    elif num_fun == '5':
        word = input('Введите слово для поиска вакансии: ')
        data = dbmanager.get_vacancies_with_keyword(word=word)
        for value in data:
            formatted_results = (" | ".join(map(str, value)))
            print(formatted_results)

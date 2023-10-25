from crs.utils import get_hh_data, create_database, fill_tables, close_all_database_connections
from config import config
from DBManager import DBManager


def main():
    companies = [78638,  # Тинькофф
                 84585,  # Авито
                 3529,  # Сбер
                 633069,  # Selectel
                 1740,  # Яндекс
                 1375441,  # Okko
                 1272486,  # Сбермаркет
                 2324020,  # Точка
                 1122462,  # Skyeng
                 15478  # VK
                 ]

    bd_name = 'hh_ru'
    # data = get_hh_data(companies)
    # close_all_database_connections()
    params = config()
    # create_database(bd_name, params)
    # fill_tables(data, bd_name, params)
    hh_class = DBManager(bd_name, params)
    print(hh_class.get_vacancies_with_keyword('аналитик'))
if __name__ == '__main__':
    main()
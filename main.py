import argparse
import hashlib
import json
import logging
import math
import random
import re
import datetime
import time
import requests
import sqlalchemy as sa

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)

from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from db.connection import SessionLocal
from utils import requests_retry_session
from crud import DebtorCrud
from db.models import Debtor




class MainParser:
    def __init__(self, debt_type: int, db_session: Session):
        self.url = 'https://tazalau.qoldau.kz/ru/list/'
        self.debt_type = debt_type
        self.start_time = str(datetime.datetime.now())
        self.upserted_count = 0
        self.total_rows = 0
        self.total_pages = 1
        self.total_deleted = 0
        self.unique_rows = set()

        self.session = requests_retry_session()
        self.params = {}
        self.crud = DebtorCrud(db_session)

        self.labels = {}

        self.set_logging()
        logging.info("*" * 30)
        logging.info("FL Debtors Parser")
        logging.info(f"type_id: {self.debt_type}, start time: {self.start_time}")
        logging.info("*" * 30)

    def shadow_prepare_tables(self):
        engine = self.crud.session.get_bind()
        conn = engine.connect()
        try:
            logging.info(f"Удаляем таблицу {self.shadow_table_name} если она существует.")
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {self.shadow_table_name} CASCADE"))

            logging.info(f"Создаем таблицу {self.shadow_table_name}.")
            conn.execute(sa.text(f"CREATE TABLE {self.shadow_table_name} (LIKE {self.base_table_name} INCLUDING ALL)"))
            logging.info(f"Таблица {self.shadow_table_name} успешно создана.")
        except Exception as e:
            logging.error(f"Ошибка при подготовке shadow таблицы: {e}")
            raise
        finally:
            conn.close()

    def shadow_swap_tables(self):
        engine = self.crud.session.get_bind()
        conn = engine.connect()
        transaction = conn.begin()
        try:
            result = conn.execute(sa.text("SELECT to_regclass('public.debtor')"))
            if result.fetchone()[0] is None:
                raise ValueError("Таблица debtor не существует.")
            logging.info("Таблица debtor существует.")

            result = conn.execute(sa.text("SELECT to_regclass('public.debtor_shadow')"))
            if result.fetchone()[0] is None:
                raise ValueError(f"Таблица {self.shadow_table_name} не существует.")
            logging.info(f"Таблица {self.shadow_table_name} существует.")

            url = f'{self.url}{self.debt_type}?p=1'
            data = self.parse_page(1)

            if not data:
                logging.warning("Нет данных для записи в таблицу.")
                return

            columns = ', '.join(self.labels.values())
            values_placeholder = ', '.join(['%s'] * len(self.labels))

            logging.info(f"Записываем данные в таблицу {self.shadow_table_name}.")
            for debtor in data:
                values = [debtor[label] for label in self.labels.values()]
                conn.execute(
                    sa.text(f"INSERT INTO {self.shadow_table_name} ({columns}) VALUES ({values_placeholder})"),
                    values
                )
            logging.info(f"Данные успешно записаны в {self.shadow_table_name}.")

            logging.info(f"Переименование таблицы {self.base_table_name} в {self.base_table_name}_backup.")
            conn.execute(sa.text(f"ALTER TABLE {self.base_table_name} RENAME TO {self.base_table_name}_backup"))
            logging.info(f"Таблица {self.base_table_name} переименована в {self.base_table_name}_backup.")

            logging.info(f"Переименование таблицы {self.shadow_table_name} в {self.base_table_name}.")
            conn.execute(sa.text(f"ALTER TABLE {self.shadow_table_name} RENAME TO {self.base_table_name}"))
            logging.info(f"Таблица {self.shadow_table_name} переименована в {self.base_table_name}.")

            # Drop backup table
            logging.info(f"Удаляем таблицу {self.base_table_name}_backup если она существует.")
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {self.base_table_name}_backup"))
            logging.info(f"Таблица {self.base_table_name}_backup удалена.")

            logging.info(f"Таблицы успешно переименованы: `{self.shadow_table_name}` теперь `{self.base_table_name}`.")

            transaction.commit()
        except Exception as e:
            logging.error(f"Ошибка при переименовании таблиц: {e}")
            transaction.rollback()
            raise
        finally:
            conn.close()

    def shadow_migration(self):
        logging.info("Shadow Migration Start")
        self.shadow_prepare_tables()
        self.start()
        self.shadow_swap_tables()
        logging.info("Shadow Migration Completed")

    @property
    def base_table_name(self):
        return Debtor.__tablename__

    @property
    def shadow_table_name(self):
        return self.base_table_name + "_shadow"
    def set_logging(self):
        pass

    def parse_page(self, page_num: int):
        self.params['p'] = page_num
        self.params['limit'] = 100
        try:
            r = self.session.get(url=self.url, params=self.params)
            r.raise_for_status()                     # Fail-fast on request error
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from {self.url}: {e}")
            raise SystemExit(1)                      # Stop execution immediately

        soup = BeautifulSoup(r.text, 'lxml')

        thead = soup.find('thead')
        self.load_labels(thead)

        page_content = []
        tbody = soup.find('tbody')
        trs = tbody.find_all('tr')
        if len(trs) == 1 and trs[0].text == 'Нет записей':
            self.total_pages = page_num
            return page_content

        for tr in trs:
            debtor = self.get_debtor(tr)
            debtor['hash_value'] = self.get_hash(debtor)
            self.unique_rows.add(debtor['hash_value'])
            page_content.append(debtor)

        return page_content

    def load_labels(self, thead):
        pass

    def get_debtor(self, tr) -> dict:
        pass

    def save_total_rows(self):
        r = self.session.get(url=self.url, params=self.params)
        page_text = r.text
        page_text = page_text.split('Всего записей</small> ')[1].split('<', 1)[0]
        page_text = re.sub(r'\D', '', page_text)
        self.total_rows = int(page_text)
        self.total_pages = math.ceil(self.total_rows / 15)

    def get_hash(self, debtor):
        json_str = json.dumps(debtor, sort_keys=True, default=str)
        hash_object = hashlib.sha256()
        hash_object.update(json_str.encode())
        hash_value = hash_object.hexdigest()
        return hash_value

    def send_to_upsert(self, debtors):
        try:
            self.crud.bulk_upsert(debtors)
            self.upserted_count += len(debtors)
            logging.info(f'##### Current State: {self.state}')
        except Exception as e:
            logging.error(f"Error during upsert operation: {e}")
            raise SystemExit(1)  # Fail-fast if database operation fails

    def delete_old(self):
        try:
            logging.info(f"##### Deleting rows older than {self.start_time} with type_id {self.debt_type}")  #deleting
            self.total_deleted = self.crud.delete_all(to_date=self.start_time, type_id=self.debt_type)
            logging.info(f"##### Cleanup completed! Deleted {self.total_deleted} rows")
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")
            raise SystemExit(1)


    def start(self):
        page_num = 1
        self.save_total_rows()
        while page_num <= self.total_pages:
            debtors = self.parse_page(page_num=page_num)
            if not debtors:
                break
            self.send_to_upsert(debtors)
            page_num += 1
            time.sleep(random.randint(1, 2))
        self.delete_old()

    @property
    def state(self):
        return {
            'total_rows': self.total_rows,
            'total_pages': self.total_pages,
            'upserted': self.upserted_count,
            'deleted': self.total_deleted,
            'unique_rows': len(self.unique_rows)
        }

    def close(self):
        self.session.close()
        self.crud.close()
        logging.info("*" * 30)
        logging.info(f'Finished with state: {self.state}')
        logging.info("*" * 30)


class Type1Parser(MainParser):
    def __init__(self, db_session: Session = SessionLocal()):
        super().__init__(debt_type=1, db_session=db_session)
        self.url += 'debtor'
        self.params = {
            'p': 1,
        }

    def load_labels(self, thead):
        ths = thead.find_all('th')
        for i, th in enumerate(ths):
            label = th.text
            if label == 'ИИН заявителя':
                self.labels[i] = 'identifier'
            if label == 'ФИО заявителя':
                self.labels[i] = 'fio'
            if label == 'Услугодатель':
                self.labels[i] = 'provider'
            if label == 'Входящий номер заявления':
                self.labels[i] = 'app_num'
            if label == 'Дата подачи заявления от фронт системы на применение процедуры':
                self.labels[i] = 'app_date'
            if label == 'Дата начала процедуры внесудебного банкротства':
                self.labels[i] = 'procedure_start_date'
            if label == 'Статус заявителя':
                self.labels[i] = 'status'
            if label == 'Список кредиторов' or label == 'Список кредиторов, данные ПКБ/ГКБ':
                self.labels[i] = 'creditors_list'
            if label == 'Cумма задолженности, указанная заявителем, тг.' or label == 'Cумма задолженности, данные ПКБ/ГКБ, тг.':
                self.labels[i] = 'debt_sum'

    def get_debtor(self, tr):
        tds = tr.find_all('td')
        debtor_data = {"debt_type_id": self.debt_type}
        for i, label in self.labels.items():
            debtor_data[label] = tds[i].text if len(tds) > i else None

        if not debtor_data.get('identifier'):    #none
            debtor_data['identifier'] = None

        if isinstance(debtor_data['debt_sum'], str):
            debtor_data['debt_sum'] = re.sub(r'\D', '', debtor_data['debt_sum'])

        date_format = "%d.%m.%Y %H:%M:%S"
        debtor_data['app_date'] = datetime.datetime.strptime(debtor_data['app_date'], date_format)
        debtor_data['procedure_start_date'] = datetime.datetime.strptime(debtor_data['procedure_start_date'], date_format)

        debtor_data['creditors_list'] = re.sub(r'(\d+\))', r'\n\1', debtor_data['creditors_list']).lstrip("\n")
        return debtor_data


class Type2Parser(MainParser):
    def __init__(self, db_session: Session = SessionLocal()):
        super().__init__(debt_type=2, db_session=db_session)
        self.url += 'debtor/judicial'
        self.params = {
            'p': 1,
        }

    def load_labels(self, thead):
        ths = thead.find_all('th')
        for i, th in enumerate(ths):
            label = th.text.strip()
            if label == 'ИИН':
                self.labels[i] = 'identifier'
            if label == 'ФИО':
                self.labels[i] = 'fio'
            if label == 'Категория дела':
                self.labels[i] = 'category'
            if label == 'Дата Иска':
                self.labels[i] = 'app_date'
            if label == 'Дата принятия к производству':
                self.labels[i] = 'procedure_start_date'
            if label == 'Дата решения суда':
                self.labels[i] = 'decision_date'
            if label == 'Дата вступления в силу решения суда':
                self.labels[i] = 'decision_start_date'
            if label == 'Дата завершения дела':
                self.labels[i] = 'procedure_end_date'
            if label == 'Наименование суда':
                self.labels[i] = 'provider'
            if label == 'Регион':
                self.labels[i] = 'region'
            if label == 'Статус дела':
                self.labels[i] = 'status'

    def get_debtor(self, tr):
        tds = tr.find_all('td')
        debtor_data = {"debt_type_id": self.debt_type}
        for i, label in self.labels.items():
            debtor_data[label] = tds[i].text

        date_format = "%d.%m.%Y"
        for k, v in debtor_data.items():
            if '_date' in k:
                debtor_data[k] = datetime.datetime.strptime(debtor_data[k], date_format) if v else None

        return debtor_data


class Type3Parser(MainParser):
    def __init__(self, db_session: Session = SessionLocal()):
        super().__init__(debt_type=3, db_session=db_session)
        self.url += 'debtor/extrajudicial/terminated-and-cancelled'
        self.params = {
            'p': 1,
        }

    def load_labels(self, thead):
        ths = thead.find_all('th')
        for i, th in enumerate(ths):
            label = th.text
            if label == 'ИИН заявителя':
                self.labels[i] = 'identifier'
            if label == 'ФИО заявителя':
                self.labels[i] = 'fio'
            if label == 'Услугодатель':
                self.labels[i] = 'provider'
            if label == 'Входящий номер заявления':
                self.labels[i] = 'app_num'
            if label == 'Дата подачи заявления от фронт системы на применение процедуры':
                self.labels[i] = 'app_date'
            if label == 'Дата начала процедуры внесудебного банкротства':
                self.labels[i] = 'procedure_start_date'
            if label == 'Инициатор процедуры':
                self.labels[i] = 'stop_initiator'
            if label == 'Дата прекращения процедуры внесудебного банкротства':
                self.labels[i] = 'procedure_end_date'
            if label == 'Входящий номер заявления на прекращение':
                self.labels[i] = 'procedure_stop_num'
            if label == 'Список кредиторов':
                self.labels[i] = 'creditors_list'
            if label == 'Cумма задолженности, указанная заявителем, тг.':
                self.labels[i] = 'debt_sum'

    def get_debtor(self, tr):
        tds = tr.find_all('td')
        debtor_data = {"debt_type_id": self.debt_type}
        for i, label in self.labels.items():
            debtor_data[label] = tds[i].text

        if isinstance(debtor_data['debt_sum'], str):
            debtor_data['debt_sum'] = re.sub(r'\D', '', debtor_data['debt_sum'])

        date_format = "%d.%m.%Y %H:%M:%S"
        debtor_data['app_date'] = datetime.datetime.strptime(debtor_data['app_date'], date_format)
        debtor_data['procedure_end_date'] = datetime.datetime.strptime(debtor_data['procedure_end_date'], date_format)
        debtor_data['procedure_start_date'] = datetime.datetime.strptime(debtor_data['procedure_start_date'], date_format)

        debtor_data['creditors_list'] = re.sub(r'(\d+\))', r'\n\1', debtor_data['creditors_list']).lstrip("\n")
        return debtor_data


class Type4Parser(MainParser):
    def __init__(self, db_session: Session = SessionLocal()):
        super().__init__(debt_type=4, db_session=db_session)
        self.url += 'bankruptcy-and-insolvent'
        self.params = {
            'p': 1,
        }

    def load_labels(self, thead):
        ths = thead.find_all('th')
        for i, th in enumerate(ths):
            label = th.text
            if label == 'ИИН заявителя':
                self.labels[i] = 'identifier'
            if label == 'ФИО заявителя':
                self.labels[i] = 'fio'
            if label == 'Услугодатель':
                self.labels[i] = 'provider'
            if label == 'Входящий номер заявления':
                self.labels[i] = 'app_num'
            if label == 'Дата подачи заявления от фронт системы на применение процедуры':
                self.labels[i] = 'app_date'
            if label == 'Дата начала процедуры внесудебного банкротства':
                self.labels[i] = 'procedure_start_date'
            if label == 'Дата завершения процедуры внесудебного банкротства':
                self.labels[i] = 'procedure_end_date'
            if label == 'Статус заявителя':
                self.labels[i] = 'status'
            if label == 'Список кредиторов':
                self.labels[i] = 'creditors_list'
            if label == 'Cумма задолженности, указанная заявителем, тг.':
                self.labels[i] = 'debt_sum'

    def get_debtor(self, tr):
        tds = tr.find_all('td')
        debtor_data = {"debt_type_id": self.debt_type}
        for i, label in self.labels.items():
            debtor_data[label] = tds[i].text

        if isinstance(debtor_data['debt_sum'], str):
            debtor_data['debt_sum'] = re.sub(r'\D', '', debtor_data['debt_sum'])

        date_format = "%d.%m.%Y %H:%M:%S"
        debtor_data['app_date'] = datetime.datetime.strptime(debtor_data['app_date'], date_format)
        debtor_data['procedure_end_date'] = datetime.datetime.strptime(debtor_data['procedure_end_date'], date_format)
        debtor_data['procedure_start_date'] = datetime.datetime.strptime(debtor_data['procedure_start_date'], date_format)

        debtor_data['creditors_list'] = re.sub(r'(\d+\))', r'\n\1', debtor_data['creditors_list']).lstrip("\n")
        return debtor_data


class Type5Parser(MainParser):
    def __init__(self, db_session: Session = SessionLocal()):
        super().__init__(debt_type=5, db_session=db_session)
        self.url += 'bankruptcy/judicial'
        self.params = {
            'p': 1,
        }

    def load_labels(self, thead):
        ths = thead.find_all('th')
        for i, th in enumerate(ths):
            label = th.text.strip()
            if label == 'ИИН':
                self.labels[i] = 'identifier'
            if label == 'ФИО':
                self.labels[i] = 'fio'
            if label == 'Категория дела':
                self.labels[i] = 'category'
            if label == 'Дата Иска':
                self.labels[i] = 'app_date'
            if label == 'Дата принятия к производству':
                self.labels[i] = 'procedure_start_date'
            if label == 'Дата решения суда':
                self.labels[i] = 'decision_date'
            if label == 'Дата вступления в силу решения суда':
                self.labels[i] = 'decision_start_date'
            if label == 'Дата завершения дела':
                self.labels[i] = 'procedure_end_date'
            if label == 'Наименование суда':
                self.labels[i] = 'provider'
            if label == 'Регион':
                self.labels[i] = 'region'
            if label == 'Статус дела':
                self.labels[i] = 'status'

    def get_debtor(self, tr):
        tds = tr.find_all('td')
        debtor_data = {"debt_type_id": self.debt_type}
        for i, label in self.labels.items():
            debtor_data[label] = tds[i].text

        date_format = "%d.%m.%Y"
        for k, v in debtor_data.items():
            if '_date' in k:
                debtor_data[k] = datetime.datetime.strptime(debtor_data[k], date_format) if v else None

        return debtor_data


class Type6Parser(MainParser):
    def __init__(self, db_session: Session = SessionLocal()):
        super().__init__(debt_type=6, db_session=db_session)
        self.url += 'bankruptcy/recovery'
        self.params = {
            'p': 1,
        }

    def load_labels(self, thead):
        ths = thead.find_all('th')
        for i, th in enumerate(ths):
            label = th.text.strip()
            if label == 'ИИН':
                self.labels[i] = 'identifier'
            if label == 'ФИО':
                self.labels[i] = 'fio'
            if label == 'Категория дела':
                self.labels[i] = 'category'
            if label == 'Дата Иска':
                self.labels[i] = 'app_date'
            if label == 'Дата принятия к производству':
                self.labels[i] = 'procedure_start_date'
            if label == 'Дата решения суда':
                self.labels[i] = 'decision_date'
            if label == 'Дата вступления в силу решения суда':
                self.labels[i] = 'decision_start_date'
            if label == 'Дата завершения дела':
                self.labels[i] = 'procedure_end_date'
            if label == 'Наименование суда':
                self.labels[i] = 'provider'
            if label == 'Регион':
                self.labels[i] = 'region'
            if label == 'Статус дела':
                self.labels[i] = 'status'

    def get_debtor(self, tr):
        tds = tr.find_all('td')
        debtor_data = {"debt_type_id": self.debt_type}
        for i, label in self.labels.items():
            debtor_data[label] = tds[i].text

        date_format = "%d.%m.%Y"
        for k, v in debtor_data.items():
            if '_date' in k:
                debtor_data[k] = datetime.datetime.strptime(debtor_data[k], date_format) if v else None

        return debtor_data


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(
    #     description="Parser for fl debtor",
    #     formatter_class=argparse.RawTextHelpFormatter,
    # )
    # parser.add_argument(
    #     "-t", "--type_id",
    #     required=True,
    # )
    # args = parser.parse_args()
    parsers = [
        Type1Parser,
        Type2Parser,
        Type3Parser,
        Type4Parser,
        Type5Parser,
        Type6Parser,
    ]
    try:
        parser_type = 1
        p = parsers[parser_type - 1]()
        p.shadow_migration()
    except Exception as e:
        logging.exception(e)
"""
будем читать локальную базу данных и отправлять чек из нее в сервер 1С
а потом чистить базу
"""
import os
from dotenv import load_dotenv
from typing import Tuple, List
import requests
import logging
import datetime
import barcode

current_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H_%M_%S')
logging.basicConfig(
    filename='d:\\files\\' + os.path.basename(__file__) + '_.log',
    filemode='a',
    level=logging.DEBUG,
    format="%(asctime)s - %(filename)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S')

logger_sender: logging.Logger = logging.getLogger(__name__)
logger_sender.setLevel(logging.DEBUG)
logger_sender.debug('start')

env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)

try:
    from shtrih.receipt_db import Receiptinsql
except Exception as exc:
    print(exc)


def make_list_dict_items(tuple_items: Tuple) -> List:
    list_items = []
    logger_sender.debug('формируем состав чека')
    for elem in tuple_items:
        item = {
            'nn': elem[1],
            'barcode': elem[2],
            'name': elem[3],
            'quantity': elem[4],
            'price': elem[5],
            'seller': elem[6],
            'comment': elem[7],
        }
        list_items.append(item)
    logger_sender.debug('конец формируем состав чека')
    return list_items


def make_list_dict_rec(tuple_rec: Tuple, list_rec: List, receipt_db: Receiptinsql) -> List:
    """
    метод наполнения списка чеков
    :param tuple_rec: кортеж данных чека из БД
    :param list_rec: выходной список всех чеков
    :return:
    """
    logger_sender.debug('начало формирования чека')
    if list_rec is None:
        list_rec = []
    rec_items = receipt_db.get_items(tuple_rec[0])
    items = make_list_dict_items(rec_items)
    if str(tuple_rec[6]) != 'XЧЛ':
        inn = str(barcode.get('ean13', str(tuple_rec[6])))
    else:
        inn = 'XЧЛ'
    rec = {
        'id': tuple_rec[0],
        'number_receipt': tuple_rec[1],
        'date_create': tuple_rec[2],
        'shop_id': tuple_rec[3],
        'items': items,
        'sum': tuple_rec[4],
        'SumBeforeSale': tuple_rec[5],
        'clientID': inn,
        'inn_pman': inn,
        'phone': tuple_rec[8],
        'bonus_add': tuple_rec[9],
        'bonus_dec': tuple_rec[10],
        'bonus_begin': tuple_rec[11],
        'bonus_end': tuple_rec[12],
        'operation_type': tuple_rec[13],
    }
    logger_sender.debug('конец формирования чека')
    list_rec.append(rec)
    return list_rec


def get_receipts(rec_db: Receiptinsql) -> List:
    """
    получаем список чеков
    :return:
    """

    rec_list = rec_db.get_receipt()
    list_receipts = []
    for elem in rec_list:
        list_receipts = make_list_dict_rec(elem, list_receipts, rec_db)
    logging.debug('получили список чеков {}'.format(list_receipts))
    return list_receipts


def send_receipt_to_1C(list_receipt: List) -> List:
    """
    функция отправки чеков в 1С
    :param list_receipt: список с чеками
    :return:
    """
    list_good_sended = []
    url = os.getenv('server_1C_add_receipt')
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + os.getenv('token')
    }
    for elem in list_receipt:
        try:
            r = requests.post(url=url, headers=headers, json=elem, timeout=20)
            r.raise_for_status()
            status_code = r.status_code
        except requests.exceptions.MissingSchema as exc:
            logging.debug(exc)
            status_code = 404
        except requests.exceptions.Timeout as exc:
            logging.debug(exc)
            status_code = 700
        except requests.exceptions.HTTPError as exc:
            status_code = r.status_code
            logging.debug(exc)
        if status_code == 200:
            list_good_sended.append(elem['id'])
    return list_good_sended


def delete_sended_receipts_from_local_db(rec_db: Receiptinsql, list_sended: List) -> None:
    """
    функция удаления чеков, которые успешно отправлены в 1С из локальной базы
    :param rec_db: локальная база с чеками
    :param list_sended: список успешных
    :return: None
    """
    for elem in list_sended:
        rec_db.delete_receipt(elem)


def main():
    receipt_db = Receiptinsql(db_path=os.getenv('receipt_sql_path'))
    list_receipt_to_1C = get_receipts(receipt_db)
    list_sended = send_receipt_to_1C(list_receipt_to_1C)
    delete_sended_receipts_from_local_db(receipt_db, list_sended)

if __name__ == '__main__':
    main()

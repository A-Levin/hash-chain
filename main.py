# ТЗ звучит так. Есть таблица в Mysql,
# ее нужно конвертировать в цепочку хешей,
# где ячейка таблица хеш, строка - цепочка хешей ячейек,
# а вся таблица İPNS хеш
# хеш с ячейка1  и добавляется в ячейка2 и т.п.

import mysql.connector
import os
from dotenv import load_dotenv
import ipfshttpclient
import logging
import hashlib


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Loading settings from .env file
load_dotenv()

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
IPFS_HOST = os.getenv('IPFS_HOST', "127.0.0.1")
IPFS_PORT = os.getenv('IPFS_PORT', 5001)
def fetch_data_from_mysql(table_name='test_table'):
    """Getting data from MySQL database"""
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise

def calculate_cell_hash(cell_data, previous_cell_hash="") -> str:
    """Calculating cell hash"""
    hash_input = cell_data + previous_cell_hash
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

def calculate_row_hash(row_data) -> str:
    """Calculating row hash"""
    row_hash = ""
    previous_cell_hash = ""
    for cell_data in row_data:
        # step 1: calculate cell hash and add it to row hash
        cell_hash = calculate_cell_hash(cell_data, previous_cell_hash)
        # step 2: renew previous cell hash
        previous_cell_hash = cell_hash
        row_hash += cell_hash
    return hashlib.sha256(row_hash.encode("utf-8")).hexdigest()

def calculate_table_hash(table_data) -> str:
    """Calculating table IPNS-hash"""
    try:
        # TODO: we can make it parallel and split to batches if needed
        # step 1: calculate row hashes
        all_row_hashes = [calculate_row_hash(row_data) for row_data in table_data]
        # step 2: concatenate all row hashes
        all_row_hashes = "".join(all_row_hashes)
        # step 3: calculate table hash
        return upload_to_ipfs(all_row_hashes)
    except Exception as e:
        logger.error(f"Error calculating table hash: {e}")
        raise



def upload_to_ipfs(data) -> str:
    """Uploading data to IPFS"""
    try:
        with ipfshttpclient.connect(f'/ip4/{IPFS_HOST}/tcp/{IPFS_PORT}') as client:
            # uploading data to IPFS
            ipfs_hash = client.add_str(data)['Hash']
            # publishing data to IPNS
            # need to implement this step
        return ipfs_hash
    except Exception as e:
        logger.error(f"Error uploading to IPFS or publishing to IPNS: {e}")
        raise


def main():
    try:
        table_data = fetch_data_from_mysql('test_table')
        ipns_hash = calculate_table_hash(table_data)
        logger.info(f"IPNS hash: {ipns_hash}\n")
    except Exception as e:
        logger.error(f"Main process error: {e}")
        raise

if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 12:00:00 2023

@author: Jhonatan Martínez
"""
import threading

import cx_Oracle
from typing import List, Dict
from cx_Oracle import SessionPool
from loguru import logger
from OracleCnx.constants import *


class PoolDB:
    """ Permite realizar un pool de conexiones a una Base de Datos"""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super(PoolDB, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, setup: Dict[str, str], pool_size: int = 10) -> None:
        """Constructor.

        Args:
        setup (Dict[str, str]):
            El diccionario necesita de las siguientes keys:
                - host: Server host.
                - port: Server port.
                - sdi: Database SDI.
                - user: Database user.
                - password: Database password.
                - driver: Database driver.
        pool_size (int): tamaño del pool por defecto 10

        Returns:
            None.
        """
        if self._initialized:
            return
        self._initialized = True
        self.__attributes = ['host', 'port', 'sdi', 'user', 'password', 'driver']
        self.__setup: Dict = setup
        self.__pool_size = pool_size
        self.__pool: SessionPool = None
        self.__main()

    @staticmethod
    def __find_lob_columns(column_descriptions) -> List:
        """Iterar sobre las descripciones de las columnas y mostrar información sobre los tipos de datos.
        Returns:
            List: Lista con los índices de las columnas Lobs.
        """
        lob_columns = []
        for index, column in enumerate(column_descriptions):
            # column_name, type_code, display_size, internal_size, precision, scale, null_ok = column
            type_code: int = column[1]
            if type_code == cx_Oracle.CLOB:
                lob_columns.append(index)
        return lob_columns

    def __main(self) -> None:
        """Válida que el diccionario contenga los atributos necesarios para que la clase funcione e inicia la conexión."""
        logger.debug(self.__setup)
        missing = [key for key in self.__attributes if str(key).lower() not in self.__setup.keys()]
        if len(missing) > 0:
            logger.error(MISSING_ATTRIBUTES)
            logger.error(missing)
        try:
            cx_Oracle.init_oracle_client(lib_dir=self.__setup["driver"])
        except (cx_Oracle.DatabaseError, cx_Oracle.IntegrityError, Exception) as exc:
            logger.warning(str(exc))
        try:
            dsn = self.__setup["host"] + ":" + self.__setup["port"] + '/' + self.__setup["sdi"]
            self.__pool = cx_Oracle.SessionPool(
                user=self.__setup["user"],
                password=self.__setup["password"],
                dsn=dsn,
                min=self.__pool_size,
                max=self.__pool_size,
                increment=0,
                threaded=True
            )
        except (cx_Oracle.DatabaseError, cx_Oracle.IntegrityError, Exception) as exc:
            logger.warning(str(exc))

    def read_data(self, query: str, parameters: dict = {}, datatype: str = "dict") -> [Dict, List]:
        show_data = None
        datatype = datatype.lower()
        if datatype in ['dict', 'list']:
            try:
                with self.__pool.acquire() as cnx:
                    with cnx.cursor() as cursor:
                        cursor.prefetchrows = 100000
                        cursor.arraysize = 100000
                        cursor.execute(query, parameters)
                        query = cursor.statement
                        lob_columns = self.__find_lob_columns(cursor.description)
                        data = []
                        if len(lob_columns) > 0:
                            for row in cursor:
                                new_row = list(row)
                                for i, column in enumerate(row):
                                    if i in lob_columns:
                                        new_row[i] = column.read()
                                data.append(tuple(new_row))
                        else:
                            data = cursor.fetchall()
                        columns = [column[0].upper() for column in cursor.description]
                        if datatype == 'dict':
                            dictionary = []
                            for item in data:
                                dictionary.append(dict(zip(columns, item)))
                            show_data = dictionary
                        elif datatype == 'list':
                            show_data = [columns, data]
                    logger.info(f"{DATA_OBTAINED} {query}")
            except (cx_Oracle.DatabaseError, Exception) as exc:
                logger.error(f"Error in query {query}: {str(exc)}", exc_info=True)
        else:
            logger.warning(INVALID_DATATYPE)

        return show_data

    def execute_query(self, query: str, parameters: Dict = {}) -> bool:
        result = False

        try:
            with self.__pool.acquire() as cnx:
                with cnx.cursor() as cursor:
                    cursor.execute(query, parameters)
                    query = cursor.statement
                cnx.commit()
                logger.info(f"{EXECUTED_QUERY} {query}")
                result = True
        except (cx_Oracle.DatabaseError, Exception) as exc:
            logger.error(f"Error in query {query}: {str(exc)}", exc_info=True)
            cnx.rollback()
        return result

    def execute_many(self, query: str, values: List) -> bool:
        result = False
        try:
            with self.__pool.acquire() as cnx:
                with cnx.cursor() as cursor:
                    cursor.prepare(query)
                    cursor.executemany(None, values)
                    query = cursor.statement
                cnx.commit()
                logger.info(f"{EXECUTED_QUERY} {query}")
                result = True
        except (cx_Oracle.DatabaseError, Exception) as exc:
            logger.error(f"Error in query {query}: {str(exc)}", exc_info=True)
            cnx.rollback()
        finally:
            self.__pool.release(self.__connection)

        return result

# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 12:00:00 2023

@author: Jhonatan Martínez
"""

import cx_Oracle
from typing import List, Dict
from cx_Oracle import SessionPool
from MyLogger import setup_logger
from OracleCnx.constants import *

logger = setup_logger(__name__)


class PoolDB:
    """ Permite realizar un pool de conexiones a una Base de Datos"""
    def __init__(self, setup: Dict[str, str], pool_size: int = 5) -> None:
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
        """Válida que el diccionario contenga los atributos necesarios para que la clase funcione."""
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

    def __get_connection(self) -> bool:
        """Crear y obtener la conexión a una base de datos

        Returns:
            bool: True si se establece la conexión, False en caso contrario.
        """
        try:
            connection = self.__pool.acquire()
            if connection is not None:
                self.__connection = connection
                logger.debug(ESTABLISHED_CONNECTION, self.__setup["host"])
                return True
            else:
                logger.warning(NO_CONNECTION)
                return False
        except (cx_Oracle.DatabaseError, Exception) as exc:
            logger.error(str(exc), exc_info=True)
            return False

    def read_data(self, query: str, parameters: dict = {}, datatype: str = "dict") -> [Dict, List]:
        show_data = None
        if self.__get_connection():
            datatype = datatype.lower()
            if datatype in ['dict', 'list']:
                try:
                    with self.__connection as cnx:
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
                        logger.info(DATA_OBTAINED, query)
                except (cx_Oracle.DatabaseError, Exception) as exc:
                    logger.error(f"Error in query {query}: {str(exc)}", exc_info=True)
            else:
                logger.warning(INVALID_DATATYPE)
        else:
            logger.warning(NO_CONNECTION)

        return show_data

    def execute_query(self, query: str, parameters: Dict = {}) -> bool:
        if self.__get_connection():
            try:
                with self.__connection as cnx:
                    with cnx.cursor() as cursor:
                        cursor.execute(query, parameters)
                        query = cursor.statement
                    cnx.commit()
                    logger.info(EXECUTED_QUERY, query)
                    return True
            except (cx_Oracle.DatabaseError, Exception) as exc:
                logger.error(f"Error in query {query}: {str(exc)}", exc_info=True)
                cnx.rollback()
                return False
        else:
            logger.warning(NO_CONNECTION)
            return False

    def execute_many(self, query: str, values: List) -> bool:
        if self.__get_connection():
            try:
                with self.__connection as cnx:
                    with cnx.cursor() as cursor:
                        cursor.prepare(query)
                        cursor.executemany(None, values)
                        query = cursor.statement
                    cnx.commit()
                    logger.info(EXECUTED_QUERY, query)
                    return True
            except (cx_Oracle.DatabaseError, Exception) as exc:
                logger.error(f"Error in query {query}: {str(exc)}", exc_info=True)
                cnx.rollback()
                return False
        else:
            logger.warning(NO_CONNECTION)
            return False

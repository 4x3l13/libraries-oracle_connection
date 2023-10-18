# -*- coding: utf-8 -*-
"""
Created on Mon Oct 02 10:00:00 2023

@author: Jhonatan Martínez
"""

from typing import Dict, List
from MyLogger import setup_logger
import cx_Oracle
from OracleCnx.constants import *


logger = setup_logger(__name__)


class ConnectionDB:
    """ Permite realizar una conexión a una Base de Datos"""

    def __init__(self, setup: Dict[str, str]) -> None:
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

        Returns:
            None.
        """

        self.__attributes = ['host', 'port', 'sdi', 'user', 'password', 'driver']
        self.__connection: cx_Oracle.connect = None
        self.__setup: Dict = setup
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
        logger.debug(self.__setup)
        """Válida que el diccionario contenga los atributos necesarios para que la clase funcione."""
        missing = [key for key in self.__attributes if str(key).lower() not in self.__setup.keys()]
        if len(missing) > 0:
            logger.error(MISSING_ATTRIBUTES)
            logger.error(missing)
        try:
            cx_Oracle.init_oracle_client(lib_dir=self.__setup["driver"])
        except (cx_Oracle.DatabaseError, cx_Oracle.IntegrityError, Exception) as exc:
            if cx_Oracle.clientversion() is None:
                logger.warning(str(exc))

    def __close_connection(self) -> None:
        """Cerrar la conexión a la base de datos."""
        try:
            if self.__connection is not None:
                self.__connection.close()
                logger.debug(CLOSE_CONNECTION)
        except (cx_Oracle.DatabaseError, Exception) as exc:
            logger.error(str(exc), exc_info=True)

    def __get_connection(self) -> bool:
        """Crear y obtener la conexión a una base de datos

        Returns:
            bool: True si se establece la conexión, False en caso contrario.
        """

        self.__connection = None
        try:
            dsn = self.__setup["host"] + ":" + self.__setup["port"] + '/' + self.__setup["sdi"]
            self.__connection = cx_Oracle.connect(user=self.__setup["user"],
                                                  password=self.__setup["password"],
                                                  dsn=dsn,
                                                  encoding="UTF-8")
            logger.debug(ESTABLISHED_CONNECTION, self.__setup["host"])
            return True
        except (ConnectionError, Exception) as exc:
            self.__connection = None
            logger.error(str(exc), exc_info=True)
            return False

    def read_data(self, query: str, parameters: dict = {}, datatype: str = "dict") -> [Dict, List]:
        """Obtener los datos de una consulta.

        Args:
            query (str): Consulta a ejecutar.
            parameters (Dict, optional): Parámetros de la consulta.
            datatype (str, optional): Tipo de datos a retornar.

        Returns:
            show_data[Dict,List]: Datos obtenidos.
        """
        show_data = None
        if self.__get_connection():
            datatype = datatype.lower()
            if datatype in ['dict', 'list']:
                try:
                    with self.__connection as cnx:
                        with cnx.cursor() as cursor:
                            cursor.prefetchrows = 100000
                            cursor.arraysize = 100000
                            # Ejecutar la consulta
                            cursor.execute(query, parameters)
                            query = cursor.statement
                            # Obtener las descripciones de las columnas.
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
                            # Gets column_names
                            columns = [column[0].upper() for column in cursor.description]
                            # Validate the datatype to return
                            if datatype == 'dict':
                                dictionary = []
                                for item in data:
                                    dictionary.append(dict(zip(columns, item)))
                                show_data = dictionary
                            elif datatype == 'list':
                                show_data = [columns, data]
                        logger.info(DATA_OBTAINED, query)
                except (cx_Oracle.DatabaseError, Exception) as exc:
                    logger.error(f"Error en query {query}: {str(exc)}", exc_info=True)
            else:
                logger.warning(INVALID_DATATYPE)
        else:
            logger.warning(NO_CONNECTION)

        return show_data

    def execute_query(self, query: str, parameters: Dict = {}) -> bool:
        """
        Ejecutar una consulta.

        Args:
            query (str): Consulta a ejecutar.
            parameters (Dict, optional): Parámetros de la consulta.

        Returns:
            bool: True si se ejecuta correctamente, False en caso contrario.
        """
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
                logger.error(f"Error en query {query}: {str(exc)}", exc_info=True)
                cnx.rollback()
                return False
        else:
            logger.warning(NO_CONNECTION)
            return False

    def execute_many(self, query: str, values: List) -> bool:
        """Ejecutar una consulta con varios valores.

        Args:
            query (str): Consulta a ejecutar.
            values (List): Valores de la consulta.

        Returns:
            bool: True si se ejecuta correctamente, False en caso contrario.
        """
        if self.__get_connection():
            try:
                with self.__get_connection() as cnx:
                    with cnx.cursor() as cursor:
                        cursor.prepare(query)
                        cursor.executemany(None, values)
                        query = cursor.statement
                    cnx.commit()
                    logger.info(EXECUTED_QUERY, query)
                    return True
            except (cx_Oracle.DatabaseError, Exception) as exc:
                logger.error(f"Error en query {query}: {str(exc)}", exc_info=True)
                cnx.rollback()
                return False
        else:
            logger.warning(NO_CONNECTION)
            return False

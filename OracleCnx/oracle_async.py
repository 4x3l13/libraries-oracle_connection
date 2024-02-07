# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 14:00:00 2024

@author: Jhonatan Martínez
"""

import asyncio
import cx_Oracle
from typing import Dict, List, Optional
from loguru import logger
from OracleCnx.constants import *


class AsyncDB:
    """ Permite realizar una conexión asincrona a una Base de Datos"""

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
        self.__validate_attributes()

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

    def __validate_attributes(self) -> None:
        """Válida que el diccionario contenga los atributos necesarios para que la clase funcione."""
        logger.debug(self.__setup)
        missing_attributes = [key for key in self.__attributes if str(key).lower() not in self.__setup.keys()]
        if missing_attributes:
            logger.error(MISSING_ATTRIBUTES)
            logger.error(missing_attributes)
        try:
            cx_Oracle.init_oracle_client(lib_dir=self.__setup["driver"])
        except (cx_Oracle.DatabaseError, cx_Oracle.IntegrityError, Exception) as exc:
            if cx_Oracle.clientversion() is None:
                logger.warning(str(exc))

    async def __open_connection(self) -> bool:
        """Crear y obtener la conexión a una base de datos

        Returns:
            bool: True si se establece la conexión, False en caso contrario.
        """

        self.__connection = None
        server = f"{self.__setup['host']}:{self.__setup['port']}/{self.__setup['sdi']}"
        try:
            logger.debug(f"Trying to connect to server {server}")

            def sync_connect():
                try:
                    dsn = f"{self.__setup['host']}:{self.__setup['port']}/{self.__setup['sdi']}"
                    conn = cx_Oracle.connect(user=self.__setup["user"],
                                             password=self.__setup["password"],
                                             dsn=dsn,
                                             encoding="UTF-8")
                except (ConnectionError, cx_Oracle.DatabaseError, Exception) as exc:
                    conn = None
                    logger.error(f"Error connecting to server {server}: {str(exc)}", exc_info=True)
                return conn

            self.__connection = await asyncio.get_event_loop().run_in_executor(None, sync_connect)
            logger.debug(f'{ESTABLISHED_CONNECTION} {server}')
            return True
        except (ConnectionError, cx_Oracle, Exception) as exc:
            self.__connection = None
            logger.error(f"Error connecting to server {server}: {str(exc)}", exc_info=True)
            return False

    def __close_connection(self) -> None:
        """Cerrar la conexión a la base de datos."""
        try:
            if self.__connection:
                self.__connection.close()
                logger.debug(CLOSE_CONNECTION)
        except (cx_Oracle.DatabaseError, Exception) as exc:
            logger.warning(str(exc))

    async def read_data(self, query: str, parameters: Optional[dict] = None, datatype: str = "dict") -> [Dict, List]:
        """Obtener los datos de una consulta.

        Args:
            query (str): Consulta a ejecutar.
            parameters (dict, optional): Parámetros de la consulta.
            datatype (str, optional): Tipo de datos a retornar.

        Returns:
            show_data[Dict,List]: Datos obtenidos.
        """
        show_data = None

        if await self.__open_connection():
            datatype = datatype.lower()

            if datatype in ['dict', 'list']:

                    def sync_read_data():
                        try:
                            with self.__connection as cnx:
                                with cnx.cursor() as cursor:
                                    cursor.prefetchrows = 100000
                                    cursor.arraysize = 100000
                                    # Ejecutar la consulta
                                    if parameters:
                                        cursor.execute(query, parameters)
                                    else:
                                        cursor.execute(query)
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
                                        data = [dict(zip(columns, item)) for item in data]
                                    elif datatype == 'list':
                                        data = [columns, data]

                                    logger.info(f'{DATA_OBTAINED} {query}')
                                    return data
                        except (cx_Oracle.DatabaseError, Exception) as exc:
                            logger.error(f"Error: {str(exc)}", exc_info=True)
                        finally:
                            self.__close_connection()

                    show_data = await asyncio.get_event_loop().run_in_executor(None, sync_read_data)



            else:
                logger.warning(INVALID_DATATYPE)
        else:
            logger.warning(NO_CONNECTION)

        return show_data

    async def execute_query(self, query: str, parameters: Optional[dict] = None) -> bool:
        """
        Ejecutar una consulta.

        Args:
            query (str): Consulta a ejecutar.
            parameters (Dict, optional): Parámetros de la consulta.

        Returns:
            bool: True si se ejecuta correctamente, False en caso contrario.
        """
        if await self.__open_connection():
            def sync_execute_query():
                try:
                    with self.__connection as cnx:
                        with cnx.cursor() as cursor:
                            if parameters:
                                cursor.execute(query, parameters)
                            else:
                                cursor.execute(query)
                        cnx.commit()
                        logger.info(f"{EXECUTED_QUERY} {query}")
                        return True
                except (cx_Oracle.DatabaseError, Exception) as exc:
                    logger.error(f"Error en query {query}: {str(exc)}", exc_info=True)
                    cnx.rollback()
                finally:
                    self.__close_connection()

            await asyncio.get_event_loop().run_in_executor(None, sync_execute_query)
            return True
        else:
            logger.warning(NO_CONNECTION)
            return False

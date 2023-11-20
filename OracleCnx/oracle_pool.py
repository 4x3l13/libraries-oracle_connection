import cx_Oracle
from typing import List, Dict
from cx_Oracle import SessionPool
from MyLogger import setup_logger
from OracleCnx.constants import *
import asyncio

logger = setup_logger(__name__)


class AsyncPoolDB:
    def __init__(self, setup: Dict[str, str], pool_size: int = 5) -> None:
        self.__attributes = ['host', 'port', 'sdi', 'user', 'password', 'driver']
        self.__setup: Dict = setup
        self.__pool_size = pool_size
        self.__pool: SessionPool = None
        self.__main()

    async def __main(self) -> None:
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

    async def read_data(self, query: str, parameters: dict = {}, datatype: str = "dict") -> [Dict, List]:
        show_data = None
        datatype = datatype.lower()
        if datatype in ['dict', 'list']:
            try:
                async with self.__pool.acquire() as cnx:
                    async with cnx.cursor() as cursor:
                        cursor.prefetchrows = 100000
                        cursor.arraysize = 100000
                        await cursor.execute(query, parameters)
                        query = cursor.statement
                        lob_columns = await self.__find_lob_columns(cursor.description)
                        data = []
                        if len(lob_columns) > 0:
                            async for row in cursor:
                                new_row = list(row)
                                for i, column in enumerate(row):
                                    if i in lob_columns:
                                        new_row[i] = await column.read()
                                data.append(tuple(new_row))
                        else:
                            data = await cursor.fetchall()
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

        return show_data

    async def execute_query(self, query: str, parameters: Dict = {}) -> bool:
        result = False

        try:
            async with self.__pool.acquire() as cnx:
                async with cnx.cursor() as cursor:
                    await cursor.execute(query, parameters)
                    query = cursor.statement
                await cnx.commit()
                logger.info(EXECUTED_QUERY, query)
                result = True
        except (cx_Oracle.DatabaseError, Exception) as exc:
            logger.error(f"Error in query {query}: {str(exc)}", exc_info=True)
            await cnx.rollback()
        return result

    async def execute_many(self, query: str, values: List) -> bool:
        result = False
        try:
            async with self.__pool.acquire() as cnx:
                async with cnx.cursor() as cursor:
                    await cursor.prepare(query)
                    await cursor.executemany(None, values)
                    query = cursor.statement
                await cnx.commit()
                logger.info(EXECUTED_QUERY, query)
                result = True
        except (cx_Oracle.DatabaseError, Exception) as exc:
            logger.error(f"Error in query {query}: {str(exc)}", exc_info=True)
            await cnx.rollback()

        return result

    async def __find_lob_columns(self, column_descriptions) -> List:
        lob_columns = []
        for index, column in enumerate(column_descriptions):
            type_code: int = column[1]
            if type_code == cx_Oracle.CLOB:
                lob_columns.append(index)
        return lob_columns

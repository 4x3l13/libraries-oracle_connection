Librería Conectar a bases de datos


Por Jhonatan Martínez - jhonatanmartinez130220@gmail.com


Librería te permite crear conexiones para base de datos Oracle:

Para utilizarla solo necesitas pasar un diccionario con los datos solicitados, estos se pueden ver al invocar la clase ConnectionDB.

💡 Prerequisitos:
Python 3.8.9,

cx_Oracle==8.3.0,

📚 Ejemplo de uso para conexión normal:

    from OracleCnx import ConnectionDB
    
    cnx = ConnectionDB(setup=my_dictionary)
    
    data = cnx.read_data(query='select * from table')

📚 Ejemplo de uso para conexión pool:

    from OraclePool import PoolDB
    
    cnx = ConnectionDB(setup=my_dictionary, pool_size=10)
    
    data = cnx.read_data(query='select * from table')
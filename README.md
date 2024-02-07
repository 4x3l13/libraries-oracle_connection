Librería Conectar a bases de datos Oracle


Por Jhonatan Martínez - jhonatanmartinez130220@gmail.com


Librería que permite crear conexiones normal, con pool y asincronas para base de datos Oracle:

Para utilizarla solo necesitas pasar un diccionario con los siguientes datos:

                - host: Server host.
                - port: Server port.
                - sdi: Database SDI.
                - user: Database user.
                - password: Database password.
                - driver: Database driver.

💡 Prerequisitos:
Python 3.8.9, [Otras librerias](requirements.txt)

📚 Ejemplo de uso para conexión normal:

    from OracleCnx import CnxOracle

    my_setup = {
        "host": "localhost",
        "port": "5432",
        "sdi": "mi_base_de_datos",
        "user": "mi_usuario",
        "password": "mi_contrasena"
        "driver": "F:\Drivers\oracle\instantclient_11_2",
    }
    
    cnx = ConnectionDB(setup=my_setup)
    
    data = cnx.read_data(query='select * from table')

📚 Ejemplo de uso para conexión pool:

    from OracleCnx import PoolOracle

    my_setup = {
        "host": "localhost",
        "port": "5432",
        "sdi": "mi_base_de_datos",
        "user": "mi_usuario",
        "password": "mi_contrasena"
        "driver": "F:\Drivers\oracle\instantclient_11_2",
    }
    
    cnx = PoolDB(setup=my_setup, pool_size=10)
    
    data = cnx.read_data(query='select * from table')

Ejemplo de uso para conexión asincrona:

    from OracleCnx import AsyncOracle

    my_setup = {
        "host": "localhost",
        "port": "5432",
        "sdi": "mi_base_de_datos",
        "user": "mi_usuario",
        "password": "mi_contrasena"
        "driver": "F:\Drivers\oracle\instantclient_11_2",
    }
    
    cnx = AsyncDB(setup=my_setup)
    
    data = await cnx.read_data(query='select * from table')
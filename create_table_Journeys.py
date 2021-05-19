"""Generate Table Journey

    This script connects a posgreSQL database and brings the data from 
    column type:json from a table in the database and creats a new table 
    in the same database and converts the data to it.
    It brings a specified number of rows at a time until the end of the 
    lines or until a specified row

    Author: Roaa MASRI
"""
import psycopg2
import json

table =  "log_inputs"
schema = "transport"
column = "id, parameters, journeys"
TblName = "journey"
counter = 0
id_min = 0
id_max = 1000
id_final = 1002277

#- - - - - - - - - - - - - - - - - - - - - - - - -  Connect to PostgreSQL database
def connect_to_db(host, port, database, use, password):
    """
    Connects to postgreSQL database
    Parameters:
    -----------
        host: string
        posrt: string
        database: string
        use: string
        password: string

    Returns:
    --------
        Message if it connected to the database or there was an error.
            
    """
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host=host, port=port, database=database, user=use, password=password)
        print('Connected')
        return (conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


#- - - - - - - - - - - - - - - - - - - - - - - - -  Select column from PostgreSQL database table
def select_column(connection, min_id, max_id):
    """
    Brings a column's data from Postgres database table
    Parameters:
    -----------
        connection: PostgreSQL database connection
        min_id: integer
        max_id: integer

    Returns:
    --------
        List
            ex. [{
                    "type": "best",
                    "duration": 27,
                    "sections": 
                        [{
                            "mode": "walking",
                            "type": "street_network",
                            "to_id": "stop_area:ANG:SA:zone_JUSTICES",
                            "from_id": "stop_area:ANG:SA:zone_JUSTICES",
                            "line_id": "",
                            "to_name": "JUSTICES",
                            "duration": 27,
                            "route_id": "",
                            "to_coord": {
                                "lat": 47.453564,
                                "lon": -0.526251
                            },
                            "from_name": "JUSTICES",
                            "from_coord": {
                                "lat": 47.453564,
                                "lon": -0.526251
                            },
                            "network_id": "",
                            "to_admin_id": "admin:fr:49007",
                            "from_admin_id": "admin:fr:49007",
                            "to_admin_name": "Angers",
                            "to_admin_insee": "49007",
                            "from_admin_name": "Angers",
                            "from_admin_insee": "49007",
                            "physical_mode_id": "",
                            "to_embedded_type": "stop_area",
                            "arrival_date_time": 1537953682,
                            "commercial_mode_id": "",
                            "from_embedded_type": "stop_area",
                            "vehicle_journey_id": "",
                            "departure_date_time": 1537953655
                        }],
                    "nb_transfers": 0,
                    "arrival_date_time": 1537953682,
                    "departure_date_time": 1537953655,
                    "requested_date_time": 1537953655
                }]
            
    """
    # create cursor
    cursor = connection.cursor()
    # The sql query that select a column from a table from a schema in the database  
    postgreSQL_select_Query = """select """ + column + """ from """ + schema + """.
    """ +  table + """ where id > """ + str(min_id) + """ and id <= """ + str(max_id) + """;"""
    # select the column
    cursor.execute(postgreSQL_select_Query)
    # put rows' data in JTab
    JTab = cursor.fetchall()
    return JTab
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  Create Table
def create_table(connection):
    """
    Creats a table in the database
    Parameters:
    -----------
        connection: PostgreSQL database connection
        
    Returns:
    --------
        an empty table in the database or an error message if it didn't 
        connect correctly to the database
    """
    # the sql query that creat a new table
    sql = """DROP TABLE IF EXISTS """ + schema + "." + TblName +""" ;
    CREATE TABLE """ + schema + "." + TblName + """ ( 
        "request_id" int,
        "parameters" jsonb,
        "num_journey" int,
        "type" varchar,
        "duration" int,
        "sections" jsonb,
        "nb_transfers" int,
        "from_coord_lat" float,
        "from_coord_lon" float,
        "to_coord_lat" float,
        "to_coord_lon" float,
        "first_pt_name" varchar,
        "arrival_date_time" int,
        "departure_date_time" int,
        "requested_date_time" int,
        "last_pt_name" varchar,
        "first_pt_coord_lat" float,
        "first_pt_coord_lon" float,
        "last_pt_coord_lat" float,
        "last_pt_coord_lon" float  
    )"""
    print( sql)

    try:
        # create cursor
        cur = connection.cursor()
        # create table
        cur.execute(sql)
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -  main()

print("*** connecting to db")
# conecting to the database 
connection = connect_to_db("localhost", "5432", "SMA", "postgres", "password")
print("*** create table ")
# creat the table
create_table(connection)
# inserting the data in the table
# a while loop to bring a specified number of rows at a time until the end or until a specified row
while(counter < id_final):
    # Convert to journys_records the result Lists from the function select_column
    journys_records = select_column (connection, id_min, id_max)
    id_min += 1000
    id_max += 1000
    if (id_max > id_final) :
        id_max = id_final
    # The sql query that insert the data in the table wich created before
    sql = """ INSERT INTO transport.journey
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    print("Print each row and it's columns values")
    for row in journys_records :
        for i in range (0, len(row[2])):
            cur = connection.cursor()
            log_id = row[0]
            parameters = row[1]
            num_journey = i + 1
            journey_type = row[2][i]["type"]
            duration = row[2][i]["duration"]
            sections = row[2][i]["sections"]
            nb_transfers = row[2][i]["nb_transfers"]
            from_coord_lat = row[2][i]["sections"][0]["from_coord"]["lat"]
            from_coord_lon = row[2][i]["sections"][0]["from_coord"]["lon"]
            to_coord_lat = row[2][i]["sections"][-1]["to_coord"]["lat"]
            to_coord_lon = row[2][i]["sections"][-1]["to_coord"]["lon"]
            if ("first_pt_name" in row[2][i]) :
                first_pt_name = row[2][i]["first_pt_name"]
            else:
                first_pt_name = ''
            arrival_date_time = row[2][i]["arrival_date_time"]
            departure_date_time = row[2][i]["departure_date_time"]
            requested_date_time = row[2][i]["requested_date_time"]
            if ("last_pt_name" in row[2][i]) :
                last_pt_name = row[2][i]["last_pt_name"]
            else :
                last_pt_name = ''
            if ("first_pt_coord" in row[2][i]):
                first_pt_coord_lat = row[2][i]["first_pt_coord"]["lat"]
                first_pt_coord_lon = row[2][i]["first_pt_coord"]["lon"]
            else :
                first_pt_coord_lat = None
                first_pt_coord_lon = None
            if ("last_pt_coord" in row[2][i]):
                last_pt_coord_lat = row[2][i]["last_pt_coord"]["lat"]
                last_pt_coord_lon = row[2][i]["last_pt_coord"]["lon"]
            else :
                last_pt_coord_lat = None
                last_pt_coord_lon = None
            # insert the data
            cur.execute(sql, (log_id, json.dumps(parameters), num_journey, journey_type, duration, json.dumps(sections), 
            nb_transfers, from_coord_lat, from_coord_lon, to_coord_lat, to_coord_lon,
            first_pt_name, arrival_date_time, departure_date_time, requested_date_time, last_pt_name,
            first_pt_coord_lat, first_pt_coord_lon, last_pt_coord_lat, last_pt_coord_lon))
            # commit the changes
            connection.commit()
        counter += 1
        print(counter)
    print("id_min = ", id_min)
    print("id_max = ", id_max)
# close the connection to the database
connection.close()

print( "*** fin tmp ***" )
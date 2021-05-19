"""Connect to the HERE routing API

    This script sends coordinats from table in the postgreSQL database 
    to the HERE RoutingAPI and update the table with the (time and distance)
    for three type of transport (bike, car and walk)

    Author: Roaa MASRI
"""

import psycopg2
import json
import requests
from herepy import (
    RoutingApi,
    RouteMode,
    MatrixRoutingType,
    MatrixSummaryAttribute,
    RoutingTransportMode,
    RoutingMode,
    RoutingApiReturnField,
    RoutingMetric,
    RoutingApiSpanField,
)

# table = "sample"
table = "sample_10"
schema = "transport"
counter = 0
id_min = 0
id_max = 1000
# id_final = 13973
id_final = 92446

#- - - - - - - - - - - - - - - - - - - - - - - - -  Connect to Postgres database
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

#- - - - - - - - - - - - - - - - - - - - - - - - -  bring data from Postgres database table
def select_row(connection, min_id, max_id):
    """
    Brings data from Postgres database table
    Parameters:
    -----------
        connection: PostgreSQL database connection
        min_id: integer
        max_id: integer

    Returns:
    --------
        List
            in this list there are the data that comme from a table in the database
    """
    # create cursor
    cursor = connection.cursor()
    # The sql query that select columns from a table from a schema in the database 
    postgreSQL_select_Query = """select id, first_pt_coord_lat, first_pt_coord_lon, 
    last_pt_coord_lat, last_pt_coord_lon from """+ schema +"""."""+ table +""" where 
    id > """ + str(min_id) + """ and id <= """ + str(max_id) + """;"""
    # select the columns
    cursor.execute(postgreSQL_select_Query)
    # put rows' data in Tab 
    Tab = cursor.fetchall()
    return Tab
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - main()

# connecting to db
connection = connect_to_db("localhost", "5432", "SMA", "postgres", "password")
# using RoutingAPI
routing_api = RoutingApi(api_key=" ")
# a while loop to bring coordinats from a specified number of rows at a time
# and send it to the Api and insert the reponses in the table
while(counter < id_final):
    # Convert to data_table the result Lists from the function select_row
    data_table = select_row (connection, id_min, id_max)
    id_min += 1000
    id_max += 1000
    if (id_max > id_final) :
        id_max = id_final
    for row in data_table :
        cur = connection.cursor()
        # fetches a bicycle route between two points
        response = routing_api.bicycle_route(
            waypoint_a=[row[1], row[2]],
            waypoint_b=[row[3], row[4]]
        )
        response = response.as_dict()
        distance_bic = response['response']['route'][0]['summary']['distance']
        time_bic = response['response']['route'][0]['summary']['baseTime']
        # print("bicycle",distance_bic)
        # print("bicycle",time_bic)

        # fetches a driving route between two points
        response = routing_api.car_route(
            waypoint_a=[row[1], row[2]],
            waypoint_b=[row[3], row[4]],
            modes=[RouteMode.car, RouteMode.fastest],
        )
        response = response.as_dict()
        distance_car = response['response']['route'][0]['summary']['distance']
        time_car = response['response']['route'][0]['summary']['baseTime']
        # print("car",distance_car)
        # print("car",time_car)

        # fetches a pedastrian route between two points
        response = routing_api.pedastrian_route(
            waypoint_a=[row[1], row[2]],
            waypoint_b=[row[3], row[4]],
            modes=[RouteMode.pedestrian, RouteMode.fastest],
        )
        response = response.as_dict()
        distance_walk = response['response']['route'][0]['summary']['distance']
        time_walk = response['response']['route'][0]['summary']['baseTime']
        # print("walk",distance_walk)
        # print("walk",time_walk)
        # The sql query that update data in the table with the data from API
        sql = """ UPDATE """ + schema + """.""" + table + """ 
            SET bike_distance = %s,
                bike_time = %s,
                car_distance = %s,
                car_time = %s,
                walk_distance = %s,
                walk_time = %s
                WHERE id = %s;"""
        # update the table
        cur.execute(sql, (distance_bic, time_bic, distance_car, time_car, distance_walk,time_walk,row[0]))
        # commit the changes
        connection.commit()
        counter+=1
        print(counter)

    print("id_min = ", id_min)
    print("id_max = ", id_max)
# close the connection to the database
connection.close()

print( "*** fin tmp ***" )
        
        
        

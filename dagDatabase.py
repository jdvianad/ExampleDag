from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'datapath',
}

with DAG(
    dag_id='dag_JuanDavidViana_TaskDatabase',
    default_args=default_args,
    start_date=datetime(2023, 6, 28),
    schedule_interval='@once',
    catchup=False,
    schedule=None
) as dag:
    
    task1 = PostgresOperator(
        task_id='create_flights_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS flights (
            flight_number VARCHAR PRIMARY KEY,
            airline VARCHAR NOT NULL,
            origin VARCHAR NOT NULL,
            destination VARCHAR NOT NULL,
            flight_date DATE NOT NULL);
        """
    )

    task2 = PostgresOperator(
        task_id='create_passengers_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS passengers (
            passenger_id SERIAL PRIMARY KEY,
            passenger_name VARCHAR NOT NULL,
            passenger_lastname VARCHAR NOT NULL,
            passenger_nationality VARCHAR NOT NULL,
            luggage_quantity INT NULL,
            flight_id VARCHAR NOT NULL);
        """
    )

    task3 = PostgresOperator(
        task_id='insert_into_table_flights_1',
        postgres_conn_id='postgres_localhost',
        sql="""
            INSERT INTO flights (flight_number, airline, origin, destination,flight_date)  VALUES ( 'AV2700', 'Avianca', 'Medellin', 'San Jose','2023-08-04');
            INSERT INTO flights (flight_number, airline, origin, destination,flight_date)  VALUES ( 'AV6900', 'Avianca', 'Bogota', 'Miami','2023-08-12');
        """
    )

    task4 = PostgresOperator(
        task_id='insert_into_table_flights_2',
        postgres_conn_id='postgres_localhost',
        sql="""

            INSERT INTO flights (flight_number, airline, origin, destination,flight_date)  VALUES ( 'LA2200', 'Latam', 'Bogota', 'Madrid','2023-07-02');
            INSERT INTO flights (flight_number, airline, origin, destination,flight_date)  VALUES ( 'VL008', 'Volaris', 'Cali', 'Cdmx','2023-07-02');
            INSERT INTO flights (flight_number, airline, origin, destination,flight_date)  VALUES ( 'CC2019', 'Copa Airlines', 'Medellin', 'Cancun','2023-09-02');

        """
    )

    task5 = PostgresOperator(
        task_id='insert_into_table_passengers',
        postgres_conn_id='postgres_localhost',
        sql="""
            INSERT INTO passengers (passenger_name,passenger_lastname,passenger_nationality, luggage_quantity,flight_id)  VALUES ( 'Ana', 'Duque', 'Colombian', 1,'LA2200');
            INSERT INTO passengers (passenger_name,passenger_lastname,passenger_nationality, luggage_quantity,flight_id)  VALUES ( 'Katherine', 'Sanchez', 'Mexican', 3,'VL008');
            INSERT INTO passengers (passenger_name,passenger_lastname,passenger_nationality, luggage_quantity,flight_id)  VALUES ( 'Juan', 'Viana', 'Colombian', 1,'AV2700');
            INSERT INTO passengers (passenger_name,passenger_lastname,passenger_nationality, luggage_quantity,flight_id)  VALUES ( 'Isabel', 'Pineda', 'Colombian', 1,'AV6900');
             INSERT INTO passengers (passenger_name,passenger_lastname,passenger_nationality, luggage_quantity,flight_id)  VALUES ( 'Stefany', 'Viana', 'Colombian', 2,'CC2019');
        """
    )

    task6 = PostgresOperator(
        task_id='join_tables_flights_passengers',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE final_trips_table AS SELECT flight_number,airline,origin,destination,flight_date,passenger_id,passenger_name,passenger_lastname,passenger_nationality,luggage_quantity FROM flights INNER JOIN passengers ON flight_number=flight_id;
        """
    )

    task7 = PostgresOperator(
        task_id='get_coming_flights_for_passengers',
        postgres_conn_id='postgres_localhost',
        sql="""
            SELECT * FROM final_trips_table WHERE flight_date>CURRENT_DATE ;
        """
    )

    task1>>[task3,task4]>>task6>>task7
    task2>>task5>>task6
   
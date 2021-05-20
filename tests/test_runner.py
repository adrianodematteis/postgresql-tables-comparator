import json
import pytest

from src.util.compare_tables import compare_tables
from src.util.red_shift_connection import Connection


@pytest.fixture(scope="session")
def postgres_service(docker_ip, docker_services):
    connection_conf = dict(dbname="postgres", host="127.0.0.1", user="postgres", password="mysecretpassword", port=5432)
    connection = Connection(connection_conf)

    return connection


def test(postgres_service):
    config_json = """{   
        "tablePairs": [
            {
                "oldTable": {
                    "schema": "public",
                    "tableName": "test_old"
                },
    
                "newTable": {
                    "schema": "public",
                    "tableName": "test_new"
                },
    
                "filterColumn": "filter_column",
                "filterDateValue": "timestamp"
            },
    
            {
                "oldTable": {
                    "schema": "public",
                    "tableName": "test_old"
                },
    
                "newTable": {
                    "schema": "public",
                    "tableName" : "test_new_2"
                },
    
                "filterColumn": "filter_column",
                "filterDateValue": "timestamp"
            }
        ]
    }"""

    config = json.loads(config_json)

    # Creating tables
    postgres_service.execute_and_commit("create table test_old (user_id serial PRIMARY KEY,"
                                        " username varchar (50) unique not null)")

    postgres_service.execute_and_commit("create table test_new (user_id serial PRIMARY KEY,"
                                        " username varchar (50) unique not null)")

    postgres_service.execute_and_commit("create table test_new_2 (user_id serial PRIMARY KEY,"
                                        " username varchar (50) unique not null)")

    postgres_service.execute_and_commit('INSERT INTO test_old (user_id, username) VALUES '
                                        '(1, \'henry88\'), (2, \'john88\')')

    postgres_service.execute_and_commit('INSERT INTO test_new (user_id, username) VALUES '
                                        '(1, \'henry88\'), (2, \'john88\')')

    postgres_service.execute_and_commit('INSERT INTO test_new_2 (user_id, username) VALUES '
                                        '(1, \'henry88\'), (2, \'john88\'), (3, \'mross92\')')

    report = ''
    except_dict = dict([('test_new', 0), ('test_new_2', 1)])

    for table_pair in config['tablePairs']:
        old_schema = table_pair['oldTable']['schema']
        old_name = table_pair['oldTable']['tableName']
        new_schema = table_pair['newTable']['schema']
        new_name = table_pair['newTable']['tableName']

        stt = "Tables {}.{} and {}.{} have the same columns".format(old_schema, old_name, new_schema, new_name)

        exc_stt = "\tNumber of different rows (sum of the rows that are in the old table but not in the new one " \
                  "and viceversa): {} \n\n".format(except_dict[new_name])

        report = '\n'.join([report, stt, exc_stt])

    assert compare_tables(config, postgres_service) == report

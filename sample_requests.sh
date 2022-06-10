#!/bin/bash

curl --location --request POST 'localhost:9000/leader' \
--header 'Content-Type: application/json' \
--data-raw '{"read":[{"table_name":"table1", "select_column":"col1","primary_column":"pcol","primary_value":25}], 
"commit": [
    {"table_name":"table1","data":[134,26]}
]}'

curl --location --request POST 'localhost:9000/leader' \
--header 'Content-Type: application/json' \
--data-raw '{"read_only":[{"table_name":"table1","select_column":"col1","primary_column":"pcol", "primary_value": 1},{"table_name":"table1","select_column":"col1","primary_column":"pcol", "primary_value": 2}]}'




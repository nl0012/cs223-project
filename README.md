# cs223-project

## Demo Link
https://youtu.be/o1Q4YeeWaCM

## Dependencies
1. NodeJS 
2. NPM, the package manager for NodeJS
3. PostgreSQL (3 different servers should be started on different ports)

## Getting Started
1. Start 3 different PostgreSQL servers on different ports. Take note of ports.
2. Create a mock database to run experiments on.
3. In the config.json file, replace the configuration parameters with the appropriate ones installed on your local machine (user, password, database name, ports, etc.)
4. cd into project folder and run `npm install` to install all Node dependencies required.
5. run `npm start` to launch the REST API.

## Notes
1. The server is ran on localhost:9000/ 
2. API endpoints may be changed as well for the leader/followers.

## Running Experiments
To test the system, the use of a HTTP Client is recommended, such as Postman. The local servers must also have some sort of shared schema/table. The creation of tables in a replicated system is outside the scope of this project.
1. Using postman, send a POST request to the leader/ with a request following the specified format in the report. 

Example: 
{"read":[{"table_name":"table1", "select_column":"col1","primary_column":"pcol","primary_value":25}], 
"commit": [
    {"table_name":"table1","data":[134,26]}
]}

2. To test read-only transactions, also send a POST request that follows the request format specified in the report. 

Example:
{"read_only":[{"table_name":"table1","select_column":"col1","primary_column":"pcol", "primary_value": 1},{"table_name":"table1","select_column":"col1","primary_column":"pcol", "primary_value": 2}]}

3. To test leader replacement, first send a POST request with {"failed":true} as the body to mark the leader node as failed, next POST a regular request to receive information about the status of the recovery and the answer to the request.

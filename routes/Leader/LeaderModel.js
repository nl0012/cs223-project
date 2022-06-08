const Pool = require('pg').Pool;
const config = require('../../config.json');

const pool = new Pool({
    user: config.user,
    host: config.host,
    database: config.database,
    port: config.leader_port,
});

const executeReads = (read_set) => {
 return new Promise(resolve,reject) {
        pool.query('SELECT',(error,results) => {
            if(error){
            reject(error);
            return;
            }
            resolve(results.rows);
        })
    })
}

const executeCommit = (commit_set) => {
    //start T, do work, check if commit or abort, send back commit or abort
}
const startTransaction = (commit_request) => {
    return new Promise(resolve,reject) {
        pool.query('BEGIN',(error,results) => {
            if(error){
            reject(error);
            return;
            }
            resolve("Transaction Started");
        })
    })
}



const endTransaction = () => {
}



/**
* File name: LeaderModel
* Functionality: This file serves as the model for the Leader Node.
* It contains the functions needed to successfully coordinate a replication system
**/

const Pool = require('pg').Pool;
const config = require('../../config.json');
const axios = require('axios').default;
const otc = require('objects-to-csv');
const fs = require('fs');

const pool = new Pool({
    user: config.user,
    host: config.host,
    database: config.database,
    port: config.leader_port,
});

var transactionId = 0;

/**
*  Function that is called when READ operation(s) has been requested by the client
*  Simply SELECTs the desired object as this guarantees to read the latest committed value
*  Loops through read_set and reads every requested object
**/
const executeReads = (read_set) => {
    let promises = [];
    read_set.forEach((read,key) => {
       const {table_name,select_column,primary_column,primary_value} = read;
         let query = `SELECT ` + select_column + ` FROM ` + table_name + ` WHERE ` + primary_column + ` = $1;`;

            promises.push(new Promise(function(resolve,reject) {
                   pool.query(query,[primary_value],(error,results) => {
                       if(error){
                       reject(error);
                       return;
                       }
                       resolve({"operation": "READ", "results": results.rows});
                   })
           }));
    })
    return promises;
}

const logTransaction = (write_set) => {
    let file_name = 'leader_replication_log.txt';
    console.log(transactionId);
    fs.appendFileSync(file_name, '<T'+ transactionId+', TRANSACTION-START>\n', 'utf-8');
    write_set.forEach((write,key)=> {
       fs.appendFileSync(file_name, '<T'+ transactionId+', OPERATION-BEGIN>\n', 'utf-8');
       //if this is an INSERT
       if('data' in write){
        fs.appendFileSync(file_name,'<T'+ transactionId+', INSERT, '+ write.table_name+ ', '+JSON.stringify(write.data)+'>\n', 'utf-8');
        fs.appendFileSync(file_name, '<T'+ transactionId+', OPERATION-END, <DELETE, '+ write.table_name + ', '+JSON.stringify(write.data)+'>>\n', 'utf-8');
       }
       else{
        //need old value
         fs.appendFileSync(file_name,'<T'+ transactionId+', UPDATE, '+ write.table_name+ ', '+write.update_value+'>\n', 'utf-8');
         fs.appendFileSync(file_name, '<T'+ transactionId+', OPERATION-END, <UPDATE, '+ write.table_name + ', '+write.update_value+'>>\n', 'utf-8');

       }

    })

    fs.appendFileSync(file_name, '<T'+ transactionId+', TRANSACTION-END>\n', 'utf-8');
    transactionId++;
}

const executeCommit = (commit_set) => {
    let promises = [];
    //start T, do work, check if commit or abort, send back commit or abort
    //if commit, reflect the changes on db, send
    return new Promise(function(resolve,reject){
         startTransaction().then((res)=> {
             console.log(res);
          checkIfCommitValid(commit_set).then((prepare_res)=> {
                        console.log(prepare_res);
                        commitTransaction().then(async (commit_res)=> {
                           console.log(commit_res);
                            logTransaction(commit_set);
                            resolve([{'operation':'COMMIT TRANSACTION','results':[commit_set]}]);
                        })
                   }).catch(err => {
                      resolve([{'operation':'ABORT TRANSACTION','results':[commit_set]}]);
                   })
         })
         })
}


const executeWrite = (write_set) => {
let promises = [];
    write_set.forEach((write,key) =>{
          let query = '';
          let values = [];
            //if data key is present in write object, then it is an INSERT
            if ('data' in write){
                let row_length = write['data'].length;
                query+= 'INSERT INTO ' + write.table_name + ' VALUES(';
                for( var i = 1; i <= row_length; i++){
                    query+= '$' + i + ',';
                }
                query = query.slice(0,-1);
                query+= ');';
                values = write['data'];
            }
            // else, it is an UPDATE
            else {
                query+= 'UPDATE '+ write.table_name + ' SET ' + write.update_column + ' = $1 WHERE '
                + write.primary_column + ' = $2 ;';
                values = [write.update_value, write.primary_value];
            }

            promises.push(new Promise(function(resolve,reject) {
                pool.query(query,values,(error,results)=> {
                    if(error){
                    reject(error);
                    return;
                    }

                    resolve(results.rows);
                })
            }))
    })
    return promises;
}


const checkIfCommitValid = (commit_set) => {
    return new Promise(function(resolve,reject){
     Promise.all(executeWrite(commit_set)).then(results =>{resolve("Transaction Ready To Commit");})
        .catch(error => {
            abortTransaction().then((res)=> {reject(res)});
        })
    })

}

const sendCommitToFollowers = () => {

}

const startTransaction = () => {
    return new Promise(function(resolve,reject) {
        pool.query('BEGIN',(error,results) => {
            if(error){
            reject(error);
            return;
            }

            resolve("Transaction Started");
        })
    })
}



const commitTransaction = () => {
 return new Promise(function(resolve,reject) {
        pool.query('COMMIT',(error,results) => {
            if(error){
            reject(error);
            return;
            }
            resolve("Transaction Committed");
        })
    })
}

const abortTransaction = () => {
 return new Promise(function(resolve,reject) {
        pool.query('ROLLBACK',(error,results) => {
            if(error){
            reject(error);
            return;
            }
            resolve("Transaction Aborted");
        })
    })
}


module.exports = {
startTransaction,
executeCommit,
executeReads,
}
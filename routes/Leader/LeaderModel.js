/**
* File name: LeaderModel
* Functionality: This file serves as the model for the Leader Node.
* It contains the functions needed to successfully coordinate a replication system
**/

const Pool = require('pg').Pool;
const config = require('../../config.json');
const axios = require('axios').default;
const fs = require('fs');

//PostgreSQL pool that connects to leader database instance
const pool = new Pool({
    user: config.user,
    password: config.password,
    host: config.host,
    database: config.database,
    port: config.leader_port,
});

//global transaction counter, note this resets when system is shut down
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

/**
* Function to log the transaction that was just committed
* The log follows logical logging style
**/
const logTransaction = (write_set, results) => {
    let file_name = 'leader_replication_log.txt';
    let log_lines = '';
    console.log(transactionId);
    log_lines += '<T'+ transactionId+', TRANSACTION-START>\n';
    write_set.forEach((write,key)=> {
        log_lines += '<T'+ transactionId+', OPERATION-BEGIN>\n';
       //if this is an INSERT
       if('data' in write){
        log_lines +='<T'+ transactionId+', INSERT, '+ write.table_name+ ', '+JSON.stringify(write.data)+'>\n';
         log_lines += '<T'+ transactionId+', OPERATION-END, <DELETE, '+ write.table_name + ', '+JSON.stringify(write.data)+'>>\n';
       }
       else{
        //need old value which we get from the results array, each result maps directly to a write in the write_set at the same index
        let update_old_value = Object.values(results[key][0])[0];
       // console.log(update_old_value);
         log_lines +='<T'+ transactionId+', UPDATE, '+ write.table_name+ ', '+write.update_value+'>\n';
         log_lines += '<T'+ transactionId+', OPERATION-END, <UPDATE, '+ write.table_name + ', '+update_old_value+'>>\n';

       }

    })
         log_lines +=  '<T'+ transactionId+', TRANSACTION-END>\n';
    fs.appendFileSync(file_name, log_lines, 'utf-8');
    transactionId++;
    return log_lines;
}

/**
* Function to perform validation and commit phase
* Starts transaction, calls validation function
* ABORTs if transaction is not valid
* Otherwise COMMIT, log, and send to followers
**/
const executeCommit = (commit_set) => {
    let promises = [];
    //start T, do work, check if commit or abort, send back commit or abort
    //if commit, reflect the changes on db, send
    return new Promise(function(resolve,reject){
         startTransaction().then((res)=> {
             console.log(res);
          checkIfCommitValid(commit_set).then((prepare_res)=> {
                        console.log(prepare_res.message);

                        commitTransaction().then(async (commit_res)=> {
                           console.log(commit_res);
                           //Log transaction to leader log
                            let log_lines = logTransaction(commit_set, prepare_res.result);
                            //send COMMIT log to followers
                            let send_commit = await sendCommitToFollowers(commit_set, log_lines);

                            resolve([{'operation':'COMMIT TRANSACTION','results':[commit_set]}]);
                        })
                   }).catch(err => {
                      resolve([{'operation':'ABORT TRANSACTION','results':[commit_set]}]);
                   })
         })
         })
}

/**
* Function to loop through write set and accordingly parse the write into SQL queries
* If one of the queries does not succeed it will return an error which will be caught in the checkIfCommitValid
* After which the transaction will be ABORTED
**/
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
                //This update query also selects the old value which was overwritten
                //This is important for UNDO logs
                query+= 'UPDATE '+ write.table_name + ' x SET ' + write.update_column + ' = $1 FROM '+ write.table_name + ' x1 WHERE x.'
                + write.primary_column + ' = x1.' + write.primary_column + ' AND x.'+ write.primary_column + ' = $2 RETURNING x1.'+write.update_column+ ';'

                values = [write.update_value, write.primary_value];
            }

            promises.push(new Promise(function(resolve,reject) {
                pool.query(query,values,(error,results)=> {
                    if(error){
                    reject(error);
                    return;
                    }
                    //console.log(results.rows);
                    resolve(results.rows);
                })
            }))
    })
    return promises;
}


const checkIfCommitValid = (commit_set) => {
    return new Promise(function(resolve,reject){
     Promise.all(executeWrite(commit_set)).then(results =>{resolve({"message":"Transaction Ready To Commit", "result":results});})
        .catch(error => {
            abortTransaction().then((res)=> {reject(res)});
        })
    })

}

const sendCommitToFollowers = async (commit_set,log_lines) => {
    const result1 = await axios({
        method: 'post',
        url: config.follower1_url,
        contentType: 'application/json',
        data: {"commit_set":commit_set,"log":log_lines}
        });
   const result2 = await axios({
        method: 'post',
        url: config.follower2_url,
        contentType: 'application/json',
        data: {"commit_set":commit_set,"log":log_lines}
    });
    return {result1,result2};
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

/**
*  Function that is called when READ ONLY transaction has been requested by the client
*  Checks if a WRITE transaction is currently processing, by checking replication log
*  If last line of replication log is NOT a <TRANSACTION-END> this means that the transaction is still processing
*  Delay the reads until it has ended
*  Loops through read_set and reads every requested object
**/
const readOnlyTransaction = (read_set) => {
    let promises = [];
    let filename = 'leader_replication_log.txt';
    var lines = fs.readFileSync(filename, 'utf-8')
        .split('\n')
        .filter(Boolean);
    //if last line is not END then transaction still going, read again until it is
    let flag = lines.length == 0 ? true : lines[lines.length-1].includes('TRANSACTION-END');
    while(!flag) {
        lines = fs.readFileSync(filename, 'utf-8')
                .split('\n')
                .filter(Boolean);
        flag = lines[lines.length-1].includes('TRANSACTION-END');
    }
    //if flag is true, then transaction has ended and we can now perform the reads
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


module.exports = {
startTransaction,
executeCommit,
executeReads,
readOnlyTransaction
}
/**
* File name: FollowerModel
* Functionality: This file serves as the model for the Follower Node.
* It contains the functions needed to successfully coordinate a replication system
**/

const fs = require('fs');

/**
* Function to reflect committed changes from Leader
**/
const executeCommit = (write_set,pool) => {
    return new Promise(function(resolve,reject) {
        Promise.all(executeWrite(write_set,pool)).then(results => {resolve("Write set successfully applied")})
        .catch(error => {
            console.log(error);
        });
    })
}

/**
* Function to loop through write set and accordingly parse the write into SQL queries
**/
const executeWrite = (write_set,pool) => {
console.log(write_set);
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

/**
*  Function that is called when READ ONLY transaction has been requested by the client
*  Checks if a WRITE transaction is currently processing, by checking replication log
*  If last line of replication log is NOT a <TRANSACTION-END> this means that the transaction is still processing
*  Delay the reads until it has ended
*  Loops through read_set and reads every requested object
**/
const readOnlyTransaction = (read_set, follower_id, pool) => {
    let promises = [];
    let filename = follower_id+'_replication_log.txt';
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

/**
* Function to log the transaction that was just committed
* The log follows logical logging style
**/
const logTransaction = (log_lines, follower_id) => {
        let file_name = follower_id+'_replication_log.txt';
      fs.appendFileSync(file_name, log_lines, 'utf-8');
}

module.exports = {
executeCommit,
logTransaction,
readOnlyTransaction,
}
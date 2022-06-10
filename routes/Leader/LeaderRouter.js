/**
* File name: LeaderRouter
* Functionality: This file serves as the startpoint for the Leader API Endpoint.
* It contains the type of HTTP request that the router supports and the functions called within the request.
* The functions called are directly imported from the Leader Model file.
**/

const leaderModel = require('./LeaderModel');
const express = require('express');


var router = express.Router({mergeParams:true});

//flag to indicate if leader node has failed or not
var running = true;

/**
* This is the HTTP POST request that the client calls to post its READ/COMMIT message to along with the transaction operations
* One POST request => one transaction
* For format of req body, please check the report documentation
**/
router.post('/', (req,res) => {
    let promises = [], read = [], commit = [], read_only = [];

    //if client triggers failure of leader node. This is just for testing purposes
    if('failed' in req.body) {

        running = false;
        res.status(200).end();
        return;
    }


    if(!running){

        //res.write('Leader node has failed. Starting Recovery...\n');
        leaderModel.recoveryAlgorithm().then((recov) => {
         res.write('Leader node has failed. Starting Recovery...\n');
          res.write(recov+ '\n');
        }).catch(err => {console.log(err)});
    }

   //parse request body into read-set and write-set or READ-ONLY
   if ('read_only' in req.body) {

        read_only = req.body.read_only;
       Promise.all(leaderModel.readOnlyTransaction(read_only)).then((reads)=> {
                console.log(reads);
                reads.forEach((read,key)=> {
                    res.write(read.operation+ ': '+ JSON.stringify(read.results[0])+'\n');
                })
            res.status(200).end();
            })
    return;
   }
   else {
        read = req.body.read;
        commit = req.body.commit;
   }
   if (read.length > 0){ //if read set non-empty
       //call leader model function to execute reads
       // append response of read request to global responses
       promises.push(Promise.all(leaderModel.executeReads(read)));
   }

   if(commit.length > 0) { //if commit set non-empty
        //call leader model function to start postgreSQL transaction
        // append response of commit request to global responses
        promises.push(
        leaderModel.executeCommit(commit)
        );
   }

    Promise.all(promises).then((responses) => {
     console.log("HEere?");
        responses.forEach((ops, key) => {
            ops.forEach((op_response,key1)=> {
                res.write(op_response.operation + ': ' + JSON.stringify(op_response.results[0]) + '\n');
            })
        })
        res.status(200).end();
    })

})


module.exports = router;
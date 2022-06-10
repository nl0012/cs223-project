/**
* File name: Follower1Router
* Functionality: This file serves as the startpoint for the Follower 1 API Endpoint.
* It contains the type of HTTP request that the router supports and the functions called within the request.
* The functions called are directly imported from the Follower Model file.
**/

const Pool = require('pg').Pool;
const config = require('../../config.json');
const followerModel = require('./FollowerModel');
const express = require('express');

//PostgreSQL pool that connects to follower database instance
const pool = new Pool({
    user: config.user,
    password: config.password,
    host: config.host,
    database: config.database,
    port: config.follower1_port,
});

var router = express.Router({mergeParams:true});

/**
* This is the HTTP POST request that the leader calls to post its COMMIT message to along with the transaction operations
* One POST request => one transaction
* For format of req body, please refer to the report documentation
**/
router.post('/', (req,res) => {
    let promises = [], commit_set=[], read_only = [], log = '';

        //parse request body into write-set and log lines
   if ('read_only' in req.body) {
        read_only = req.body.read_only;
   }
   else {
        commit_set = req.body.commit_set;
        log = req.body.log;
   }
    if(commit_set.length > 0) {
         followerModel.executeCommit(commit_set,pool).then(()=> {
         followerModel.logTransaction(log,'follower1');
         res.status(200).end();
        })
    }

    if(read_only.length > 0){
        Promise.all(followerModel.readOnlyTransaction(read_only,'follower1',pool)).then((reads)=> {
            reads.forEach((read,key)=> {
                res.write(read.operation+ ': '+ JSON.stringify(read.results[0])+'\n');
            })
            res.status(200).end();
        })
    }
})

module.exports = router;
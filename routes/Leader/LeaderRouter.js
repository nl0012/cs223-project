/**
* File name: LeaderRouter
* Functionality: This file serves as the startpoint for the Leader API Endpoint.
* It contains the type of HTTP request that the router supports and the functions called within the request.
* The functions called are directly imported from the Leader Model file.
**/

const leaderModel = require('./LeaderModel');
const express = require('express');
var router = express.Router({mergeParams:true});

/**
* This is the HTTP POST request that the client calls to post its READ/COMMIT message to along with the transaction operations
* One POST request => one transaction
* For format of req body, please check the report documentation
**/
router.post('/', (req,res) => {
    let responses = {};
   //parse request body into read-set and write-set
   const {read,commit} = body;
   if (read.length > 0){ //if read set non-empty
       //call leader model function to execute reads
       leaderModel.executeReads(read).then(response => {
//             res.status(200).send(response);
            responses['read_response'] = response;
       }).catch(error => {
       res.status(500).send(error);
       });
   }

   if(commit.length > 0) { //if commit set non-empty
        //call leader model function to start postgreSQL transaction
        leaderModel.executeCommit(commit).then(response => {
            responses['commit_response'] = response;
        }).catch(error => {
        res.status(500).send(error);
        });
   }

})

module.exports = router;
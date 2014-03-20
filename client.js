var async = require('async');
var readline = require('readline');
var fs = require('fs');
var extend = require('util')._extend;
var moment = require('moment');
var client = require('./lib/client');

//determine the hosts to use
var hosts = fs.readFileSync('hosts.cfg').toString().trim().split('\n');

var hostsToUse = parseInt(process.argv[2]);
if (hostsToUse > hosts.length) {
    console.error('ERROR, number of hosts(' + hostsToUse + ') to use exceeds the number of available hosts (' + hosts.length + ')');
    process.exit();
}

hosts = hosts.slice(0, hostsToUse);

var sockets = [];

var count = 0;

const CHUNK_SIZE = 25;

var start = moment();

//initialize each socket
async.each(hosts, function(host, callback) {

    var hostParts = host.split(':');
    var socket = new client(hostParts[0], hostParts[1], callback);

    sockets.push(socket);

}, function(err) {

    //console.log('All hosts connected');

    var rl = readline.createInterface({
        input: process.stdin,
        output: new require('stream').Writable()
    });

    //function to determine which socket to write a word to based on its first character
    var getSocketIndexToSaveWordTo = function(input) {
        var firstChar = input.toLowerCase().charCodeAt(0) - 97;
        var socketIndex = firstChar % sockets.length;
        return socketIndex;
    }

    var chunks = new Array(sockets.length);

    //if the chunk size exceeds the defined limit then flush it out to the server node
    var chunkHandler = function(index, endOfInput) {

        endOfInput = endOfInput || false;

        if (chunks[index] != null && Object.keys(chunks[index]).length >= CHUNK_SIZE || endOfInput) {
            var dataToSend = extend({}, chunks[index]);
            chunks[index] = {};

            var callback = null;
            if (endOfInput) {
                callback = function (data) {
                    endOfInput(data);
                }
            }

            sockets[index].executeCommand('storeWords', dataToSend, callback);
        }

    }

    //simply function to handle input from the stdin
    rl.on('line', function (word) {

        if (word.length == 0) { //end of input
            //console.log('END OF INPUT');
            rl.close();
            var completed = 0;
			sockets.forEach(function(socket, index) {
                chunkHandler(index, function() {
                    completed++;
                    if (completed == sockets.length) {
                        emitRemoveDupes();
                    }
                });
            });
        } else {
            var socketIndex = getSocketIndexToSaveWordTo(word);
            chunks[socketIndex] = chunks[socketIndex] || {};
            chunks[socketIndex][count] = word;

            chunkHandler(socketIndex);
            count++;
        }
    });

});

//basic function to tell each node to remove duplicates
var hasStartedRemovingDupes = false;
var emitRemoveDupes = function() {
	
	if (hasStartedRemovingDupes) return;
	hasStartedRemovingDupes = true;

    var transmissionEnd = moment();
    var diff = transmissionEnd - start;
    //console.log('TOOK', moment(diff).format('mm:ss'));
    //process.exit();

    async.each(sockets, function(socket, callback) {
        socket.executeCommand('removeDuplicates', null, function() {
            callback();
        });
    }, function(err) {
        //console.log('\n--RESULT--\n');

        retrieveWords(0);
    });
}

//function to retrieve words
var retrieveWords = function(previousLimit) {

    var indexLessThanOrEqualTo = previousLimit + (CHUNK_SIZE * sockets.length);

    var build = [];

    //execute the command on each node
    async.each(sockets, function(socket, callback) {

        socket.executeCommand('retrieveWords', {indexLessThanOrEqualTo: indexLessThanOrEqualTo, indexGreaterThanOrEqualTo: previousLimit}, function(words) {
            build.push(words);
            callback();
        });

    }, function(err) {

        //merge the results
        var objectsMerged = {};
        build.forEach(function(items) {
            objectsMerged = extend(objectsMerged, items);
        });

        for (var i = previousLimit; i <= indexLessThanOrEqualTo; i++) {
            if (objectsMerged[i]) console.log(objectsMerged[i]);
        }

        //continue with the next chunk if there is still data to get
        if (indexLessThanOrEqualTo < count) {
            retrieveWords(indexLessThanOrEqualTo);
        } else {

            //otherwise tell each node to cleanup and exit the client
            async.each(sockets, function(socket, callback) {
                socket.executeCommand('cleanup', null, function(result) {
                    callback();
                });
            }, function(err) {

                var diff = moment() - start;
                console.log('FINISHED', moment(diff).format('mm:ss'));
                process.exit();

            });

        }

    });

}
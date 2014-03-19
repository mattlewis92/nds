var async = require('async');
var readline = require('readline');
var fs = require('fs');
var extend = require('util')._extend;
var moment = require('moment');
var client = require('./lib/client');

var hosts = fs.readFileSync('hosts.cfg').toString().split('\n');

var sockets = [];

var count = 0;

const CHUNK_SIZE = 30;

var start = moment();

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

    var getSocketIndexToSaveWordTo = function(input) {
        var firstChar = input.toLowerCase().charCodeAt(0) - 97;
        var socketIndex = firstChar % sockets.length;
        return socketIndex;
    }

    var chunks = new Array(sockets.length);

    var chunkHandler = function(index, endOfInput) {

        endOfInput = endOfInput || false;

        if (Object.keys(chunks[index]).length >= CHUNK_SIZE || endOfInput) {
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

        retrieveWords();
    });
}

var retrieveWords = function() {

    var socketsEnded = 0;

    var buildHelper = {};

    sockets.forEach(function(socket) {

        var count = 0;

        socket.executeCommand('retrieveWords', {chunkSize: 2});
        socket.streamResponse(function(data) {
            count++;
            if (data.endOfData) {
                socketsEnded++;

                if (socketsEnded == sockets.length) {
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

            } else {

                if (buildHelper[count] == null) buildHelper[count] = [];

                buildHelper[count].push(data);

                if (buildHelper[count].length == sockets.length) {
                    var buildObj = [];
                    buildHelper[count].forEach(function(obj) {
                        for(var i in obj) {
                            buildObj[i] = obj[i];
                        }
                    });
                    buildObj.forEach(function(word) {
                        console.log(word);
                    });
                    //delete buildHelper[count];
                }

            }

        });


    });

}

var retrieveWordsOld = function(previousLimit) {

    var indexLessThanOrEqualTo = previousLimit + (CHUNK_SIZE * sockets.length);

    var build = [];

    async.each(sockets, function(socket, callback) {

        socket.executeCommand('retrieveWords', {indexLessThanOrEqualTo: indexLessThanOrEqualTo, indexGreaterThanOrEqualTo: previousLimit}, function(words) {
            build.push(words);
            callback();
        });

    }, function(err) {

        var objectsMerged = {};
        build.forEach(function(items) {
            objectsMerged = extend(objectsMerged, items);
        });

        for (var i = previousLimit; i <= indexLessThanOrEqualTo; i++) {
            if (objectsMerged[i]) console.log(objectsMerged[i]);
        }

        if (indexLessThanOrEqualTo < count) {
            retrieveWords(indexLessThanOrEqualTo);
        } else {

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
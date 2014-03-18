var io = require('socket.io-client');
var async = require('async');
var readline = require('readline');
var fs = require('fs');
var extend = require('util')._extend;
var moment = require('moment');

var hosts = fs.readFileSync('hosts.cfg').toString().split('\n');

var sockets = [];

var count = 0;

const CHUNK_SIZE = 20;

var start = moment();

async.forEach(hosts, function(host, callback) {

    var socket = io.connect(host);
    var called = false;

    socket.on('connect', function() {

        socket.on('disconnect', function() {
            console.log('HOST ' + host + ' DISCONNECTED');
        });

        if (!called) callback();
        called = true;
    });

    sockets.push(socket);

}, function(err) {

    console.log('All hosts connected');

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

            sockets[index].emit('storeWords', dataToSend, callback);
        }

    }

    rl.on('line', function (word) {

        if (word.length == 0) { //end of input
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

    async.forEach(sockets, function(socket, callback) {
        socket.emit('removeDuplicates', null, function() {
            callback();
        });
    }, function(err) {
        console.log('\n--RESULT--\n');
        retrieveWords(0);
    });
}

var retrieveWords = function(previousLimit) {

    var indexLessThanOrEqualTo = previousLimit + (CHUNK_SIZE * sockets.length);

    var build = [];

    async.forEach(sockets, function(socket, callback) {

        socket.emit('retrieveWords', {indexLessThanOrEqualTo: indexLessThanOrEqualTo, indexGreaterThanOrEqualTo: previousLimit}, function(words) {
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
            var diff = moment() - start;
            console.log('FINISHED', moment(diff).format('mm:ss'));
            process.exit();
        }

    });

}
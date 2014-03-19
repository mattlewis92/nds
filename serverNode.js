var port = parseInt(process.argv[2]);
var os = require('os');
var async = require('async');
var fs = require('fs.extra');
var extend = require('util')._extend;
var server = require('./lib/server')('127.0.0.1', port);

var dbPath = './data/' + port;

if (fs.existsSync(dbPath)) {
    fs.rmrfSync(dbPath);
}

fs.mkdirSync(dbPath);

var allowedMemorySize = os.totalmem() / 8;
//allowedMemorySize = 1000;

var memoryUsageGuess = 0;

var words = {};

var result = {};

var filesWritten = 0;

var checkMemoryUsage = function() {
    if (memoryUsageGuess > allowedMemorySize) {
        memoryUsageGuess = 0;
        var wordsBackup = extend({}, words);
        words = {};
        filesWritten++;

        fs.writeFile(dbPath + '/words' + filesWritten, 'module.exports=' + JSON.stringify(wordsBackup) + ';', function(err) {
            if(err) {
                console.log(err);
            } else {
                console.log("The file was saved!");
            }
        });

    }
}

server.registerCommand('storeWords', function(chunkOfWords, callback) {

    words = extend(words, chunkOfWords);

    for (var i in chunkOfWords) {
        memoryUsageGuess += (i + chunkOfWords[i]).length;
    }

    checkMemoryUsage();

    callback('saved');

});

server.registerCommand('removeDuplicates', function(data, callback) {

    var mapReduceFn = function(obj) {
        var keys = Object.keys(obj);
        var build = {};
        keys.forEach(function(key) {
            var word = obj[key];
            if (!build[word]) {
                build[word] = key;
            } else if(parseInt(build[word]) > parseInt(key)) {
                build[word] = key;
            }
        });

        var keys = Object.keys(build);
        var result = {};
        keys.forEach(function(key) {
            result[build[key]] = key;
        });

        return result;
    }

    if (filesWritten == 0) {

        result = mapReduceFn(words);
        words = null;
        callback();

    } else {
        //TODO
    }

});

server.registerCommand('retrieveWords', function(data, callback) {

    var chunkSize = data.chunkSize;
    var maxId = Object.keys(result).pop();

    var chunks = new Array(Math.ceil(maxId / chunkSize));
    var chunksAdded = 0;
    var buildChunk = {};
    for (var i in result) {

        var bottomThreshold = chunksAdded * chunkSize;
        var topThreshold = bottomThreshold + chunkSize;

        while (parseInt(i) >= topThreshold) {
            chunks[chunksAdded] = extend({}, buildChunk);
            buildChunk = {};
            chunksAdded++;

            bottomThreshold = chunksAdded * chunkSize;
            topThreshold = bottomThreshold + chunkSize;
        }

        buildChunk[i] = result[i];
    }
    chunks[chunksAdded] = buildChunk;
    chunks.reverse();

    if (chunks[chunks.length - 1] == null) chunks.pop();

    result = null;

    var sendChunk = function() {

        var response = chunks.pop();

        if (chunks.length > 0) {
            server.writeToStream(response, sendChunk);
        } else {
            server.writeToStream(response);
            server.writeToStream({endOfData: true});
        }

        response = null;

    };

    sendChunk();
});

server.registerCommand('cleanup', function(data, callback) {
    memoryUsageGuess = 0;
    filesWritten = 0;
    words = {};
    result = {};
    callback();
});
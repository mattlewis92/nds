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

        fs.writeFile(dbPath + '/words' + filesWritten + '.json', JSON.stringify(wordsBackup), function(err) {
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
            var lineNumber = build[word];
            if (!lineNumber) {
                build[word] = key;
            } else if(parseInt(lineNumber) > parseInt(key)) {
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

var chunkObject = function(obj, chunkSize) {

    var maxId = Object.keys(result).pop();

    var chunks = new Array(Math.ceil(maxId / chunkSize));
    var chunksAdded = 0;
    var buildChunk = {};
    for (var i in obj) {

        var bottomThreshold = chunksAdded * chunkSize;
        var topThreshold = bottomThreshold + chunkSize;

        while (parseInt(i) >= topThreshold) {
            chunks[chunksAdded] = extend({}, buildChunk);
            buildChunk = {};
            chunksAdded++;

            bottomThreshold = chunksAdded * chunkSize;
            topThreshold = bottomThreshold + chunkSize;
        }

        buildChunk[i] = obj[i];
    }
    chunks[chunksAdded] = buildChunk;

    return chunks;
}

var chunkedResult = null;

server.registerCommand('retrieveWords', function(data, callback) {

    var chunkSize = data.indexLessThanOrEqualTo - data.indexGreaterThanOrEqualTo;
    if (chunkedResult == null) {
        chunkedResult = chunkObject(result, chunkSize);
        result = {};
    }

    var chunkIndexToSend = data.indexGreaterThanOrEqualTo / chunkSize;

    callback(chunkedResult[chunkIndexToSend]);

    chunkedResult[chunkIndexToSend] = null;

});

server.registerCommand('cleanup', function(data, callback) {
    memoryUsageGuess = 0;
    filesWritten = 0;
    words = {};
    chunkedResult = null;
    result = {};
    callback();
});
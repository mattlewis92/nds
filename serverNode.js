var port = parseInt(process.argv[2]);
var io = require('socket.io').listen(port, { log: false });
var find = require('findit');

var Hashids = require('hashids'),
    hashids = new Hashids('networks and distributed systems', 20, '0123456789abcdefghijklmnopqrstuvwxyz');

var fs = require('fs.extra');

var dbPath = './data/' + port;

if (fs.existsSync(dbPath)) {
    fs.rmrfSync(dbPath);
}

fs.mkdirSync(dbPath);

console.log('Server listening on', port);

var hashWord = function(word) {
    var characterIds = [];
    for(var i = 0; i < word.length; i++) {
        characterIds.push(word.charCodeAt(i));
    }
    return hashids.encrypt(characterIds);
}

var dehashWord = function(hash) {
    var numbers = hashids.decrypt(hash);
    var word = '';
    numbers.forEach(function(number) {
        word += String.fromCharCode(number);
    });
    return word;
}

io.sockets.on('connection', function (socket) {

    console.log('Connection received');

    var highestIndex = -1;

    socket.on('storeWord', function (data, fn) {

        if (data.index > highestIndex) highestIndex = data.index;

        var indexPath = (data.index + '').split('').join('/');

		console.log('STORING', data.index, data.word);

        var wordPath = dbPath + '/index/' + indexPath + '/word/' + hashWord(data.word);
        fs.mkdirp(wordPath, function(err) {
            if (err) {
                console.log('ERROR creating word', err);
            } else {
                fn('saved');
            }
        });

    });

    socket.on('removeDuplicates', function(data, fn) {

        var deleteIfExists = function(indexPath, word) {
            console.log('DELETE IF EXISTS', indexPath, word);
            fs.exists(indexPath + 'word/' + word, function (exists) {

                if (exists) {
                    fs.rmrf(indexPath, function (err) {});
                }

            });
        }

        if (highestIndex > -1) {
            var finder = find(dbPath);

            finder.on('directory', function (dir, stat, stop) {

                if (dir.indexOf('/word/') > -1) { //bottom level directory
                    var fileParts = dir.split('/');
                    var word = fileParts.pop();

                    var index = parseInt(dir.match(/\/index\/([\/\d]+)\//)[1].replace(/\//g, ''));

                    var i = (index + 1);

                    while(i <= highestIndex) {
                        deleteIfExists(dbPath + '/index/' + (i + '').split('').join('/') + '/', word);
                        i++;
                    }
                }

            });

            finder.on('end', function() {
                fn();
            });

        } else {
            fn();
        }

    });

    socket.on('retrieveWord', function(index, fn) {

        fs.readdir(dbPath + '/index/' + (index + '').split('').join('/') + '/word', function(err, files) {
            if (err || files.length == 0) {
                fn(null);
            } else {
                var word = dehashWord(files[0].split('/').pop());
                //fs.rmrf(dbPath + '/' + index, function(err) {});
                fn(word);
            }
        });

    });

});
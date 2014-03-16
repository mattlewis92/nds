var port = parseInt(process.argv[2]);
var io = require('socket.io').listen(port, { log: false });
var find = require('findit');

var env = process.env.NDS_ENV || 'development';
var config = require('./config/' + env);

var fs = require('fs.extra');

var dbPath = config.dbBaseDir + '/' + port;

if (fs.existsSync(dbPath)) {
    fs.rmrfSync(dbPath);
}

fs.mkdirSync(dbPath);

console.log('Server listening on', port);

io.sockets.on('connection', function (socket) {

    console.log('Connection received');

    var highestIndex = -1;

    socket.on('storeWord', function (data, fn) {

        if (data.index > highestIndex) highestIndex = data.index;

        var indexPath = dbPath + '/' + data.index;
        fs.mkdir(indexPath, function(err) {
            if (err) {
                console.log('ERROR creating index folder', err);
            } else {

                fs.open(indexPath + '/' + data.word, 'w', function(err, fd) {
                    if (err) console.log('ERROR SAVING FILE', err);
                    fs.close(fd);
                    fn('saved');
                });

            }
        });

    });

    socket.on('removeDuplicates', function(data, fn) {

        if (highestIndex > -1) {
            var finder = find(dbPath);

            finder.on('file', function (file, stat) {
                var fileParts = file.replace(dbPath + '/', '').split('/');
                var index = parseInt(fileParts[0]);
                var word = fileParts[1];

                var i = (index + 1);

                while(i <= highestIndex) {

                    var indexPath = dbPath + '/' + i + '/';

                    fs.exists(indexPath + word, function (exists) {

                        if (exists) {
                            fs.rmrf(indexPath, function (err) {});
                        }

                    });

                    i++;
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

        fs.readdir(dbPath + '/' + index, function(err, files) {
            if (err || files.length == 0) {
                fn(null);
            } else {
                var word = files[0].split('/').pop();
                fs.rmrf(dbPath + '/' + index, function(err) {});
                fn(word);
            }
        });

    });

});
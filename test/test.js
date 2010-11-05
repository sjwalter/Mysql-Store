var Store = require('../lib/mysql-store').MysqlStore,
    config = require('./test-config');

var s = new Store({dbUser: 'qfile', dbPassword: 'qfile', dbHost: 'localhost'}),
    testObj = {foo: {bar: 'baz', thing: 100}, boo: true};

s.init(function(){
    s.set('foobar', JSON.stringify(testObj), function() {
        s.get('foobar', function(err, data) {
            if(data.foo.bar == testObj.foo.bar) {
                console.log('Successfully retrieved object');
            } else {
                console.log('Failed to retrieve object:' + JSON.stringify(data));
                process.exit(-1);
            }
            s.length(function(count) {
                if(count == 1) {
                    console.log('Count is correct');
                } else {
                    console.log('Count is incorrect: ' + count);
                    process.exit(-1);
                }
                s.destroy('foobar', function() {
                    s.length(function(count) {
                        if(count == 0) {
                            console.log('Count is correct');
                        } else {
                            console.log('Count is incorrect: ' + count);
                        }
                        process.exit(0);
                    });
                });
            });
        });
    });
});

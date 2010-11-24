var sys = require('sys'),
    Store = require('connect').session.Store, 
    MysqlClient = require('mysql').Client;

var MysqlStore = function(options) {
    var self = this;
    
    options = options || {};

    if(!options.dbUser || !options.dbPassword || !options.dbHost) {
        throw new Error('Must provide user, password, and host for db.');
    }

    Store.call(this, options);

    // Default reapInterval is 10 mins
    this.reapInterval = options.reapInterval || 600000;
    this.database = options.database || 'sessionstore';
    this.table = options.table || 'sessions';
   
    // Set up the mysql client
    this.dbOpts = {
        host: options.dbHost,
        user: options.dbUser,
        password: options.dbPassword
    };
    if(options.port) {
        dbOpts.port = options.port;
    }
};

sys.inherits(MysqlStore, Store);
exports.MysqlStore = MysqlStore;

/**
 * Initialize the database for storage
 *
 * @param {Function} fn
 */
MysqlStore.prototype.init = function(fn) {
    var self = this;
    
    this.client = new MysqlClient(this.dbOpts);
    this.client.connect();
    
    // Configure our tables
    this.client.query('CREATE DATABASE IF NOT EXISTS `' + this.database + '`;', function(err) {
        if(err && err.number != MysqlClient.ERROR_DB_CREATE_EXISTS) {
            throw err;
        } 
        
        self.client.query('USE `' + self.database + '`;', function(err) {
            if(err) {
                throw err;
            }
            // Database exists now, create the table
            self.client.query('CREATE TABLE IF NOT EXISTS ' + self.table +
                '(sid varchar(64) DEFAULT NULL, sess LONGTEXT DEFAULT NULL, PRIMARY KEY (`sid`));',
                function(err) {
                    if(err) {
                        throw err;
                    }
                    if(self.reapInterval !== -1) {
                        setInterval(function(self) {
                            self.reap(self.maxAge);
                        }, self.reapInterval, self);
                    }
                    fn();
                }
            );
        });
    });
};

/**
 * Get a session identified by sid
 *
 * @param {String} sid
 * @param {Function} fn
 */
MysqlStore.prototype.get = function(sid, fn) {
    var self = this;
    function _get() {
        self.client.query('SELECT sess from `' + self.table + '` where `sid`=\'' + sid + '\';',
            function(err, results) {
                if(err) {
                    fn && fn(err);
                    return;
                }
                if(!results || !results[0]) {
                    fn();
                    return;
                }
                
                var data = results[0].sess;
                
                try {
                    data = JSON.parse(data);
                } catch(err) {
                    // Wasn't a JSON block. Oh well.
                }
                typeof fn == 'function' && fn(null, data);
            }
        );
    }
    
    if(!self.client) {
        self.init(_get);
    } else {
        _get();
    }
};

/**
 * Set a session
 *
 * @param {String} sid
 * @param {Mixed} sess
 * @param {Function} fn
 */
MysqlStore.prototype.set = function(sid, sess, fn) {
    var self = this;
    
    function _set() {
        if(typeof sess !== 'string') {
            sess = JSON.stringify(sess);
        }
        
        self.client.query('INSERT INTO `' + self.table + '` (sid, sess) VALUES (?, ?) ON DUPLICATE KEY UPDATE sess=?', [sid, sess, sess], 
            function(err) {
                if(err) {
                    fn && fn(err);
                }
                fn && fn();
            }
        );
    }

    if(!self.client) {
        self.init(_set);
    } else {
        _set();
    }
};

/**
 * Get the number of sessions stored.
 *
 * @param {Function} fn
 */
MysqlStore.prototype.length = function(fn) {
    var self = this;

    function _length() {
        self.client.query('SELECT COUNT(sid) AS count FROM `' + self.table + '`;', 
            function(err, results) {
                if(!results || !results[0]) {
                    fn(new Error(err));
                }
                fn(results[0].count);
            }
        );
    }

    if(!self.client) {
        self.init(_length);
    } else {
        _length();
    }
};

/**
 * Clear all sessions
 *
 * @param {Function} fn
 */
MysqlStore.prototype.clear = function(fn) {
    var self = this;

    function _clear() {
        self.client.query('DELETE FROM `' + this.table +'`;', function(err) {
            fn(err);
        });
    }

    if(!self.client) {
        self.init(_clear);
    } else {
        _clear();
    }
};

/**
 * Reap old sessions
 *
 * @param {number} maxAge: The oldest age of a session
 */
MysqlStore.prototype.reap = function(maxAge) {
    //TODO: Fill in
};

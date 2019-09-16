const mysql = require('mysql');
const util = require('util');
const EventEmitter = require('events').EventEmitter;
const initBinlogClass = require('./lib/sequence/binlog');

const ConnectionConfigMap = {
  'Connection': obj => obj.config,
  'Pool': obj => obj.config.connectionConfig,
};

const TableInfoQueryTemplate = 'SELECT ' +
  'COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME, ' +
  'COLUMN_COMMENT, COLUMN_TYPE ' +
  'FROM information_schema.columns ' + "WHERE table_schema='%s' AND table_name='%s'" + 
  ' ORDER BY ORDINAL_POSITION';

function ZongJi(dsn, options) {
  this.set(options);

  EventEmitter.call(this);

  this.ctrlCallbacks = [];
  this.tableMap = {};
  this.ready = false;
  this.useChecksum = false;
  this.grouping = false;
  this.groupCommit = undefined;

  this._establishConnection(dsn);
  this._init();
}

util.inherits(ZongJi, EventEmitter);

// dsn - can be one instance of Connection or Pool / object / url string
ZongJi.prototype._establishConnection = function(dsn) {
  let binlogDsn;
  let configFunc = ConnectionConfigMap[dsn.constructor.name];

  if (typeof dsn === 'object' && configFunc) {
    let conn = dsn;
    // reuse as ctrlConnection
    this.ctrlConnection = conn;
    this.ctrlConnectionOwner = false;
    binlogDsn = Object.assign({}, configFunc(conn));
  }

  let createConnection = (options) => {
    let connection = mysql.createConnection(options);
    connection.on('error', this.emit.bind(this, 'error'));
    connection.on('unhandledError', this.emit.bind(this, 'error'));
    // Prevents idle connection
    setInterval(function () {
      connection.query('SELECT 1');
    }, 10000);
    // don't need to call connection.connect() here
    // we use implicitly established connection
    // see https://github.com/mysqljs/mysql#establishing-connections
    return connection;
  };

  if (!binlogDsn) {
    // assuming that the object passed is the connection settings
    this.ctrlConnectionOwner = true;
    this.ctrlConnection = createConnection(dsn);
    binlogDsn = dsn;
  }

  this.connection = createConnection(binlogDsn);
};

ZongJi.prototype._init = function() {

  let ready = () => {
    this.BinlogClass = initBinlogClass(this);
    this.ready = true;
    this.emit('ready');
    this._executeCtrlCallbacks();
  };

  let testChecksum = new Promise((resolve, reject) => {
    this._isChecksumEnabled((err, checksumEnabled) => {
      if (err) {
        reject(err);
      }
      else {
        this.useChecksum = checksumEnabled;
        resolve();
      }
    });
  });

  let findBinlogEnd = new Promise((resolve, reject) => {
    this._findBinlogEnd((err, result) => {
      if (err) {
        return reject(err);
      }

      if (result && this.options.startAtEnd) {
        Object.assign(this.options, {
          filename: result.Log_name,
          position: result.File_size,
        });
      }

      resolve();
    });
  });

  Promise.all([testChecksum, findBinlogEnd])
    .then(ready)
    .catch(err => {
      this.emit('error', err);
    });
};

ZongJi.prototype._isChecksumEnabled = function(next) {
  const SelectChecksumParamSql = 'select @@GLOBAL.binlog_checksum as checksum';
  const SetChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';

  let query = (conn, sql) => {
    return new Promise(
      (resolve, reject) => {
        conn.query(sql, (err, result) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(result);
          }
        });
      }
    );
  };

  let checksumEnabled = true;

  query(this.ctrlConnection, SelectChecksumParamSql)
    .then(rows => {
      if (rows[0].checksum === 'NONE') {
        checksumEnabled = false;
      }

      if (checksumEnabled) {
        return query(this.connection, SetChecksumSql);
      }
    })
    .catch(err => {
      if (err.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)) {
        checksumEnabled = false;
        // a simple query to open this.connection
        return query(this.connection, 'SELECT 1');
      }
      else {
        next(err);
      }
    })
    .then(() => {
      next(null, checksumEnabled);
    });
};

ZongJi.prototype._findBinlogEnd = function(next) {
  this.ctrlConnection.query('SHOW BINARY LOGS', function(err, rows) {
    if (err) {
      // Errors should be emitted
      next(err);
    }
    else {
      next(null, rows.length > 0 ? rows[rows.length - 1] : null);
    }
  });
};

ZongJi.prototype._executeCtrlCallbacks = function() {
  if (this.ctrlCallbacks.length > 0) {
    this.ctrlCallbacks.forEach(function(cb) {
      setImmediate(cb);
    });
  }
};

ZongJi.prototype._fetchTableInfo = function(tableMapEvent, next) {
  var self = this;
  var sql = util.format(TableInfoQueryTemplate,
    tableMapEvent.schemaName, tableMapEvent.tableName);

  this.ctrlConnection.query(sql, function(err, rows) {
    if (err) {
      // Errors should be emitted
      self.emit('error', err);
      // This is a fatal error, no additional binlog events will be
      // processed since next() will never be called
      return;
    }

    if (rows.length === 0) {
      self.emit('error', new Error(
        'Insufficient permissions to access: ' +
        tableMapEvent.schemaName + '.' + tableMapEvent.tableName));
      // This is a fatal error, no additional binlog events will be
      // processed since next() will never be called
      return next();
    }

    self.tableMap[tableMapEvent.tableId] = {
      columnSchemas: rows,
      parentSchema: tableMapEvent.schemaName,
      tableName: tableMapEvent.tableName
    };

    next();
  });
};

const AVAILABLE_OPTIONS = [
  'includeEvents',
  'excludeEvents',
  'includeSchema',
  'excludeSchema',
  'serverId',
  'filename',
  'position',
  'startAtEnd',
];

ZongJi.prototype.set = function(options = {}) {
  this.options = {};

  for (const key of AVAILABLE_OPTIONS) {
    if (options[key]) {
      this.options[key] = options[key];
    }
  }
};

ZongJi.prototype.get = function(name) {
  let result;
  if (typeof name === 'string') {
    result = this.options[name];
  }
  else if (Array.isArray(name)) {
    result = name.reduce(
      (acc, cur) => {
        acc[cur] = this.options[cur];
        return acc;
      },
      {}
    );
  }

  return result;
};

ZongJi.prototype.start = function(options) {
  var self = this;
  self.set(options);

  var _start = function() {
    self.connection._protocol._enqueue(new self.BinlogClass(function(error, event) {
      if (error) return self.emit('error', error);
      // Do not emit events that have been filtered out
      if (event === undefined || event._filtered === true) return;
      switch (event.getTypeName().toLowerCase()) {
        case 'tablemap':
          var tableMap = self.tableMap[event.tableId];
          if (!tableMap) {
            self.connection.pause();
            self._fetchTableInfo(event, function() {
              // merge the column info with metadata
              event.updateColumnInfo();
              self.emit('binlog', event);
              self.connection.resume();
            });
            // if table map contains mv_delivery and task then start grouping
            const mappedTablesObject = event.tableMap;
            let mappedTables = [];
            Object.keys(mappedTablesObject).forEach((key) => {
              const table = mappedTablesObject[key];
              mappedTables.push(table.tableName);
            });
            if (mappedTables.includes('task')){
              this.grouping = true;
            }
            return;
          }          
          // if table map contains mv_delivery and task then start grouping
          const mappedTablesObject = event.tableMap;
          let mappedTables = [];
          Object.keys(mappedTablesObject).forEach((key) => {
            const table = mappedTablesObject[key];
            mappedTables.push(table.tableName);
          });
          if (mappedTables.includes('task')){
            this.grouping = true;
          }
          break;
        case 'rotate':
          if (self.options.filename !== event.binlogName) {
            self.options.filename = event.binlogName;
          }
          break;
        case 'xid': 
          if (this.grouping){
            this.grouping = false;
            if (this.groupCommit){
              Object.keys(this.groupCommit).forEach((key) => {
                const event = this.groupCommit[key];
                self.emit('binlog', event);
              });
              this.groupCommit = undefined;              
            }
          }
          break;
        case 'writerows':
        case 'updaterows':
        case 'deleterows':
          if (this.grouping){
            if (!this.groupCommit){
              this.groupCommit = {};
            }
            const tableName = event.tableId ? event.tableMap[event.tableId].tableName: undefined;
            if (!tableName){
              self.options.position = event.nextPosition;
              self.emit('binlog', event);
              return;
            }
            if (!this.groupCommit[tableName]){  // if grouping have not started
              this.groupCommit[tableName] = event
            } else { // if grouping is already started
              event.rows.map((row)=>{
                this.groupCommit[tableName].rows.push(row); 
              })
              this.groupCommit[tableName].timestamp = event.timestamp;
              this.groupCommit[tableName].nextPosition = event.nextPosition;
            }
            self.options.position = event.nextPosition;
            return;  
          }
          break;
      }
      self.options.position = event.nextPosition;
      self.emit('binlog', event);
    }));
  };

  if (this.ready) {
    _start();
  }
  else {
    this.ctrlCallbacks.push(_start);
  }
};

ZongJi.prototype.stop = function() {
  var self = this;
  // Binary log connection does not end with destroy()
  self.connection.destroy();
  self.ctrlConnection.query(
    'KILL ' + self.connection.threadId,
    function() {
      if (self.ctrlConnectionOwner)
        self.ctrlConnection.destroy();
    }
  );
};

ZongJi.prototype._skipEvent = function(eventName) {
  var include = this.get('includeEvents');
  var exclude = this.get('excludeEvents');
  return !(
   (include === undefined ||
    (include instanceof Array && include.indexOf(eventName) !== -1)) &&
   (exclude === undefined ||
    (exclude instanceof Array && exclude.indexOf(eventName) === -1)));
};

ZongJi.prototype._skipSchema = function(database, table) {
  var include = this.get('includeSchema');
  var exclude = this.get('excludeSchema');

  return !(
   (include === undefined ||
    (database !== undefined && (database in include) &&
     (include[database] === true ||
      (include[database] instanceof Array &&
       include[database].indexOf(table) !== -1)))) &&
   (exclude === undefined ||
      (database !== undefined &&
       (!(database in exclude) ||
        (exclude[database] !== true &&
          (exclude[database] instanceof Array &&
           exclude[database].indexOf(table) === -1))))));
};

module.exports = ZongJi;

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
  'FROM information_schema.columns ' + "WHERE table_schema='%s' AND table_name='%s' ORDER BY ORDINAL_POSITION";

function ZongJi(dsn) {
  EventEmitter.call(this);

  this._options({});
  this._filters({});
  this.ctrlCallbacks = [];
  this.tableMap = {};
  this.ready = false;
  this.useChecksum = false;

  this._establishConnection(dsn);
  this.grouping = false;
  this.groupCommit = undefined;
}

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype._executeCtrlCallbacks = function() {
  if (this.ctrlCallbacks.length > 0) {
    this.ctrlCallbacks.forEach(function(cb) {
      setImmediate(cb);
    });
  }
};

// dsn - can be one instance of Connection or Pool / object / url string
ZongJi.prototype._establishConnection = function(dsn) {
  const createConnection = (options) => {
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

  const configFunc = ConnectionConfigMap[dsn.constructor.name];
  let binlogDsn;

  if (typeof dsn === 'object' && configFunc) {
    // dsn is a pool or connection object
    let conn = dsn; // reuse as ctrlConnection
    this.ctrlConnection = conn;
    this.ctrlConnectionOwner = false;
    binlogDsn = Object.assign({}, configFunc(conn));
  }

  if (!binlogDsn) {
    // assuming that the object passed is the connection settings
    this.ctrlConnectionOwner = true;
    this.ctrlConnection = createConnection(dsn);
    binlogDsn = dsn;
  }

  this.connection = createConnection(binlogDsn);
};

ZongJi.prototype._isChecksumEnabled = function(next) {
  const SelectChecksumParamSql = 'select @@GLOBAL.binlog_checksum as checksum';
  const SetChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';

  const query = (conn, sql) => {
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
  this.ctrlConnection.query('SHOW BINARY LOGS', (err, rows) => {
    if (err) {
      // Errors should be emitted
      next(err);
    }
    else {
      next(null, rows.length > 0 ? rows[rows.length - 1] : null);
    }
  });
};

ZongJi.prototype._fetchTableInfo = function(tableMapEvent, next) {
  const sql = util.format(TableInfoQueryTemplate,
    tableMapEvent.schemaName, tableMapEvent.tableName);

  this.ctrlConnection.query(sql, (err, rows) => {
    if (err) {
      // Errors should be emitted
      this.emit('error', err);
      // This is a fatal error, no additional binlog events will be
      // processed since next() will never be called
      return;
    }

    if (rows.length === 0) {
      this.emit('error', new Error(
        'Insufficient permissions to access: ' +
        tableMapEvent.schemaName + '.' + tableMapEvent.tableName));
      // This is a fatal error, no additional binlog events will be
      // processed since next() will never be called
      return;
    }

    this.tableMap[tableMapEvent.tableId] = {
      columnSchemas: rows,
      parentSchema: tableMapEvent.schemaName,
      tableName: tableMapEvent.tableName
    };

    next();
  });
};

// #_options will reset all the options.
ZongJi.prototype._options = function({
  serverId,
  filename,
  position,
  startAtEnd,
}) {
  this.options = {
    serverId,
    filename,
    position,
    startAtEnd,
  };
};

// #_filters will reset all the filters.
ZongJi.prototype._filters = function({
  includeEvents,
  excludeEvents,
  includeSchema,
  excludeSchema,
}) {
  this.filters = {
    includeEvents,
    excludeEvents,
    includeSchema,
    excludeSchema,
  };
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

// @options contains a list options
// - `serverId` unique identifier
// - `filename`, `position` the position of binlog to beigin with
// - `startAtEnd` if true, will update filename / postion automatically
// - `includeEvents`, `excludeEvents`, `includeSchema`, `exludeSchema` filter different binlog events bubbling
ZongJi.prototype.start = function(options = {}) {

  this._options(options);
  this._filters(options);

  const testChecksum = (resolve, reject) => {
    this._isChecksumEnabled((err, checksumEnabled) => {
      if (err) {
        reject(err);
      }
      else {
        this.useChecksum = checksumEnabled;
        resolve();
      }
    });
  };


  const findBinlogEnd = (resolve, reject) => {
    this._findBinlogEnd((err, result) => {
      if (err) {
        return reject(err);
      }

      if (result) {
        this._options(
          Object.assign({}, options, {
            filename: result.Log_name,
            position: result.File_size,
          })
        );
      }

      resolve();
    });
  };

  const binlogHandler = (error, event) => {
    if (error) {
      return this.emit('error', error);
    }

    // Do not emit events that have been filtered out
    if (event === undefined || event._filtered === true) return;
    console.log('eventreceived', event.getTypeName());
    switch (event.getTypeName().toLowerCase()) {
      case 'tablemap': {
        const tableMap = this.tableMap[event.tableId];
        if (!tableMap) {
          this.connection.pause();
          this._fetchTableInfo(event, () => {
            // merge the column info with metadata
            event.updateColumnInfo();
            this.emit('binlog', event);
            this.connection.resume();
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
      }
      case 'rotate':
        if (this.options.filename !== event.binlogName) {
          this.options.filename = event.binlogName;
        }
        break;
      case 'xid': 
        if (this.grouping){
          this.grouping = false;
          if (this.groupCommit){
            Object.keys(this.groupCommit).forEach((key) => {
              const event = this.groupCommit[key];
              this.emit('binlog', event);
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
            this.options.position = event.nextPosition;
            this.emit('binlog', event);
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
          this.options.position = event.nextPosition;
          return;  
        }
        break;
    }
    this.options.position = event.nextPosition;
    this.emit('binlog', event);
  };

  let promises = [new Promise(testChecksum)];

  if (this.options.startAtEnd) {
    promises.push(new Promise(findBinlogEnd));
  }

  Promise.all(promises)
    .then(() => {
      this.BinlogClass = initBinlogClass(this);
      this.ready = true;
      this.emit('ready');

      this.connection._protocol._enqueue(
        new this.BinlogClass(binlogHandler)
      );
    })
    .catch(err => {
      this.emit('error', err);
    });

};

ZongJi.prototype.stop = function() {
  // Binary log connection does not end with destroy()
  this.connection.destroy();
  this.ctrlConnection.query(
    'KILL ' + this.connection.threadId,
    () => {
      if (this.ctrlConnectionOwner) {
        this.ctrlConnection.destroy();
      }
      this.emit('stopped');
    }
  );
};

// It includes every events by default.
ZongJi.prototype._skipEvent = function(name) {
  const includes = this.filters.includeEvents;
  const excludes = this.filters.excludeEvents;

  let included = (includes === undefined) ||
    (Array.isArray(includes) && (includes.indexOf(name) > -1));
  let excluded = Array.isArray(excludes) && (excludes.indexOf(name) > -1);

  return excluded || !included;
};

// It doesn't skip any schema by default.
ZongJi.prototype._skipSchema = function(database, table) {
  const includes = this.filters.includeSchema;
  const excludes = this.filters.excludeSchema || {};

  let included = (includes === undefined) ||
    (
      (database in includes) &&
      (
        includes[database] === true ||
        (
          Array.isArray(includes[database]) &&
          includes[database].indexOf(table) > -1
        )
      )
    );
  let excluded = (database in excludes) &&
    (
      excludes[database] === true ||
      (
        Array.isArray(excludes[database]) &&
        excludes[database].indexOf(table) > -1
      )
    );

  return excluded || !included;
};

module.exports = ZongJi;

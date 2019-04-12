# winston-sql-transport

Universal [winston](https://www.npmjs.com/package/winston) SQL transport.

Supports:
- MySQL
- PostgreSQL
- SQL Server
- SQLite

via [knex](http://knexjs.org/) library.

## Installation

```console
  $ npm install winston
  $ npm install winston-sql-transport
```

and then install the appropriate database library: [pg](https://github.com/brianc/node-postgres) for PostgreSQL, [mysql](https://github.com/felixge/node-mysql) for MySQL / MariaDB, [mssql](https://github.com/patriksimek/node-mssql) for MSSQL or [sqlite](https://github.com/mapbox/node-sqlite3)..

## Options

See the default values used:

```js
const options = {
  client: 'database_driver', // replace with sqlite3, mysql, etc
  tableName: 'winston_logs',
};
```

For SQLite3, the options should also have:
```js
client: 'sqlite3',
connection: {
  filename: 'sqlite_database.file'
},
useNullAsDefault: true
```

## Usage

```js
const { Logger } = require('winston');
const { SQLTransport } = require('winston-sql-transport');

const logger = new Logger({
  transports: [
    new SQLTransport({
      tableName: 'winston_logs',
    })]
});

module.exports = logger;
```

## Logging

```js
logger.log('info', 'message');
```

## Querying Logs

This transport supports querying of logs with Loggly-like options. [See Loggly Search API](https://www.loggly.com/docs/api-retrieving-data/)

```js
const options = {
  fields: ['message'],
  from: new Date - 24 * 60 * 60 * 1000,
  until: new Date,
  start: 0,
  limit: 10,
  order: 'desc'
};

//
// Find items logged between today and yesterday.
//
logger.query(options, (err, results) => {
  if (err) {
    throw err;
  }

  console.log(results);
});
```

## Streaming Logs

Streaming allows you to stream your logs back

```js
//
// Start at the end.
//
logger.stream({ start: -1 }).on('log', (log) => {
  console.log(log);
});
```

## Run Tests

The tests are written in [vows](http://vowsjs.org), and designed to be run with npm.

```bash
  $ npm test
```

## LICENSE

[MIT License](http://en.wikipedia.org/wiki/MIT_License)

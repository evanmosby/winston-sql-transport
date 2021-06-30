/**
 * @module 'winston-sql-transport'
 * @fileoverview Winston universal SQL transport for logging
 * @license MIT
 * @author Andrei Tretyakov <andrei.tretyakov@gmail.com>
 */

 const knex = require('knex');
 const moment = require('moment');
 const { Stream } = require('stream');
 const TransportStream = require('winston-transport');
 const cluster = require("cluster");
 
 
 class SQLTransport extends TransportStream {
   /**
    * Constructor for the universal transport object.
    * @constructor
    * @param {Object} options
    * @param {string} options.client - Database client
    * @param {string} options.connection - Knex connection object
    * @param {string} [options.label] - Label stored with entry object if defined.
    * @param {string} [options.level=info] - Level of messages that this transport
    * should log.
    * @param {string} [options.name] - Transport instance identifier. Useful if you
    * need to create multiple universal transports.
    * @param {boolean} [options.silent=false] - Boolean flag indicating whether to
    * suppress output.
    * @param {string} [options.tableName=winston_logs] - The name of the table you
    * want to store log messages in.
    * @param {string} [options.daysToKeep] - Number of days to keep the logs (will
    * delete old entries from the database)
    */
   constructor(options = {}) {
     super();
     this.name = 'SQLTransport';
 
     //
     // Configure your storage backing as you see fit
     //
     if (!options.client) {
       throw new Error('You have to define client');
     }
 
     const connection = options.connection || {};
     const pool = options.pool || {};
 
 
     this.client = knex({
       client: options.client,
       connection,
       pool,
       useNullAsDefault: true
     });
 
     this.label = options.label || '';
 
     //
     // Set the level from your options
     //
     this.level = options.level || 'info';
 
     this.silent = options.silent || false;
 
     this.tableName = options.tableName || 'winston_logs';
 
     this.daysToKeep = options.daysToKeep || null;
 
     this.schema = options.schema || function(){}
 
     this.lastId = 0;
     //this.init();
   }
 
   /**
    * Cleanup function, used for log rotation.
    * It executes based on a random number between 0 and 10. It only fires when the random number is 7.
    * Also, it needs to have a daysToKeep configuration option set
    *
    * @private
    */
   _cleanup(override=false) {
     if(this.daysToKeep === null) {
       return;
     }
     let random = override ? 7 : Math.floor(Math.random() * 10);
     
     if(random !== 7) {
       return;
     }
      this.client(this.tableName)
      .where('timestamp', '<', moment().utc().subtract(this.daysToKeep, 'day').format('YYYY-MM-DD HH:mm:ss'))
      .del()
      .then((r) => {
      let d = new Date().toISOString().replace("T", " ").replace("Z", "")
        this.log({message:"successfully trimmed logs",timestamp:d,level:"info",source:"logs::cleanup"},()=>{})
       //setImmediate(() => this.emit('logs::cleanup', {some:"property"}));
      }).catch((r)=>{
        let d = new Date().toISOString().replace("T", " ").replace("Z", "")
        this.log({message:"FAILED TO TRIM LOGS",level:"info",timestamp:d,details:JSON.stringify(r),source:"logs::cleanup"},()=>{})
      })
     
   }

   async clear(){
      await this.client(this.tableName).truncate();
   }

   forceCleanup(){
      this._cleanup(true)  
   }
 
   /**
    * Create logs table and sets the highest ID in Class (for stream)
    * @return {Promise} result of creation within a Promise
    */
   async init() {
     const { client, tableName } = this;
 
     var self = this;
     self.lastId = 0;
 
     await client.schema.hasTable(tableName).then(function(exists) {
       if(!exists) {
         return client.schema.createTable(tableName, function(table) {
           table.increments('id').primary();
           table.string('level');
           table.string('message');
           table.timestamp('timestamp').defaultTo(client.fn.now());
           self.schema(table)
         });
       } else {
        client
             .select('id')
             .from(tableName)
             .orderBy('id', 'desc')
             .limit(1)
             .then((r) => {
               if(r.length > 0) {
                 self.lastId = r[0].id;
               }
             });
       }
     });
   }
 
   /**
    * Core logging method exposed to Winston. Metadata is optional.
    * @param {Object} info - TODO: add param description.
    * @param {Function} callback - TODO: add param description.
    */
   log(info, callback) {
     setImmediate(() => this.emit('logged', info));
 
     const { client, tableName } = this;
     var self = this;
    
     let response = client
     .insert(info)
     .into(tableName)
     .then(() => {
       client
           .select('id')
           .from(tableName)
           .orderBy('id', 'desc')
           .limit(1)
           .then((r) => {
             self.lastId = r[0].id;
           });
       callback(null, true)
     })
     .catch(err => callback(err));

     
     this._cleanup();
 
     return response
   }
 
   /**
    * Query the transport. Options object is optional.
    * @param {Object} options - Loggly-like query options for this instance.
    * @param {string} [options.from] - Start time for the search.
    * @param {string} [options.until=now] - End time for the search. Defaults to "now".
    * @param {string} [options.rows=100] - Limited number of rows returned by search. Defaults to 100.
    * @param {string} [options.order=desc] - Direction of results returned, either "asc" or "desc".
    * @param {string} [options.fields]
    * @param {Function} callback - Continuation to respond to when complete.
    */
   query(...args) {
     let options = args.shift() || {};
     let callback = args.shift();
 
     if (typeof options === 'function') {
       callback = options;
       options = {};
     }
 
     options.fields = options.fields || [];
 
     let query = this.client
       .select(options.fields)
       .from(this.tableName);
 
     if (options.from && options.until) {
       query = query.whereBetween('timestamp', [
         moment(options.from).utc().format('YYYY-MM-DD HH:mm:ss'),
         moment(options.until).utc().format('YYYY-MM-DD HH:mm:ss')
       ]);
     }
 
     if (options.rows) {
       query = query.limit(options.rows);
     }
 
     if (options.order) {
       query = query.orderBy('timestamp', options.order);
     }
 
     query
       .then((data) => {
         callback(null, data);
       })
       .catch(callback);
   }
 
   /**
    * Returns a log stream for this transport. Options object is optional.
    * @param {Object} options - Stream options for this instance.
    * @param {Stream} stream - Pass in a pre-existing stream.
    * @return {Stream}
    */
   stream(...args) {
     const options = args.shift() || {};
     const stream = args.shift() || new Stream();
 
     const self = this;
 
     let start = (typeof options.start === 'undefined') ? null : options.start;
     let row = 0;
 
     if (start === -1) {
       start = null;
     }
 
     let lastId = 0;
     if(start === null) {
       lastId = this.lastId - 1;
     } else {
       lastId = start;
     }
 
     stream.destroy = function destroy() {
       this.destroyed = true;
     };
 
     function poll() {
       self.client
           .select()
           .from(self.tableName)
           .where('id', '>', lastId)
           .then((results) => {
             if (stream.destroyed) {
               return null;
             }
 
             results.forEach((log) => {
               stream.emit('log', log);
 
               lastId = log.id;
             });
 
             return setTimeout(poll, 2000);
           })
           .catch((error) => {
             if (stream.destroyed) {
               return;
             }
             stream.emit('error', error);
             setTimeout(poll, 2000);
           });
     }
 
     // we need to poll here.
     poll(start);
 
     return stream;
   }
 
 }
 
 module.exports = { SQLTransport };
 
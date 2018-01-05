const _ = require('lodash');
const assert = require('assert');
const url = require('url');
const which = require('which');
const path = require('path');
const {spawn} = require('child_process');
const log = require('./log');

/* eslint taskcluster/no-for-in: "off" */

function assertValidSpotRequest(spotRequest) {
  let requiredKeys = [
    'id',
    'workerType',
    'region',
    'az',
    'instanceType',
    'state',
    'status',
    'imageId',
    'created',
  ];
  let errors = [];
  for (let key of Object.keys(spotRequest)) {
    if (!requiredKeys.includes(key)) {
      errors.push(`Extraneous Key "${key}" found`);
    }
  }

  for (let key of requiredKeys) {
    if (!spotRequest[key]) {
      errors.push(`Value not found for required key "${key}"`);
      continue;
    } else if (key === 'created') {
      if (typeof spotRequest[key] !== 'object') {
        errors.push(`Key "${key}" is incorrect type "${typeof spotRequest[key]}"`);
      }
    } else if (typeof spotRequest[key] !== 'string') {
      errors.push(`Key "${key}" is incorrect type "${typeof spotRequest[key]}"`);
    }
  }

  if (errors.length > 0) {
    throw new Error(`Invalid Spot Request:\n\n${errors.join('\n')}`);
  }

  return spotRequest;
}

function assertValidInstance(instance) {
  let requiredKeys = [
    'id',
    'workerType',
    'region',
    'az',
    'instanceType',
    'state',
    'imageId',
    'launched',
    'lastevent',
  ];

  let allowedKeys = [
    'srid',
  ];

  let dateKeys = [
    'launched',
    'lastevent',
  ];

  let errors = [];

  for (let key of Object.keys(instance)) {
    if (!requiredKeys.includes(key) && !allowedKeys.includes(key)) {
      errors.push(`Extraneous Key "${key}" found`);
    }
  }

  for (let key of requiredKeys) {
    let value = instance[key];
    if (!value) {
      errors.push(`Value not found for required key "${key}"`);
      continue;
    } else if (dateKeys.includes(key)) {
      if (typeof value !== 'object' || value.constructor.name !== 'Date') {
        errors.push(`Key "${key}" is incorrect type "${typeof value} or not a Date"`);
      }
    } else if (typeof value !== 'string') {
      errors.push(`Key "${key}" is incorrect type "${typeof value}"`);
    }
  }

  for (let key of allowedKeys) {
    let value = instance[key];
    if (value && typeof value !== 'string') {
      errors.push(`Optional key "${key}" is incorrect type "${typeof value}"`);
    }
  }

  if (errors.length > 0) {
    throw new Error(`Invalid Instance:\n\n${errors.join('\n')}`);
  }

  return instance;
}

/**
 * The State class tracks the in-flight status of the instances and spot
 * requests which we care about.  We only track in-flight requests and we do
 * not log the state transitions.  This is to keep the database design simple
 * and avoid issues around duplicate IDs, since AWS will happily reuse.  State
 * is stored in a Posgresql database and does not support alternate backends.
 * The database is accessed through a PG-Pool.
 *
 * Some queries use a transaction internally to ensure data consistency
*/
class State {

  constructor({knex, databaseUrl, monitor}) {
    this._knex = knex;
    this._databaseUrl = databaseUrl;
    this._monitor = monitor;
  }

  /**
   * Run a SQL script (e.g. a file) against the database.  Note that this
   * function does *not* run the queries internally, rather uses the database
   * connection parameters value that this State's pg-pool was configured with
   * to run the script using the command line 'psql' program.  It chooses the
   * first 'psql' program in the system's path to run the script.
   * 
   * This is done because I'd like to keep the database definition queries as
   * standard sql scripts, while allowing their use in the setup and teardown
   * sections of the tests
   */
  async _runScript(script) {
    let args = [];
    let parsedUrl = url.parse(this._databaseUrl);
    let [user, password] = parsedUrl.auth.split([':']);
    if (parsedUrl.hostname) {
      args.push(`--host=${parsedUrl.hostname}`);
    }
    if (parsedUrl.port) {
      args.push(`--port=${parsedUrl.port}`);
    }
    if (parsedUrl.pathname) {
      args.push(`--dbname=${parsedUrl.pathname.replace(/^\//, '')}`);
    }
    if (user) {
      args.push(`--username=${user}`);
    }
    args.push('--file=' + path.normalize(path.join(__dirname, '..', 'sql', script)));

    let env = {};
    if (password) {
      env.PGPASSWORD = password;
    }

    let psql = which.sync('psql');

    log.debug({cmd: psql, args, env}, 'Running PSQL Command');

    return new Promise(async (resolve, reject) => {
      let psql = await new Promise((res2, rej2) => {
        which('psql', (err, value) => {
          if (err) {
            return rej2(err);
          }
          return res2(value);
        });
      });

      let output = [];

      let proc = spawn(psql, args, {env});

      proc.stdout.on('data', data => {
        output.push(data);
      });

      proc.stderr.on('data', data => {
        output.push(data);
      });

      proc.on('close', code => {
        if (code === 0) {
          resolve();
        } else {
          output = Buffer.concat(output);
          
          // Let's build a string representation of the command so that it's
          // easy to figure out what we were trying to do
          let redoCmd = [];
          if (env) {
            for (let e in env) {
              if (e === 'PGPASSWORD') {
                redoCmd.push(`${e}='<scrubbed>'`);
              } else {
                redoCmd.push(`${e}='${env[e]}'`);
              }
            }
          }
          redoCmd.push(psql);
          for (let arg of args) {
            // Maybe make this a regex for all whitespace
            if (arg.includes(' ')) {
              redoCmd.push(`'${arg.replace(/"/g, '\\"')}'`);
            } else {
              redoCmd.push(arg);
            }
          }

          let err = new Error('PSQL Exited with ' + code + '\nretry: ' + redoCmd.join(' ') + '\n\n' + output);
          err.cmd = psql;
          err.args = args;
          err.env = env;
          reject(err);
        }
      });
    });
  }

  /**
   * Insert a spot request.
   */
  async insertSpotRequest(spotRequest, upsert) {
    let {
      id,
      workerType,
      region,
      az,
      instanceType,
      state,
      status,
      imageId,
      created,
    } = assertValidSpotRequest(spotRequest);

    let query = this._knex.insert({
      id,
      workertype: workerType,
      region,
      az,
      instancetype: instanceType,
      state,
      status,
      imageid: imageId,
      created,
    }).into('spotrequests');

    if (upsert) {
      query = this._knex.raw(`?? ON CONFLICT (region, id) DO UPDATE
          SET state = EXCLUDED.state, status = EXCLUDED.status`, [query]);
    }

    let res = await query;
    assert(res.rowCount === 1, 'inserting spot request had incorrect rowCount');
  }

  /**
   * Either insert or update a spot request.  This ensures an atomic insert or
   * update, but we never learn which one happened.
   */
  async upsertSpotRequest(spotRequest) {
    return this.insertSpotRequest(spotRequest, true);
  }

  /**
   * Update a spot request's state.
   */
  async updateSpotRequestState({region, id, state, status}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof status === 'string');

    let rows = await this._knex('spotrequests').where({region, id}).update({state, status});
    assert(rows === 1, 'updating spot request state had incorrect rowCount');
  }

  /**
   * Stop tracking a spot request.
   */
  async removeSpotRequest({region, id}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');

    return await this._knex('spotrequests').del().where({id, region});
  }

  /**
   * Insert an instance.  If provided, this function will ensure that any spot
   * requests which have an id of `srid` are removed safely.  The implication
   * here is that any spot request which has an associated instance must have
   * been fulfilled
   */
  async insertInstance(instance, upsert) {
    let {
      workerType,
      region,
      az,
      id,
      instanceType,
      state,
      srid,
      imageId,
      launched,
      lastevent,
    } = assertValidInstance(instance);

    await this._knex.transaction(async trx => {
      if (srid) {
        await trx('spotrequests').del().where({id: srid, region});
      }
      
      let query = this._knex.insert({
        id,
        workertype: workerType,
        region,
        az,
        instancetype: instanceType,
        state,
        srid,
        imageid: imageId,
        launched,
        lastevent,
      }).into('instances');

      if (upsert) {
        query = this._knex.raw(`?? ON CONFLICT (region, id) DO UPDATE
            SET state = EXCLUDED.state`, [query]);
      }

      let result = await query;
      assert(result.rowCount === 1, 'inserting instance had incorrect rowCount');
    });
  }

  /**
   * Insert an instance, or update it if there's a conflict.  If provided, this
   * function will ensure that any spot requests which have an id of `srid` are
   * removed safely.  The implication here is that any spot request which has
   * an associated instance must have been fulfilled
   */
  async upsertInstance(instance) {
    return this.insertInstance(instance, true);
  }

  /**
   * Update an instance's state.
   */
  async updateInstanceState({region, id, state, lastevent}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof lastevent === 'object');
    assert(lastevent.constructor.name === 'Date');

    let rows = await this._knex('instances').where({region, id}).update({state, lastevent});
    assert(rows === 1, 'updating instance state had incorrect rowCount');
  }

  /**
   * Stop tracking an instance
   */
  async removeInstance({region, id, srid}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');

    return await this._knex('instances').del().where({id, region});
  }

  /**
   * Modify an instance in a transaction.
   *
   * This guarantees that the given instance is not modified in the interm,
   * using row locking.  The given `modify` function is called with {
   *   instance,          // the instance row, or undefined if one does not exist
   *   removeSpotRequest, // async function({srid, region}) to remove the given spot request
   *   upsertInstance,    // async function(instance) to insert or update the instance; the
   *                      // update variant will only update state and lastevent, 
   *   removeInstance,    // async function() to remove the instance
   * }
   *
   * If the `modify` function throws an exception, the transaction will be
   * rolled back; otherwise it will commit.
   *
   * Note that the properties of `instance` are all lower-case, not camelCase, and that
   * the instance properties passed to upsertInstance are also lower-case.
   */
  async modifyInstance({region, id, modify}) {
    await this._knex.transaction(async trx => {
      let instances = await trx('instances').select('*').forUpdate().where({region, id});
      assert(instances.length <= 1);

      await modify({
        instance: instances[0],
        removeSpotRequest: async ({srid, region}) => {
          await trx('spotrequests').del().where({id: srid, region});
        },
        upsertInstance: async instance => {
          let query = trx.insert(_.defaults({region, id}, instance)).into('instances');
          query = trx.raw(`?? ON CONFLICT (region, id) DO UPDATE SET
              state = EXCLUDED.state,
              lastevent = EXCLUDED.lastevent`, [query]);

          let result = await query;
          assert(result.rowCount === 1, 'upserting instance had incorrect rowCount');
        },
        removeInstance: async () => {
          await trx('instances').del().where({id, region});
        },
      });
    });
  }
  
  /**
   * Either insert or update an AMI's usage.  This ensures an atomic insert or
   * update, but we never learn which one happened.
   */
  async reportAmiUsage({region, id}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    const lastused = new Date();

    let query = this._knex.insert({region, id, lastused}).into('amiusage');
    query = this._knex.raw(`?? ON CONFLICT (region, id) DO UPDATE SET
        lastused = EXCLUDED.lastused`, [query]);
    let result = await query;
    assert(result.rowCount === 1, 'upserting AMI usage had incorrect rowCount');
  }

  /**
   * Internal method used to generate select queries with simple conditions.
   */
  _generateTableListQuery(table, conditions = {}) {
    assert(typeof table === 'string', 'must provide table');
    assert(typeof conditions === 'object', 'conditions must be object');

    let query = this._knex.select('*').from(table);

    // sort so the resulting queries are not dependent on object property ordering
    let conditionNames = Object.keys(conditions);
    conditionNames.sort();
    for (let condition of conditionNames) {
      if (Array.isArray(conditions[condition])) {
        query = query.whereIn(condition, conditions[condition]);
      } else {
        query = query.where(condition, conditions[condition]);
      }
    }

    return query;
  }

  /**
   * Get a list of specific instances
   */
  async listInstances(conditions = {}) {
    return this._generateTableListQuery('instances', conditions);
  }

  /**
   * Get a list of specific spot requests
   */
  async listSpotRequests(conditions = {}) {
    return this._generateTableListQuery('spotrequests', conditions);
  }
   
  /**
   * Get a list of AMIs and their usage
   */
  async listAmiUsage(conditions = {}) {
    return this._generateTableListQuery('amiusage', conditions);
  }
  
  /**
  * Get a list of the current EBS volume usage
  */
  async listEbsUsage(conditions = {}) {
    return this._generateTableListQuery('ebsusage', conditions);
  }

  /**
   * We want to be able to get a simple count of how many worker types there
   * are in a given region.  basically we want something which groups instances
   * first by state (our domain's pending vs. running, not aws's states) and
   * then gives a count for each instance type
   */
  async instanceCounts({workerType}) {
    assert(typeof workerType === 'string');

    let counts = {pending: [], running: []};

    let instances = await this._knex
      .select(
        'state',
        'instancetype',
        {count: this._knex.raw('count("instancetype")')},
      ).from('instances')
      .groupBy('state', 'instancetype')
      .where({workertype: workerType})
      .whereIn('state', ['pending', 'running']);
    for (let row of instances) {
      counts[row.state].push({
        instanceType: row.instancetype,
        count: row.count,
        type: 'instance',
      });
    }

    let spotReqs = await this._knex
      .select(
        'state',
        'instancetype',
        {count: this._knex.raw('count("instancetype")')},
      ).from('spotrequests')
      .groupBy('state', 'instancetype')
      .where({workertype: workerType})
      .where('state', 'open');
    for (let row of spotReqs) {
      counts.pending.push({
        instanceType: row.instancetype,
        count: row.count,
        type: 'spot-request',
      });
    }

    return counts;
  }

  /**
   * We want to be able to get a list of those spot requests which need to be
   * checked.  This is done per region only because the list doesn't really
   * make sense to be pan-ec2.  Basically, this is a list of spot requests which
   * we're going to check in on.
   */
  async spotRequestsToPoll({region}) {
    let result = await this._knex
      .select('id')
      .from('spotrequests')
      .where({region})
      .where('state', 'open')
      .whereIn('status', [
        'pending-evaluation',
        'pending-fulfillment',
      ]);
    return result.map(x => x.id);
  }

  /**
   * List all of the known workerTypes in any running or pending instances
   */
  async listWorkerTypes() {
    let result = await this._knex
      .union(
        this._knex.select('workertype').from('instances'),
        this._knex.select('workertype').from('spotrequests'),
      ).orderBy('workertype');
    return result.map(x => x.workertype);
  }

  /**
   * List all the instance ids and request ids by region
   * so that we can kill them
   */
  async listIdsOfWorkerType({workerType}) {
    let instanceIds = [];
    let requestIds = [];

    let instances = await this._knex
      .select('id', 'srid', 'region')
      .from('instances')
      .where({workertype: workerType});
    for (let instance of instances) {
      instanceIds.push({region: instance.region, id: instance.id});
      if (instance.srid) {
        requestIds.push({region: instance.region, id: instance.srid});
      }
    }

    let requests = await this._knex
      .select('id', 'region')
      .from('spotrequests')
      .where({workertype: workerType});
    for (let request of requests) {
      requestIds.push({region: request.region, id: request.id});
    }

    return {instanceIds, requestIds};
  }

  async logCloudWatchEvent({region, id, state, generated}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof generated === 'object');
    assert(generated.constructor.name === 'Date');

    let query = this._knex.insert({id, region, state, generated}).into('cloudwatchlog');
    // We're going to ignore this primary key violation because it's a sign
    // that a message has already been handled by this system.  Since this is
    // a second receiption of the message we don't really want to log it
    query = this._knex.raw('?? ON CONFLICT DO NOTHING', [query]);
    let result = await query;
    assert(result.rowCount === 1, 'inserting spot request had incorrect rowCount');
  }

  async reportEbsUsage(rowData) {
    assert(typeof rowData === 'object');
    assert(Array.isArray(rowData));
    rowData.forEach(row => {
      assert(typeof row.region === 'string');
      assert(typeof row.volumetype === 'string');
      assert(typeof row.state === 'string');
      assert(typeof row.totalcount === 'number');
      assert(typeof row.totalgb === 'number');
      assert(Object.keys(row).length == 5);
    });

    if (rowData.length === 0) {
      // In this case, we have no rows to insert so ignore the request.
      return;
    }

    await this._knex.transaction(async trx => {
      await trx.truncate('ebsusage');
      let result = await trx.insert(rowData).into('ebsusage');
      assert(result.rowCount === rowData.length, 'inserting EBS usage had incorrect rowCount');
    });
  }
}

module.exports = {State};

'use strict'

module.exports = (function () {
var __MODS__ = {}
var __DEFINE__ = function (id, factory, map) { __MODS__[id] = { factory: factory, map: map, exports: {}, loaded: false } }
var __REQUIRE__ = function (id, source) {
  if (!__MODS__[id]) throw new Error("Cannot find bundled module: " + source)
  var mod = __MODS__[id]
  if (!mod.loaded) {
    mod.loaded = true
    var __LOCAL_REQUIRE__ = function (request) { return typeof request === "number" ? __REQUIRE__(request) : __REQUIRE__(mod.map[request], request) }
    mod.factory(__LOCAL_REQUIRE__, mod, mod.exports)
  }
  return mod.exports
}

// index.js
__DEFINE__(1, function (__LOCAL_REQUIRE__, module, exports) {
const Datastore = __LOCAL_REQUIRE__(2)

module.exports = Datastore

}, {"./lib/datastore":2})

// lib/datastore.js
__DEFINE__(2, function (__LOCAL_REQUIRE__, module, exports) {
const { EventEmitter } = __LOCAL_REQUIRE__(3)
const { callbackify, deprecate } = __LOCAL_REQUIRE__(4)
const Cursor = __LOCAL_REQUIRE__(5)
const customUtils = __LOCAL_REQUIRE__(8)
const Executor = __LOCAL_REQUIRE__(9)
const Index = __LOCAL_REQUIRE__(11)
const model = __LOCAL_REQUIRE__(6)
const Persistence = __LOCAL_REQUIRE__(16)
const { isDate, pick, filterIndexNames } = __LOCAL_REQUIRE__(7)

/**
 * Callback with no parameter
 * @callback NoParamCallback
 * @param {?Error} err
 */

/**
 * String comparison function.
 * ```
 *   if (a < b) return -1
 *   if (a > b) return 1
 *   return 0
 * ```
 * @callback compareStrings
 * @param {string} a
 * @param {string} b
 * @return {number}
 */

/**
 * Callback that returns an Array of documents.
 * @callback MultipleDocumentsCallback
 * @param {?Error} err
 * @param {?document[]} docs
 */

/**
 * Callback that returns a single document.
 * @callback SingleDocumentCallback
 * @param {?Error} err
 * @param {?document} docs
 */

/**
 * Generic async function.
 * @callback AsyncFunction
 * @param {...*} args
 * @return {Promise<*>}
 */

/**
 * Callback with generic parameters.
 * @callback GenericCallback
 * @param {?Error} err
 * @param {...*} args
 */

/**
 * Compaction event. Happens when the Datastore's Persistence has been compacted.
 * It happens when calling {@link Datastore#compactDatafileAsync}, which is called periodically if you have called
 * {@link Datastore#setAutocompactionInterval}.
 *
 * @event Datastore#event:"compaction.done"
 * @type {undefined}
 */

/**
 * Generic document in NeDB.
 * It consists of an Object with anything you want inside.
 * @typedef document
 * @property {?string} [_id] Internal `_id` of the document, which can be `null` or undefined at some points (when not
 * inserted yet for example).
 * @type {object}
 */

/**
 * Nedb query.
 *
 * Each key of a query references a field name, which can use the dot-notation to reference subfields inside nested
 * documents, arrays, arrays of subdocuments and to match a specific element of an array.
 *
 * Each value of a query can be one of the following:
 * - `string`: matches all documents which have this string as value for the referenced field name
 * - `number`: matches all documents which have this number as value for the referenced field name
 * - `Regexp`: matches all documents which have a value that matches the given `Regexp` for the referenced field name
 * - `object`: matches all documents which have this object as deep-value for the referenced field name
 * - Comparison operators: the syntax is `{ field: { $op: value } }` where `$op` is any comparison operator:
 *   - `$lt`, `$lte`: less than, less than or equal
 *   - `$gt`, `$gte`: greater than, greater than or equal
 *   - `$in`: member of. `value` must be an array of values
 *   - `$ne`, `$nin`: not equal, not a member of
 *   - `$exists`: checks whether the document posses the property `field`. `value` should be true or false
 *   - `$regex`: checks whether a string is matched by the regular expression. Contrary to MongoDB, the use of
 *   `$options` with `$regex` is not supported, because it doesn't give you more power than regex flags. Basic
 *   queries are more readable so only use the `$regex` operator when you need to use another operator with it
 *   - `$size`: if the referenced filed is an Array, matches on the size of the array
 *   - `$elemMatch`: matches if at least one array element matches the sub-query entirely
 * - Logical operators: You can combine queries using logical operators:
 *   - For `$or` and `$and`, the syntax is `{ $op: [query1, query2, ...] }`.
 *   - For `$not`, the syntax is `{ $not: query }`
 *   - For `$where`, the syntax is:
 *   ```
 *   { $where: function () {
 *     // object is 'this'
 *     // return a boolean
 *   } }
 *   ```
 * @typedef query
 * @type {Object.<string, *>}
 */

/**
 * Nedb projection.
 *
 * You can give `find` and `findOne` an optional second argument, `projections`.
 * The syntax is the same as MongoDB: `{ a: 1, b: 1 }` to return only the `a`
 * and `b` fields, `{ a: 0, b: 0 }` to omit these two fields. You cannot use both
 * modes at the time, except for `_id` which is by default always returned and
 * which you can choose to omit. You can project on nested documents.
 *
 * To reference subfields, you can use the dot-notation.
 *
 * @typedef projection
 * @type {Object.<string, 0|1>}
 */

/**
 * The `beforeDeserialization` and `afterSerialization` callbacks are hooks which are executed respectively before
 * parsing each document and after stringifying them. They can be used for example to encrypt the Datastore.
 * The `beforeDeserialization` should revert what `afterSerialization` has done.
 * @callback serializationHook
 * @param {string} x
 * @return {string|Promise<string>}
 */

/**
 * @external EventEmitter
 * @see http://nodejs.org/api/events.html
 */

/**
 * @class
 * @classdesc The `Datastore` class is the main class of NeDB.
 * @extends external:EventEmitter
 * @emits Datastore#event:"compaction.done"
 * @typicalname NeDB
 */
class Datastore extends EventEmitter {
  /**
   * Create a new collection, either persistent or in-memory.
   *
   * If you use a persistent datastore without the `autoload` option, you need to call {@link Datastore#loadDatabase} or
   * {@link Datastore#loadDatabaseAsync} manually. This function fetches the data from datafile and prepares the database.
   * **Don't forget it!** If you use a persistent datastore, no command (insert, find, update, remove) will be executed
   * before it is called, so make sure to call it yourself or use the `autoload` option.
   *
   * Also, if loading fails, all commands registered to the {@link Datastore#executor} afterwards will not be executed.
   * They will be registered and executed, in sequence, only after a successful loading.
   *
   * @param {object|string} options Can be an object or a string. If options is a string, the behavior is the same as in
   * v0.6: it will be interpreted as `options.filename`. **Giving a string is deprecated, and will be removed in the
   * next major version.**
   * @param {string} [options.filename = null] Path to the file where the data is persisted. If left blank, the datastore is
   * automatically considered in-memory only. It cannot end with a `~` which is used in the temporary files NeDB uses to
   * perform crash-safe writes. Not used if `options.inMemoryOnly` is `true`.
   * @param {boolean} [options.inMemoryOnly = false] If set to true, no data will be written in storage. This option has
   * priority over `options.filename`.
   * @param {object} [options.modes] Permissions to use for FS. Only used for Node.js storage module. Will not work on Windows.
   * @param {number} [options.modes.fileMode = 0o644] Permissions to use for database files
   * @param {number} [options.modes.dirMode = 0o755] Permissions to use for database directories
   * @param {boolean} [options.timestampData = false] If set to true, createdAt and updatedAt will be created and
   * populated automatically (if not specified by user)
   * @param {boolean} [options.autoload = false] If used, the database will automatically be loaded from the datafile
   * upon creation (you don't need to call `loadDatabase`). Any command issued before load is finished is buffered and
   * will be executed when load is done. When autoloading is done, you can either use the `onload` callback, or you can
   * use `this.autoloadPromise` which resolves (or rejects) when autloading is done.
   * @param {NoParamCallback} [options.onload] If you use autoloading, this is the handler called after the `loadDatabase`. It
   * takes one `error` argument. If you use autoloading without specifying this handler, and an error happens during
   * load, an error will be thrown.
   * @param {serializationHook} [options.beforeDeserialization] Hook you can use to transform data after it was serialized and
   * before it is written to disk. Can be used for example to encrypt data before writing database to disk. This
   * function takes a string as parameter (one line of an NeDB data file) and outputs the transformed string, **which
   * must absolutely not contain a `\n` character** (or data will be lost).
   * @param {serializationHook} [options.afterSerialization] Inverse of `afterSerialization`. Make sure to include both and not
   * just one, or you risk data loss. For the same reason, make sure both functions are inverses of one another. Some
   * failsafe mechanisms are in place to prevent data loss if you misuse the serialization hooks: NeDB checks that never
   * one is declared without the other, and checks that they are reverse of one another by testing on random strings of
   * various lengths. In addition, if too much data is detected as corrupt, NeDB will refuse to start as it could mean
   * you're not using the deserialization hook corresponding to the serialization hook used before.
   * @param {number} [options.corruptAlertThreshold = 0.1] Between 0 and 1, defaults to 10%. NeDB will refuse to start
   * if more than this percentage of the datafile is corrupt. 0 means you don't tolerate any corruption, 1 means you
   * don't care.
   * @param {compareStrings} [options.compareStrings] If specified, it overrides default string comparison which is not
   * well adapted to non-US characters in particular accented letters. Native `localCompare` will most of the time be
   * the right choice.
   * @param {boolean} [options.testSerializationHooks=true] Whether to test the serialization hooks or not,
   * might be CPU-intensive
   */
  constructor (options) {
    super()
    let filename

    // Retrocompatibility with v0.6 and before
    if (typeof options === 'string') {
      deprecate(() => {
        filename = options
        this.inMemoryOnly = false // Default
      }, '@seald-io/nedb: Giving a string to the Datastore constructor is deprecated and will be removed in the next major version. Please use an options object with an argument \'filename\'.')()
    } else {
      options = options || {}
      filename = options.filename
      /**
       * Determines if the `Datastore` keeps data in-memory, or if it saves it in storage. Is not read after
       * instanciation.
       * @type {boolean}
       * @protected
       */
      this.inMemoryOnly = options.inMemoryOnly || false
      /**
       * Determines if the `Datastore` should autoload the database upon instantiation. Is not read after instanciation.
       * @type {boolean}
       * @protected
       */
      this.autoload = options.autoload || false
      /**
       * Determines if the `Datastore` should add `createdAt` and `updatedAt` fields automatically if not set by the user.
       * @type {boolean}
       * @protected
       */
      this.timestampData = options.timestampData || false
    }

    // Determine whether in memory or persistent
    if (!filename || typeof filename !== 'string' || filename.length === 0) {
      /**
       * If null, it means `inMemoryOnly` is `true`. The `filename` is the name given to the storage module. Is not read
       * after instanciation.
       * @type {?string}
       * @protected
       */
      this.filename = null
      this.inMemoryOnly = true
    } else {
      this.filename = filename
    }

    // String comparison function
    /**
     * Overrides default string comparison which is not well adapted to non-US characters in particular accented
     * letters. Native `localCompare` will most of the time be the right choice
     * @type {compareStrings}
     * @function
     * @protected
     */
    this.compareStrings = options.compareStrings

    // Persistence handling
    /**
     * The `Persistence` instance for this `Datastore`.
     * @type {Persistence}
     */
    this.persistence = new Persistence({
      db: this,
      afterSerialization: options.afterSerialization,
      beforeDeserialization: options.beforeDeserialization,
      corruptAlertThreshold: options.corruptAlertThreshold,
      modes: options.modes,
      testSerializationHooks: options.testSerializationHooks
    })

    // This new executor is ready if we don't use persistence
    // If we do, it will only be ready once loadDatabase is called
    /**
     * The `Executor` instance for this `Datastore`. It is used in all methods exposed by the {@link Datastore},
     * any {@link Cursor} produced by the `Datastore` and by {@link Datastore#compactDatafileAsync} to ensure operations
     * are performed sequentially in the database.
     * @type {Executor}
     * @protected
     */
    this.executor = new Executor()
    if (this.inMemoryOnly) this.executor.ready = true

    /**
     * Indexed by field name, dot notation can be used.
     * _id is always indexed and since _ids are generated randomly the underlying binary search tree is always well-balanced
     * @type {Object.<string, Index>}
     * @protected
     */
    this.indexes = {}
    this.indexes._id = new Index({ fieldName: '_id', unique: true })
    /**
     * Stores the time to live (TTL) of the indexes created. The key represents the field name, the value the number of
     * seconds after which data with this index field should be removed.
     * @type {Object.<string, number>}
     * @protected
     */
    this.ttlIndexes = {}

    // Queue a load of the database right away and call the onload handler
    // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
    if (this.autoload) {
      /**
       * A Promise that resolves when the autoload has finished.
       *
       * The onload callback is not awaited by this Promise, it is started immediately after that.
       * @type {?Promise}
       */
      this.autoloadPromise = this.loadDatabaseAsync()
      this.autoloadPromise
        .then(() => {
          if (options.onload) options.onload()
        }, err => {
          if (options.onload) options.onload(err)
          else throw err
        })
    } else this.autoloadPromise = null
    /**
     * Interval if {@link Datastore#setAutocompactionInterval} was called.
     * @private
     * @type {null|number}
     */
    this._autocompactionIntervalId = null
  }

  /**
   * Queue a compaction/rewrite of the datafile.
   * It works by rewriting the database file, and compacts it since the cache always contains only the number of
   * documents in the collection while the data file is append-only so it may grow larger.
   *
   * @async
   */
  compactDatafileAsync () {
    return this.executor.pushAsync(() => this.persistence.persistCachedDatabaseAsync())
  }

  /**
   * Callback version of {@link Datastore#compactDatafileAsync}.
   * @param {NoParamCallback} [callback = () => {}]
   * @see Datastore#compactDatafileAsync
   */
  compactDatafile (callback) {
    const promise = this.compactDatafileAsync()
    if (typeof callback === 'function') callbackify(() => promise)(callback)
  }

  /**
   * Set automatic compaction every `interval` ms
   * @param {Number} interval in milliseconds, with an enforced minimum of 5000 milliseconds
   */
  setAutocompactionInterval (interval) {
    const minInterval = 5000
    if (Number.isNaN(Number(interval))) throw new Error('Interval must be a non-NaN number')
    const realInterval = Math.max(Number(interval), minInterval)

    this.stopAutocompaction()

    this._autocompactionIntervalId = setInterval(() => {
      this.compactDatafile()
    }, realInterval)
  }

  /**
   * Stop autocompaction (do nothing if automatic compaction was not running)
   */
  stopAutocompaction () {
    if (this._autocompactionIntervalId) {
      clearInterval(this._autocompactionIntervalId)
      this._autocompactionIntervalId = null
    }
  }

  /**
   * Callback version of {@link Datastore#loadDatabaseAsync}.
   * @param {NoParamCallback} [callback]
   * @see Datastore#loadDatabaseAsync
   */
  loadDatabase (callback) {
    const promise = this.loadDatabaseAsync()
    if (typeof callback === 'function') callbackify(() => promise)(callback)
  }

  /**
   * Stops auto-compaction, finishes all queued operations, drops the database both in memory and in storage.
   * **WARNING**: it is not recommended re-using an instance of NeDB if its database has been dropped, it is
   * preferable to instantiate a new one.
   * @async
   * @return {Promise}
   */
  dropDatabaseAsync () {
    return this.persistence.dropDatabaseAsync() // the executor is exceptionally used by Persistence
  }

  /**
   * Callback version of {@link Datastore#dropDatabaseAsync}.
   * @param {NoParamCallback} [callback]
   * @see Datastore#dropDatabaseAsync
   */
  dropDatabase (callback) {
    const promise = this.dropDatabaseAsync()
    if (typeof callback === 'function') callbackify(() => promise)(callback)
  }

  /**
   * Load the database from the datafile, and trigger the execution of buffered commands if any.
   * @async
   * @return {Promise}
   */
  loadDatabaseAsync () {
    return this.executor.pushAsync(() => this.persistence.loadDatabaseAsync(), true)
  }

  /**
   * Get an array of all the data in the database.
   * @return {document[]}
   */
  getAllData () {
    return this.indexes._id.getAll()
  }

  /**
   * Reset all currently defined indexes.
   * @param {?document|?document[]} [newData]
   * @private
   */
  _resetIndexes (newData) {
    for (const index of Object.values(this.indexes)) {
      index.reset(newData)
    }
  }

  /**
   * Callback version of {@link Datastore#ensureIndex}.
   * @param {object} options
   * @param {string|string[]} options.fieldName
   * @param {boolean} [options.unique = false]
   * @param {boolean} [options.sparse = false]
   * @param {number} [options.expireAfterSeconds]
   * @param {NoParamCallback} [callback]
   * @see Datastore#ensureIndex
   */
  ensureIndex (options = {}, callback) {
    const promise = this.ensureIndexAsync(options) // to make sure the synchronous part of ensureIndexAsync is executed synchronously
    if (typeof callback === 'function') callbackify(() => promise)(callback)
  }

  /**
   * Ensure an index is kept for this field. Same parameters as lib/indexes
   * This function acts synchronously on the indexes, however the persistence of the indexes is deferred with the
   * executor.
   * @param {object} options
   * @param {string|string[]} options.fieldName Name of the field to index. Use the dot notation to index a field in a nested
   * document. For a compound index, use an array of field names. Using a comma in a field name is not permitted.
   * @param {boolean} [options.unique = false] Enforce field uniqueness. Note that a unique index will raise an error
   * if you try to index two documents for which the field is not defined.
   * @param {boolean} [options.sparse = false] Don't index documents for which the field is not defined. Use this option
   * along with "unique" if you want to accept multiple documents for which it is not defined.
   * @param {number} [options.expireAfterSeconds] - If set, the created index is a TTL (time to live) index, that will
   * automatically remove documents when the system date becomes larger than the date on the indexed field plus
   * `expireAfterSeconds`. Documents where the indexed field is not specified or not a `Date` object are ignored.
   * @return {Promise<void>}
   */
  async ensureIndexAsync (options = {}) {
    if (!options.fieldName) {
      const err = new Error('Cannot create an index without a fieldName')
      err.missingFieldName = true
      throw err
    }

    const _fields = [].concat(options.fieldName).sort()

    if (_fields.some(field => field.includes(','))) {
      throw new Error('Cannot use comma in index fieldName')
    }

    const _options = {
      ...options,
      fieldName: _fields.join(',')
    }

    if (this.indexes[_options.fieldName]) return

    this.indexes[_options.fieldName] = new Index(_options)
    if (options.expireAfterSeconds !== undefined) this.ttlIndexes[_options.fieldName] = _options.expireAfterSeconds // With this implementation index creation is not necessary to ensure TTL but we stick with MongoDB's API here

    try {
      this.indexes[_options.fieldName].insert(this.getAllData())
    } catch (e) {
      delete this.indexes[_options.fieldName]
      throw e
    }

    // We may want to force all options to be persisted including defaults, not just the ones passed the index creation function
    await this.executor.pushAsync(() => this.persistence.persistNewStateAsync([{ $$indexCreated: _options }]), true)
  }

  /**
   * Callback version of {@link Datastore#removeIndexAsync}.
   * @param {string} fieldName
   * @param {NoParamCallback} [callback]
   * @see Datastore#removeIndexAsync
   */
  removeIndex (fieldName, callback = () => {}) {
    const promise = this.removeIndexAsync(fieldName)
    callbackify(() => promise)(callback)
  }

  /**
   * Remove an index.
   * @param {string} fieldName Field name of the index to remove. Use the dot notation to remove an index referring to a
   * field in a nested document.
   * @return {Promise<void>}
   * @see Datastore#removeIndex
   */
  async removeIndexAsync (fieldName) {
    delete this.indexes[fieldName]

    await this.executor.pushAsync(() => this.persistence.persistNewStateAsync([{ $$indexRemoved: fieldName }]), true)
  }

  /**
   * Add one or several document(s) to all indexes.
   *
   * This is an internal function.
   * @param {document} doc
   * @private
   */
  _addToIndexes (doc) {
    let failingIndex
    let error
    const keys = Object.keys(this.indexes)

    for (let i = 0; i < keys.length; i += 1) {
      try {
        this.indexes[keys[i]].insert(doc)
      } catch (e) {
        failingIndex = i
        error = e
        break
      }
    }

    // If an error happened, we need to rollback the insert on all other indexes
    if (error) {
      for (let i = 0; i < failingIndex; i += 1) {
        this.indexes[keys[i]].remove(doc)
      }

      throw error
    }
  }

  /**
   * Remove one or several document(s) from all indexes.
   *
   * This is an internal function.
   * @param {document} doc
   * @private
   */
  _removeFromIndexes (doc) {
    for (const index of Object.values(this.indexes)) {
      index.remove(doc)
    }
  }

  /**
   * Update one or several documents in all indexes.
   *
   * To update multiple documents, oldDoc must be an array of { oldDoc, newDoc } pairs.
   *
   * If one update violates a constraint, all changes are rolled back.
   *
   * This is an internal function.
   * @param {document|Array.<{oldDoc: document, newDoc: document}>} oldDoc Document to update, or an `Array` of
   * `{oldDoc, newDoc}` pairs.
   * @param {document} [newDoc] Document to replace the oldDoc with. If the first argument is an `Array` of
   * `{oldDoc, newDoc}` pairs, this second argument is ignored.
   * @private
   */
  _updateIndexes (oldDoc, newDoc) {
    let failingIndex
    let error
    const keys = Object.keys(this.indexes)

    for (let i = 0; i < keys.length; i += 1) {
      try {
        this.indexes[keys[i]].update(oldDoc, newDoc)
      } catch (e) {
        failingIndex = i
        error = e
        break
      }
    }

    // If an error happened, we need to rollback the update on all other indexes
    if (error) {
      for (let i = 0; i < failingIndex; i += 1) {
        this.indexes[keys[i]].revertUpdate(oldDoc, newDoc)
      }

      throw error
    }
  }

  /**
   * Get all candidate documents matching the query, regardless of their expiry status.
   * @param {query} query
   * @return {document[]}
   *
   * @private
   */
  _getRawCandidates (query) {
    const indexNames = Object.keys(this.indexes)

    // STEP 1: get candidates list by checking indexes from most to least frequent usecase
    // For a basic match

    let usableQuery
    usableQuery = Object.entries(query)
      .filter(filterIndexNames(indexNames))
      .pop()
    if (usableQuery) return this.indexes[usableQuery[0]].getMatching(usableQuery[1])

    // For a compound match
    const compoundQueryKeys = indexNames
      .filter(indexName => indexName.indexOf(',') !== -1)
      .map(indexName => indexName.split(','))
      .filter(subIndexNames =>
        Object.entries(query)
          .filter(filterIndexNames(subIndexNames)).length === subIndexNames.length
      )

    if (compoundQueryKeys.length > 0) return this.indexes[compoundQueryKeys[0]].getMatching(pick(query, compoundQueryKeys[0]))

    // For a $in match
    usableQuery = Object.entries(query)
      .filter(([k, v]) =>
        !!(query[k] && Object.prototype.hasOwnProperty.call(query[k], '$in')) &&
        indexNames.includes(k)
      )
      .pop()
    if (usableQuery) return this.indexes[usableQuery[0]].getMatching(usableQuery[1].$in)
    // For a comparison match
    usableQuery = Object.entries(query)
      .filter(([k, v]) =>
        !!(query[k] && (Object.prototype.hasOwnProperty.call(query[k], '$lt') || Object.prototype.hasOwnProperty.call(query[k], '$lte') || Object.prototype.hasOwnProperty.call(query[k], '$gt') || Object.prototype.hasOwnProperty.call(query[k], '$gte'))) &&
        indexNames.includes(k)
      )
      .pop()
    if (usableQuery) return this.indexes[usableQuery[0]].getBetweenBounds(usableQuery[1])
    // By default, return all the DB data
    return this.getAllData()
  }

  /**
   * Return the list of candidates for a given query
   * Crude implementation for now, we return the candidates given by the first usable index if any
   * We try the following query types, in this order: basic match, $in match, comparison match
   * One way to make it better would be to enable the use of multiple indexes if the first usable index
   * returns too much data. I may do it in the future.
   *
   * Returned candidates will be scanned to find and remove all expired documents
   *
   * This is an internal function.
   * @param {query} query
   * @param {boolean} [dontExpireStaleDocs = false] If true don't remove stale docs. Useful for the remove function
   * which shouldn't be impacted by expirations.
   * @return {Promise<document[]>} candidates
   * @private
   */
  async _getCandidatesAsync (query, dontExpireStaleDocs = false) {
    const validDocs = []

    // STEP 1: get candidates list by checking indexes from most to least frequent usecase
    const docs = this._getRawCandidates(query)
    // STEP 2: remove all expired documents
    if (!dontExpireStaleDocs) {
      const expiredDocsIds = []
      const ttlIndexesFieldNames = Object.keys(this.ttlIndexes)

      docs.forEach(doc => {
        if (ttlIndexesFieldNames.every(i => !(doc[i] !== undefined && isDate(doc[i]) && Date.now() > doc[i].getTime() + this.ttlIndexes[i] * 1000))) validDocs.push(doc)
        else expiredDocsIds.push(doc._id)
      })
      for (const _id of expiredDocsIds) {
        await this._removeAsync({ _id }, {})
      }
    } else validDocs.push(...docs)
    return validDocs
  }

  /**
   * Insert a new document
   * This is an internal function, use {@link Datastore#insertAsync} which has the same signature.
   * @param {document|document[]} newDoc
   * @return {Promise<document|document[]>}
   * @private
   */
  async _insertAsync (newDoc) {
    const preparedDoc = this._prepareDocumentForInsertion(newDoc)
    this._insertInCache(preparedDoc)

    await this.persistence.persistNewStateAsync(Array.isArray(preparedDoc) ? preparedDoc : [preparedDoc])
    return model.deepCopy(preparedDoc)
  }

  /**
   * Create a new _id that's not already in use
   * @return {string} id
   * @private
   */
  _createNewId () {
    let attemptId = customUtils.uid(16)
    // Try as many times as needed to get an unused _id. As explained in customUtils, the probability of this ever happening is extremely small, so this is O(1)
    if (this.indexes._id.getMatching(attemptId).length > 0) attemptId = this._createNewId()
    return attemptId
  }

  /**
   * Prepare a document (or array of documents) to be inserted in a database
   * Meaning adds _id and timestamps if necessary on a copy of newDoc to avoid any side effect on user input
   * @param {document|document[]} newDoc document, or Array of documents, to prepare
   * @return {document|document[]} prepared document, or Array of prepared documents
   * @private
   */
  _prepareDocumentForInsertion (newDoc) {
    let preparedDoc

    if (Array.isArray(newDoc)) {
      preparedDoc = []
      newDoc.forEach(doc => { preparedDoc.push(this._prepareDocumentForInsertion(doc)) })
    } else {
      preparedDoc = model.deepCopy(newDoc)
      if (preparedDoc._id === undefined) preparedDoc._id = this._createNewId()
      const now = new Date()
      if (this.timestampData && preparedDoc.createdAt === undefined) preparedDoc.createdAt = now
      if (this.timestampData && preparedDoc.updatedAt === undefined) preparedDoc.updatedAt = now
      model.checkObject(preparedDoc)
    }

    return preparedDoc
  }

  /**
   * If newDoc is an array of documents, this will insert all documents in the cache
   * @param {document|document[]} preparedDoc
   * @private
   */
  _insertInCache (preparedDoc) {
    if (Array.isArray(preparedDoc)) this._insertMultipleDocsInCache(preparedDoc)
    else this._addToIndexes(preparedDoc)
  }

  /**
   * If one insertion fails (e.g. because of a unique constraint), roll back all previous
   * inserts and throws the error
   * @param {document[]} preparedDocs
   * @private
   */
  _insertMultipleDocsInCache (preparedDocs) {
    let failingIndex
    let error

    for (let i = 0; i < preparedDocs.length; i += 1) {
      try {
        this._addToIndexes(preparedDocs[i])
      } catch (e) {
        error = e
        failingIndex = i
        break
      }
    }

    if (error) {
      for (let i = 0; i < failingIndex; i += 1) {
        this._removeFromIndexes(preparedDocs[i])
      }

      throw error
    }
  }

  /**
   * Callback version of {@link Datastore#insertAsync}.
   * @param {document|document[]} newDoc
   * @param {SingleDocumentCallback|MultipleDocumentsCallback} [callback]
   * @see Datastore#insertAsync
   */
  insert (newDoc, callback) {
    const promise = this.insertAsync(newDoc)
    if (typeof callback === 'function') callbackify(() => promise)(callback)
  }

  /**
   * Insert a new document, or new documents.
   * @param {document|document[]} newDoc Document or array of documents to insert.
   * @return {Promise<document|document[]>} The document(s) inserted.
   * @async
   */
  insertAsync (newDoc) {
    return this.executor.pushAsync(() => this._insertAsync(newDoc))
  }

  /**
   * Callback for {@link Datastore#countCallback}.
   * @callback Datastore~countCallback
   * @param {?Error} err
   * @param {?number} count
   */

  /**
   * Callback-version of {@link Datastore#countAsync}.
   * @param {query} query
   * @param {Datastore~countCallback} [callback]
   * @return {Cursor<number>|undefined}
   * @see Datastore#countAsync
   */
  count (query, callback) {
    const cursor = this.countAsync(query)

    if (typeof callback === 'function') callbackify(cursor.execAsync.bind(cursor))(callback)
    else return cursor
  }

  /**
   * Count all documents matching the query.
   * @param {query} query MongoDB-style query
   * @return {Cursor<number>} count
   * @async
   */
  countAsync (query) {
    return new Cursor(this, query, docs => docs.length)
  }

  /**
   * Callback version of {@link Datastore#findAsync}.
   * @param {query} query
   * @param {projection|MultipleDocumentsCallback} [projection = {}]
   * @param {MultipleDocumentsCallback} [callback]
   * @return {Cursor<document[]>|undefined}
   * @see Datastore#findAsync
   */
  find (query, projection, callback) {
    if (arguments.length === 1) {
      projection = {}
      // callback is undefined, will return a cursor
    } else if (arguments.length === 2) {
      if (typeof projection === 'function') {
        callback = projection
        projection = {}
      } // If not assume projection is an object and callback undefined
    }

    const cursor = this.findAsync(query, projection)

    if (typeof callback === 'function') callbackify(cursor.execAsync.bind(cursor))(callback)
    else return cursor
  }

  /**
   * Find all documents matching the query.
   * We return the {@link Cursor} that the user can either `await` directly or use to can {@link Cursor#limit} or
   * {@link Cursor#skip} before.
   * @param {query} query MongoDB-style query
   * @param {projection} [projection = {}] MongoDB-style projection
   * @return {Cursor<document[]>}
   * @async
   */
  findAsync (query, projection = {}) {
    const cursor = new Cursor(this, query, docs => docs.map(doc => model.deepCopy(doc)))

    cursor.projection(projection)
    return cursor
  }

  /**
   * @callback Datastore~findOneCallback
   * @param {?Error} err
   * @param {document} doc
   */

  /**
   * Callback version of {@link Datastore#findOneAsync}.
   * @param {query} query
   * @param {projection|SingleDocumentCallback} [projection = {}]
   * @param {SingleDocumentCallback} [callback]
   * @return {Cursor<document>|undefined}
   * @see Datastore#findOneAsync
   */
  findOne (query, projection, callback) {
    if (arguments.length === 1) {
      projection = {}
      // callback is undefined, will return a cursor
    } else if (arguments.length === 2) {
      if (typeof projection === 'function') {
        callback = projection
        projection = {}
      } // If not assume projection is an object and callback undefined
    }

    const cursor = this.findOneAsync(query, projection)

    if (typeof callback === 'function') callbackify(cursor.execAsync.bind(cursor))(callback)
    else return cursor
  }

  /**
   * Find one document matching the query.
   * We return the {@link Cursor} that the user can either `await` directly or use to can {@link Cursor#skip} before.
   * @param {query} query MongoDB-style query
   * @param {projection} projection MongoDB-style projection
   * @return {Cursor<document>}
   */
  findOneAsync (query, projection = {}) {
    const cursor = new Cursor(this, query, docs => docs.length === 1 ? model.deepCopy(docs[0]) : null)

    cursor.projection(projection).limit(1)
    return cursor
  }

  /**
   * See {@link Datastore#updateAsync} return type for the definition of the callback parameters.
   *
   * **WARNING:** Prior to 3.0.0, `upsert` was either `true` of falsy (but not `false`), it is now always a boolean.
   * `affectedDocuments` could be `undefined` when `returnUpdatedDocs` was `false`, it is now `null` in these cases.
   *
   * **WARNING:** Prior to 1.8.0, the `upsert` argument was not given, it was impossible for the developer to determine
   * during a `{ multi: false, returnUpdatedDocs: true, upsert: true }` update if it inserted a document or just updated
   * it.
   *
   * @callback Datastore~updateCallback
   * @param {?Error} err
   * @param {number} numAffected
   * @param {?document[]|?document} affectedDocuments
   * @param {boolean} upsert
   * @see {Datastore#updateAsync}
   */

  /**
   * Version without the using {@link Datastore~executor} of {@link Datastore#updateAsync}, use it instead.
   *
   * @param {query} query
   * @param {document|update} update
   * @param {Object} options
   * @param {boolean} [options.multi = false]
   * @param {boolean} [options.upsert = false]
   * @param {boolean} [options.returnUpdatedDocs = false]
   * @return {Promise<{numAffected: number, affectedDocuments: document[]|document|null, upsert: boolean}>}
   * @private
   * @see Datastore#updateAsync
   */
  async _updateAsync (query, update, options) {
    const multi = options.multi !== undefined ? options.multi : false
    const upsert = options.upsert !== undefined ? options.upsert : false

    // If upsert option is set, check whether we need to insert the doc
    if (upsert) {
      const cursor = new Cursor(this, query)

      // Need to use an internal function not tied to the executor to avoid deadlock
      const docs = await cursor.limit(1)._execAsync()

      if (docs.length !== 1) {
        let toBeInserted

        try {
          model.checkObject(update)
          // updateQuery is a simple object with no modifier, use it as the document to insert
          toBeInserted = update
        } catch (e) {
          // updateQuery contains modifiers, use the find query as the base,
          // strip it from all operators and update it according to updateQuery
          toBeInserted = model.modify(model.deepCopy(query, true), update)
        }
        const newDoc = await this._insertAsync(toBeInserted)
        return { numAffected: 1, affectedDocuments: newDoc, upsert: true }
      }
    }
    // Perform the update
    let numReplaced = 0
    let modifiedDoc
    const modifications = []
    let createdAt

    const candidates = await this._getCandidatesAsync(query)
    // Preparing update (if an error is thrown here neither the datafile nor
    // the in-memory indexes are affected)
    for (const candidate of candidates) {
      if (model.match(candidate, query) && (multi || numReplaced === 0)) {
        numReplaced += 1
        if (this.timestampData) { createdAt = candidate.createdAt }
        modifiedDoc = model.modify(candidate, update)
        if (this.timestampData) {
          modifiedDoc.createdAt = createdAt
          modifiedDoc.updatedAt = new Date()
        }
        modifications.push({ oldDoc: candidate, newDoc: modifiedDoc })
      }
    }

    // Change the docs in memory
    this._updateIndexes(modifications)

    // Update the datafile
    const updatedDocs = modifications.map(x => x.newDoc)
    await this.persistence.persistNewStateAsync(updatedDocs)
    if (!options.returnUpdatedDocs) return { numAffected: numReplaced, upsert: false, affectedDocuments: null }
    else {
      let updatedDocsDC = []
      updatedDocs.forEach(doc => { updatedDocsDC.push(model.deepCopy(doc)) })
      if (!multi) updatedDocsDC = updatedDocsDC[0]
      return { numAffected: numReplaced, affectedDocuments: updatedDocsDC, upsert: false }
    }
  }

  /**
   * Callback version of {@link Datastore#updateAsync}.
   * @param {query} query
   * @param {document|*} update
   * @param {Object|Datastore~updateCallback} [options|]
   * @param {boolean} [options.multi = false]
   * @param {boolean} [options.upsert = false]
   * @param {boolean} [options.returnUpdatedDocs = false]
   * @param {Datastore~updateCallback} [callback]
   * @see Datastore#updateAsync
   *
   */
  update (query, update, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    const _callback = (err, res = {}) => {
      if (callback) callback(err, res.numAffected, res.affectedDocuments, res.upsert)
    }
    callbackify((query, update, options) => this.updateAsync(query, update, options))(query, update, options, _callback)
  }

  /**
   * Update all docs matching query.
   * @param {query} query is the same kind of finding query you use with `find` and `findOne`.
   * @param {document|*} update specifies how the documents should be modified. It is either a new document or a
   * set of modifiers (you cannot use both together, it doesn't make sense!). Using a new document will replace the
   * matched docs. Using a set of modifiers will create the fields they need to modify if they don't exist, and you can
   * apply them to subdocs. Available field modifiers are `$set` to change a field's value, `$unset` to delete a field,
   * `$inc` to increment a field's value and `$min`/`$max` to change field's value, only if provided value is
   * less/greater than current value. To work on arrays, you have `$push`, `$pop`, `$addToSet`, `$pull`, and the special
   * `$each` and `$slice`.
   * @param {Object} [options = {}] Optional options
   * @param {boolean} [options.multi = false] If true, can update multiple documents
   * @param {boolean} [options.upsert = false] If true, can insert a new document corresponding to the `update` rules if
   * your `query` doesn't match anything. If your `update` is a simple object with no modifiers, it is the inserted
   * document. In the other case, the `query` is stripped from all operator recursively, and the `update` is applied to
   * it.
   * @param {boolean} [options.returnUpdatedDocs = false] (not Mongo-DB compatible) If true and update is not an upsert,
   * will return the array of documents matched by the find query and updated. Updated documents will be returned even
   * if the update did not actually modify them.
   * @return {Promise<{numAffected: number, affectedDocuments: document[]|document|null, upsert: boolean}>}
   * - `upsert` is `true` if and only if the update did insert a document, **cannot be true if `options.upsert !== true`**.
   * - `numAffected` is the number of documents affected by the update or insertion (if `options.multi` is `false` or `options.upsert` is `true`, cannot exceed `1`);
   * - `affectedDocuments` can be one of the following:
   *    - If `upsert` is `true`, the inserted document;
   *    - If `options.returnUpdatedDocs` is `false`, `null`;
   *    - If `options.returnUpdatedDocs` is `true`:
   *      - If `options.multi` is `false`, the updated document;
   *      - If `options.multi` is `true`, the array of updated documents.
   * @async
   */
  updateAsync (query, update, options = {}) {
    return this.executor.pushAsync(() => this._updateAsync(query, update, options))
  }

  /**
   * @callback Datastore~removeCallback
   * @param {?Error} err
   * @param {?number} numRemoved
   */

  /**
   * Internal version without using the {@link Datastore#executor} of {@link Datastore#removeAsync}, use it instead.
   *
   * @param {query} query
   * @param {object} [options]
   * @param {boolean} [options.multi = false]
   * @return {Promise<number>}
   * @private
   * @see Datastore#removeAsync
   */
  async _removeAsync (query, options = {}) {
    const multi = options.multi !== undefined ? options.multi : false

    const candidates = await this._getCandidatesAsync(query, true)
    const removedDocs = []
    let numRemoved = 0

    candidates.forEach(d => {
      if (model.match(d, query) && (multi || numRemoved === 0)) {
        numRemoved += 1
        removedDocs.push({ $$deleted: true, _id: d._id })
        this._removeFromIndexes(d)
      }
    })

    await this.persistence.persistNewStateAsync(removedDocs)
    return numRemoved
  }

  /**
   * Callback version of {@link Datastore#removeAsync}.
   * @param {query} query
   * @param {object|Datastore~removeCallback} [options={}]
   * @param {boolean} [options.multi = false]
   * @param {Datastore~removeCallback} [cb = () => {}]
   * @see Datastore#removeAsync
   */
  remove (query, options, cb) {
    if (typeof options === 'function') {
      cb = options
      options = {}
    }
    const callback = cb || (() => {})
    callbackify((query, options) => this.removeAsync(query, options))(query, options, callback)
  }

  /**
   * Remove all docs matching the query.
   * @param {query} query MongoDB-style query
   * @param {object} [options={}] Optional options
   * @param {boolean} [options.multi = false] If true, can update multiple documents
   * @return {Promise<number>} How many documents were removed
   * @async
   */
  removeAsync (query, options = {}) {
    return this.executor.pushAsync(() => this._removeAsync(query, options))
  }
}

module.exports = Datastore

}, {"events":3,"util":4,"./cursor.js":5,"./customUtils.js":8,"./executor.js":9,"./indexes.js":11,"./model.js":6,"./persistence.js":16,"./utils.js":7})

// node_modules/events/events.js
__DEFINE__(3, function (__LOCAL_REQUIRE__, module, exports) {
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

'use strict';

var R = typeof Reflect === 'object' ? Reflect : null
var ReflectApply = R && typeof R.apply === 'function'
  ? R.apply
  : function ReflectApply(target, receiver, args) {
    return Function.prototype.apply.call(target, receiver, args);
  }

var ReflectOwnKeys
if (R && typeof R.ownKeys === 'function') {
  ReflectOwnKeys = R.ownKeys
} else if (Object.getOwnPropertySymbols) {
  ReflectOwnKeys = function ReflectOwnKeys(target) {
    return Object.getOwnPropertyNames(target)
      .concat(Object.getOwnPropertySymbols(target));
  };
} else {
  ReflectOwnKeys = function ReflectOwnKeys(target) {
    return Object.getOwnPropertyNames(target);
  };
}

function ProcessEmitWarning(warning) {
  if (console && console.warn) console.warn(warning);
}

var NumberIsNaN = Number.isNaN || function NumberIsNaN(value) {
  return value !== value;
}

function EventEmitter() {
  EventEmitter.init.call(this);
}
module.exports = EventEmitter;
module.exports.once = once;

// Backwards-compat with node 0.10.x
EventEmitter.EventEmitter = EventEmitter;

EventEmitter.prototype._events = undefined;
EventEmitter.prototype._eventsCount = 0;
EventEmitter.prototype._maxListeners = undefined;

// By default EventEmitters will print a warning if more than 10 listeners are
// added to it. This is a useful default which helps finding memory leaks.
var defaultMaxListeners = 10;

function checkListener(listener) {
  if (typeof listener !== 'function') {
    throw new TypeError('The "listener" argument must be of type Function. Received type ' + typeof listener);
  }
}

Object.defineProperty(EventEmitter, 'defaultMaxListeners', {
  enumerable: true,
  get: function() {
    return defaultMaxListeners;
  },
  set: function(arg) {
    if (typeof arg !== 'number' || arg < 0 || NumberIsNaN(arg)) {
      throw new RangeError('The value of "defaultMaxListeners" is out of range. It must be a non-negative number. Received ' + arg + '.');
    }
    defaultMaxListeners = arg;
  }
});

EventEmitter.init = function() {

  if (this._events === undefined ||
      this._events === Object.getPrototypeOf(this)._events) {
    this._events = Object.create(null);
    this._eventsCount = 0;
  }

  this._maxListeners = this._maxListeners || undefined;
};

// Obviously not all Emitters should be limited to 10. This function allows
// that to be increased. Set to zero for unlimited.
EventEmitter.prototype.setMaxListeners = function setMaxListeners(n) {
  if (typeof n !== 'number' || n < 0 || NumberIsNaN(n)) {
    throw new RangeError('The value of "n" is out of range. It must be a non-negative number. Received ' + n + '.');
  }
  this._maxListeners = n;
  return this;
};

function _getMaxListeners(that) {
  if (that._maxListeners === undefined)
    return EventEmitter.defaultMaxListeners;
  return that._maxListeners;
}

EventEmitter.prototype.getMaxListeners = function getMaxListeners() {
  return _getMaxListeners(this);
};

EventEmitter.prototype.emit = function emit(type) {
  var args = [];
  for (var i = 1; i < arguments.length; i++) args.push(arguments[i]);
  var doError = (type === 'error');

  var events = this._events;
  if (events !== undefined)
    doError = (doError && events.error === undefined);
  else if (!doError)
    return false;

  // If there is no 'error' event listener then throw.
  if (doError) {
    var er;
    if (args.length > 0)
      er = args[0];
    if (er instanceof Error) {
      // Note: The comments on the `throw` lines are intentional, they show
      // up in Node's output if this results in an unhandled exception.
      throw er; // Unhandled 'error' event
    }
    // At least give some kind of context to the user
    var err = new Error('Unhandled error.' + (er ? ' (' + er.message + ')' : ''));
    err.context = er;
    throw err; // Unhandled 'error' event
  }

  var handler = events[type];

  if (handler === undefined)
    return false;

  if (typeof handler === 'function') {
    ReflectApply(handler, this, args);
  } else {
    var len = handler.length;
    var listeners = arrayClone(handler, len);
    for (var i = 0; i < len; ++i)
      ReflectApply(listeners[i], this, args);
  }

  return true;
};

function _addListener(target, type, listener, prepend) {
  var m;
  var events;
  var existing;

  checkListener(listener);

  events = target._events;
  if (events === undefined) {
    events = target._events = Object.create(null);
    target._eventsCount = 0;
  } else {
    // To avoid recursion in the case that type === "newListener"! Before
    // adding it to the listeners, first emit "newListener".
    if (events.newListener !== undefined) {
      target.emit('newListener', type,
                  listener.listener ? listener.listener : listener);

      // Re-assign `events` because a newListener handler could have caused the
      // this._events to be assigned to a new object
      events = target._events;
    }
    existing = events[type];
  }

  if (existing === undefined) {
    // Optimize the case of one listener. Don't need the extra array object.
    existing = events[type] = listener;
    ++target._eventsCount;
  } else {
    if (typeof existing === 'function') {
      // Adding the second element, need to change to array.
      existing = events[type] =
        prepend ? [listener, existing] : [existing, listener];
      // If we've already got an array, just append.
    } else if (prepend) {
      existing.unshift(listener);
    } else {
      existing.push(listener);
    }

    // Check for listener leak
    m = _getMaxListeners(target);
    if (m > 0 && existing.length > m && !existing.warned) {
      existing.warned = true;
      // No error code for this since it is a Warning
      // eslint-disable-next-line no-restricted-syntax
      var w = new Error('Possible EventEmitter memory leak detected. ' +
                          existing.length + ' ' + String(type) + ' listeners ' +
                          'added. Use emitter.setMaxListeners() to ' +
                          'increase limit');
      w.name = 'MaxListenersExceededWarning';
      w.emitter = target;
      w.type = type;
      w.count = existing.length;
      ProcessEmitWarning(w);
    }
  }

  return target;
}

EventEmitter.prototype.addListener = function addListener(type, listener) {
  return _addListener(this, type, listener, false);
};

EventEmitter.prototype.on = EventEmitter.prototype.addListener;

EventEmitter.prototype.prependListener =
    function prependListener(type, listener) {
      return _addListener(this, type, listener, true);
    };

function onceWrapper() {
  if (!this.fired) {
    this.target.removeListener(this.type, this.wrapFn);
    this.fired = true;
    if (arguments.length === 0)
      return this.listener.call(this.target);
    return this.listener.apply(this.target, arguments);
  }
}

function _onceWrap(target, type, listener) {
  var state = { fired: false, wrapFn: undefined, target: target, type: type, listener: listener };
  var wrapped = onceWrapper.bind(state);
  wrapped.listener = listener;
  state.wrapFn = wrapped;
  return wrapped;
}

EventEmitter.prototype.once = function once(type, listener) {
  checkListener(listener);
  this.on(type, _onceWrap(this, type, listener));
  return this;
};

EventEmitter.prototype.prependOnceListener =
    function prependOnceListener(type, listener) {
      checkListener(listener);
      this.prependListener(type, _onceWrap(this, type, listener));
      return this;
    };

// Emits a 'removeListener' event if and only if the listener was removed.
EventEmitter.prototype.removeListener =
    function removeListener(type, listener) {
      var list, events, position, i, originalListener;

      checkListener(listener);

      events = this._events;
      if (events === undefined)
        return this;

      list = events[type];
      if (list === undefined)
        return this;

      if (list === listener || list.listener === listener) {
        if (--this._eventsCount === 0)
          this._events = Object.create(null);
        else {
          delete events[type];
          if (events.removeListener)
            this.emit('removeListener', type, list.listener || listener);
        }
      } else if (typeof list !== 'function') {
        position = -1;

        for (i = list.length - 1; i >= 0; i--) {
          if (list[i] === listener || list[i].listener === listener) {
            originalListener = list[i].listener;
            position = i;
            break;
          }
        }

        if (position < 0)
          return this;

        if (position === 0)
          list.shift();
        else {
          spliceOne(list, position);
        }

        if (list.length === 1)
          events[type] = list[0];

        if (events.removeListener !== undefined)
          this.emit('removeListener', type, originalListener || listener);
      }

      return this;
    };

EventEmitter.prototype.off = EventEmitter.prototype.removeListener;

EventEmitter.prototype.removeAllListeners =
    function removeAllListeners(type) {
      var listeners, events, i;

      events = this._events;
      if (events === undefined)
        return this;

      // not listening for removeListener, no need to emit
      if (events.removeListener === undefined) {
        if (arguments.length === 0) {
          this._events = Object.create(null);
          this._eventsCount = 0;
        } else if (events[type] !== undefined) {
          if (--this._eventsCount === 0)
            this._events = Object.create(null);
          else
            delete events[type];
        }
        return this;
      }

      // emit removeListener for all listeners on all events
      if (arguments.length === 0) {
        var keys = Object.keys(events);
        var key;
        for (i = 0; i < keys.length; ++i) {
          key = keys[i];
          if (key === 'removeListener') continue;
          this.removeAllListeners(key);
        }
        this.removeAllListeners('removeListener');
        this._events = Object.create(null);
        this._eventsCount = 0;
        return this;
      }

      listeners = events[type];

      if (typeof listeners === 'function') {
        this.removeListener(type, listeners);
      } else if (listeners !== undefined) {
        // LIFO order
        for (i = listeners.length - 1; i >= 0; i--) {
          this.removeListener(type, listeners[i]);
        }
      }

      return this;
    };

function _listeners(target, type, unwrap) {
  var events = target._events;

  if (events === undefined)
    return [];

  var evlistener = events[type];
  if (evlistener === undefined)
    return [];

  if (typeof evlistener === 'function')
    return unwrap ? [evlistener.listener || evlistener] : [evlistener];

  return unwrap ?
    unwrapListeners(evlistener) : arrayClone(evlistener, evlistener.length);
}

EventEmitter.prototype.listeners = function listeners(type) {
  return _listeners(this, type, true);
};

EventEmitter.prototype.rawListeners = function rawListeners(type) {
  return _listeners(this, type, false);
};

EventEmitter.listenerCount = function(emitter, type) {
  if (typeof emitter.listenerCount === 'function') {
    return emitter.listenerCount(type);
  } else {
    return listenerCount.call(emitter, type);
  }
};

EventEmitter.prototype.listenerCount = listenerCount;
function listenerCount(type) {
  var events = this._events;

  if (events !== undefined) {
    var evlistener = events[type];

    if (typeof evlistener === 'function') {
      return 1;
    } else if (evlistener !== undefined) {
      return evlistener.length;
    }
  }

  return 0;
}

EventEmitter.prototype.eventNames = function eventNames() {
  return this._eventsCount > 0 ? ReflectOwnKeys(this._events) : [];
};

function arrayClone(arr, n) {
  var copy = new Array(n);
  for (var i = 0; i < n; ++i)
    copy[i] = arr[i];
  return copy;
}

function spliceOne(list, index) {
  for (; index + 1 < list.length; index++)
    list[index] = list[index + 1];
  list.pop();
}

function unwrapListeners(arr) {
  var ret = new Array(arr.length);
  for (var i = 0; i < ret.length; ++i) {
    ret[i] = arr[i].listener || arr[i];
  }
  return ret;
}

function once(emitter, name) {
  return new Promise(function (resolve, reject) {
    function errorListener(err) {
      emitter.removeListener(name, resolver);
      reject(err);
    }

    function resolver() {
      if (typeof emitter.removeListener === 'function') {
        emitter.removeListener('error', errorListener);
      }
      resolve([].slice.call(arguments));
    };

    eventTargetAgnosticAddListener(emitter, name, resolver, { once: true });
    if (name !== 'error') {
      addErrorHandlerIfEventEmitter(emitter, errorListener, { once: true });
    }
  });
}

function addErrorHandlerIfEventEmitter(emitter, handler, flags) {
  if (typeof emitter.on === 'function') {
    eventTargetAgnosticAddListener(emitter, 'error', handler, flags);
  }
}

function eventTargetAgnosticAddListener(emitter, name, listener, flags) {
  if (typeof emitter.on === 'function') {
    if (flags.once) {
      emitter.once(name, listener);
    } else {
      emitter.on(name, listener);
    }
  } else if (typeof emitter.addEventListener === 'function') {
    // EventTarget does not have `error` event semantics like Node
    // EventEmitters, we do not listen for `error` events here.
    emitter.addEventListener(name, function wrapListener(arg) {
      // IE does not have builtin `{ once: true }` support so we
      // have to do it manually.
      if (flags.once) {
        emitter.removeEventListener(name, wrapListener);
      }
      listener(arg);
    });
  } else {
    throw new TypeError('The "emitter" argument must be of type EventEmitter. Received type ' + typeof emitter);
  }
}

}, {})

// browser-version/lib/util.wx.js
__DEFINE__(4, function (__LOCAL_REQUIRE__, module, exports) {
/**
 * Minimal util subset used by the Weixin Mini Program bundle.
 *
 * The npm util package pulls process and other Node-oriented shims into
 * Weixin DevTools, so the bundle aliases the util module to this file.
 * @private
 */

const callbackify = fn => function (...args) {
  const callback = args.pop()

  if (typeof callback !== 'function') {
    throw new TypeError('The last argument must be of type Function')
  }

  Promise.resolve()
    .then(() => fn.apply(this, args))
    .then(
      value => callback(null, value),
      error => callback(error)
    )
}

const deprecate = fn => fn

module.exports.callbackify = callbackify
module.exports.deprecate = deprecate

}, {})

// lib/cursor.js
__DEFINE__(5, function (__LOCAL_REQUIRE__, module, exports) {
const model = __LOCAL_REQUIRE__(6)
const { callbackify } = __LOCAL_REQUIRE__(4)

/**
 * Has a callback
 * @callback Cursor~mapFn
 * @param {document[]} res
 * @return {*|Promise<*>}
 */

/**
 * Manage access to data, be it to find, update or remove it.
 *
 * It extends `Promise` so that its methods (which return `this`) are chainable & awaitable.
 * @extends Promise
 */
class Cursor {
  /**
   * Create a new cursor for this collection.
   * @param {Datastore} db - The datastore this cursor is bound to
   * @param {query} query - The query this cursor will operate on
   * @param {Cursor~mapFn} [mapFn] - Handler to be executed after cursor has found the results and before the callback passed to find/findOne/update/remove
   */
  constructor (db, query, mapFn) {
    /**
     * @protected
     * @type {Datastore}
     */
    this.db = db
    /**
     * @protected
     * @type {query}
     */
    this.query = query || {}
    /**
     * The handler to be executed after cursor has found the results.
     * @type {Cursor~mapFn}
     * @protected
     */
    if (mapFn) this.mapFn = mapFn
    /**
     * @see Cursor#limit
     * @type {undefined|number}
     * @private
     */
    this._limit = undefined
    /**
     * @see Cursor#skip
     * @type {undefined|number}
     * @private
     */
    this._skip = undefined
    /**
     * @see Cursor#sort
     * @type {undefined|Object.<string, number>}
     * @private
     */
    this._sort = undefined
    /**
     * @see Cursor#projection
     * @type {undefined|Object.<string, number>}
     * @private
     */
    this._projection = undefined
  }

  /**
   * Set a limit to the number of results for the given Cursor.
   * @param {Number} limit
   * @return {Cursor} the same instance of Cursor, (useful for chaining).
   */
  limit (limit) {
    this._limit = limit
    return this
  }

  /**
   * Skip a number of results for the given Cursor.
   * @param {Number} skip
   * @return {Cursor} the same instance of Cursor, (useful for chaining).
   */
  skip (skip) {
    this._skip = skip
    return this
  }

  /**
   * Sort results of the query for the given Cursor.
   * @param {Object.<string, number>} sortQuery - sortQuery is { field: order }, field can use the dot-notation, order is 1 for ascending and -1 for descending
   * @return {Cursor} the same instance of Cursor, (useful for chaining).
   */
  sort (sortQuery) {
    this._sort = sortQuery
    return this
  }

  /**
   * Add the use of a projection to the given Cursor.
   * @param {Object.<string, number>} projection - MongoDB-style projection. {} means take all fields. Then it's { key1: 1, key2: 1 } to take only key1 and key2
   * { key1: 0, key2: 0 } to omit only key1 and key2. Except _id, you can't mix takes and omits.
   * @return {Cursor} the same instance of Cursor, (useful for chaining).
   */
  projection (projection) {
    this._projection = projection
    return this
  }

  /**
   * Apply the projection.
   *
   * This is an internal function. You should use {@link Cursor#execAsync} or {@link Cursor#exec}.
   * @param {document[]} candidates
   * @return {document[]}
   * @private
   */
  _project (candidates) {
    const res = []
    let action

    if (this._projection === undefined || Object.keys(this._projection).length === 0) {
      return candidates
    }

    const keepId = this._projection._id !== 0
    const { _id, ...rest } = this._projection
    this._projection = rest

    // Check for consistency
    const keys = Object.keys(this._projection)
    keys.forEach(k => {
      if (action !== undefined && this._projection[k] !== action) throw new Error('Can\'t both keep and omit fields except for _id')
      action = this._projection[k]
    })

    // Do the actual projection
    candidates.forEach(candidate => {
      let toPush
      if (action === 1) { // pick-type projection
        toPush = { $set: {} }
        keys.forEach(k => {
          toPush.$set[k] = model.getDotValue(candidate, k)
          if (toPush.$set[k] === undefined) delete toPush.$set[k]
        })
        toPush = model.modify({}, toPush)
      } else { // omit-type projection
        toPush = { $unset: {} }
        keys.forEach(k => { toPush.$unset[k] = true })
        toPush = model.modify(candidate, toPush)
      }
      if (keepId) toPush._id = candidate._id
      else delete toPush._id
      res.push(toPush)
    })

    return res
  }

  /**
   * Get all matching elements
   * Will return pointers to matched elements (shallow copies), returning full copies is the role of find or findOne
   * This is an internal function, use execAsync which uses the executor
   * @return {document[]|Promise<*>}
   * @private
   */
  async _execAsync () {
    let res = []
    let added = 0
    let skipped = 0

    const candidates = await this.db._getCandidatesAsync(this.query)

    for (const candidate of candidates) {
      if (model.match(candidate, this.query)) {
        // If a sort is defined, wait for the results to be sorted before applying limit and skip
        if (!this._sort) {
          if (this._skip && this._skip > skipped) skipped += 1
          else {
            res.push(candidate)
            added += 1
            if (this._limit && this._limit <= added) break
          }
        } else res.push(candidate)
      }
    }

    // Apply all sorts
    if (this._sort) {
      // Sorting
      const criteria = Object.entries(this._sort).map(([key, direction]) => ({ key, direction }))
      res.sort((a, b) => {
        for (const criterion of criteria) {
          const compare = criterion.direction * model.compareThings(model.getDotValue(a, criterion.key), model.getDotValue(b, criterion.key), this.db.compareStrings)
          if (compare !== 0) return compare
        }
        return 0
      })

      // Applying limit and skip
      const limit = this._limit || res.length
      const skip = this._skip || 0

      res = res.slice(skip, skip + limit)
    }

    // Apply projection
    res = this._project(res)
    if (this.mapFn) return this.mapFn(res)
    return res
  }

  /**
   * @callback Cursor~execCallback
   * @param {Error} err
   * @param {document[]|*} res If a mapFn was given to the Cursor, then the type of this parameter is the one returned by the mapFn.
   */

  /**
   * Callback version of {@link Cursor#exec}.
   * @param {Cursor~execCallback} _callback
   * @see Cursor#execAsync
   */
  exec (_callback) {
    callbackify(() => this.execAsync())(_callback)
  }

  /**
   * Get all matching elements.
   * Will return pointers to matched elements (shallow copies), returning full copies is the role of {@link Datastore#findAsync} or {@link Datastore#findOneAsync}.
   * @return {Promise<document[]|*>}
   * @async
   */
  execAsync () {
    return this.db.executor.pushAsync(() => this._execAsync())
  }

  then (onFulfilled, onRejected) {
    return this.execAsync().then(onFulfilled, onRejected)
  }

  catch (onRejected) {
    return this.execAsync().catch(onRejected)
  }

  finally (onFinally) {
    return this.execAsync().finally(onFinally)
  }
}

// Interface
module.exports = Cursor

}, {"./model.js":6,"util":4})

// lib/model.js
__DEFINE__(6, function (__LOCAL_REQUIRE__, module, exports) {
/**
 * Handle models (i.e. docs)
 * Serialization/deserialization
 * Copying
 * Querying, update
 * @module model
 * @private
 */
const { uniq, isDate, isRegExp } = __LOCAL_REQUIRE__(7)

/**
 * Check a key, throw an error if the key is non valid
 * @param {string} k key
 * @param {document} v value, needed to treat the Date edge case
 * Non-treatable edge cases here: if part of the object if of the form { $$date: number } or { $$deleted: true }
 * Its serialized-then-deserialized version it will transformed into a Date object
 * But you really need to want it to trigger such behaviour, even when warned not to use '$' at the beginning of the field names...
 * @private
 */
const checkKey = (k, v) => {
  if (typeof k === 'number') k = k.toString()

  if (
    k[0] === '$' &&
    !(k === '$$date' && typeof v === 'number') &&
    !(k === '$$deleted' && v === true) &&
    !(k === '$$indexCreated') &&
    !(k === '$$indexRemoved')
  ) throw new Error('Field names cannot begin with the $ character')

  if (k.indexOf('.') !== -1) throw new Error('Field names cannot contain a .')
}

/**
 * Check a DB object and throw an error if it's not valid
 * Works by applying the above checkKey function to all fields recursively
 * @param {document|document[]} obj
 * @alias module:model.checkObject
 */
const checkObject = obj => {
  if (Array.isArray(obj)) {
    obj.forEach(o => {
      checkObject(o)
    })
  }

  if (typeof obj === 'object' && obj !== null) {
    for (const k in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, k)) {
        checkKey(k, obj[k])
        checkObject(obj[k])
      }
    }
  }
}

/**
 * Serialize an object to be persisted to a one-line string
 * For serialization/deserialization, we use the native JSON parser and not eval or Function
 * That gives us less freedom but data entered in the database may come from users
 * so eval and the like are not safe
 * Accepted primitive types: Number, String, Boolean, Date, null
 * Accepted secondary types: Objects, Arrays
 * @param {document} obj
 * @return {string}
 * @alias module:model.serialize
 */
const serialize = obj => {
  return JSON.stringify(obj, function (k, v) {
    checkKey(k, v)

    if (v === undefined) return undefined
    if (v === null) return null

    // Hackish way of checking if object is Date (this way it works between execution contexts in node-webkit).
    // We can't use value directly because for dates it is already string in this function (date.toJSON was already called), so we use this
    if (typeof this[k].getTime === 'function') return { $$date: this[k].getTime() }

    return v
  })
}

/**
 * From a one-line representation of an object generate by the serialize function
 * Return the object itself
 * @param {string} rawData
 * @return {document}
 * @alias module:model.deserialize
 */
const deserialize = rawData => JSON.parse(rawData, function (k, v) {
  if (k === '$$date') return new Date(v)
  if (
    typeof v === 'string' ||
    typeof v === 'number' ||
    typeof v === 'boolean' ||
    v === null
  ) return v
  if (v && v.$$date) return v.$$date

  return v
})

/**
 * Deep copy a DB object
 * The optional strictKeys flag (defaulting to false) indicates whether to copy everything or only fields
 * where the keys are valid, i.e. don't begin with $ and don't contain a .
 * @param {?document} obj
 * @param {boolean} [strictKeys=false]
 * @return {?document}
 * @alias module:modelel:(.*)
 */
function deepCopy (obj, strictKeys) {
  if (
    typeof obj === 'boolean' ||
    typeof obj === 'number' ||
    typeof obj === 'string' ||
    obj === null ||
    (isDate(obj))
  ) return obj

  if (Array.isArray(obj)) return obj.map(o => deepCopy(o, strictKeys))

  if (typeof obj === 'object') {
    const res = {}
    for (const k in obj) {
      if (
        Object.prototype.hasOwnProperty.call(obj, k) &&
        (!strictKeys || (k[0] !== '$' && k.indexOf('.') === -1))
      ) {
        res[k] = deepCopy(obj[k], strictKeys)
      }
    }
    return res
  }

  return undefined // For now everything else is undefined. We should probably throw an error instead
}

/**
 * Tells if an object is a primitive type or a "real" object
 * Arrays are considered primitive
 * @param {*} obj
 * @return {boolean}
 * @alias module:modelel:(.*)
 */
const isPrimitiveType = obj => (
  typeof obj === 'boolean' ||
  typeof obj === 'number' ||
  typeof obj === 'string' ||
  obj === null ||
  isDate(obj) ||
  Array.isArray(obj)
)

/**
 * Utility functions for comparing things
 * Assumes type checking was already done (a and b already have the same type)
 * compareNSB works for numbers, strings and booleans
 * @param {number|string|boolean} a
 * @param {number|string|boolean} b
 * @return {number} 0 if a == b, 1 i a > b, -1 if a < b
 * @private
 */
const compareNSB = (a, b) => {
  if (a < b) return -1
  if (a > b) return 1
  return 0
}

/**
 * Utility function for comparing array
 * Assumes type checking was already done (a and b already have the same type)
 * compareNSB works for numbers, strings and booleans
 * @param {Array} a
 * @param {Array} b
 * @return {number} 0 if arrays have the same length and all elements equal one another. Else either 1 or -1.
 * @private
 */
const compareArrays = (a, b) => {
  const minLength = Math.min(a.length, b.length)
  for (let i = 0; i < minLength; i += 1) {
    const comp = compareThings(a[i], b[i])

    if (comp !== 0) return comp
  }

  // Common section was identical, longest one wins
  return compareNSB(a.length, b.length)
}

/**
 * Compare { things U undefined }
 * Things are defined as any native types (string, number, boolean, null, date) and objects
 * We need to compare with undefined as it will be used in indexes
 * In the case of objects and arrays, we deep-compare
 * If two objects dont have the same type, the (arbitrary) type hierarchy is: undefined, null, number, strings, boolean, dates, arrays, objects
 * Return -1 if a < b, 1 if a > b and 0 if a = b (note that equality here is NOT the same as defined in areThingsEqual!)
 * @param {*} a
 * @param {*} b
 * @param {compareStrings} [_compareStrings] String comparing function, returning -1, 0 or 1, overriding default string comparison (useful for languages with accented letters)
 * @return {number}
 * @alias module:model.compareThings
 */
const compareThings = (a, b, _compareStrings) => {
  const compareStrings = _compareStrings || compareNSB

  // undefined
  if (a === undefined) return b === undefined ? 0 : -1
  if (b === undefined) return 1 // no need to test if a === undefined

  // null
  if (a === null) return b === null ? 0 : -1
  if (b === null) return 1 // no need to test if a === null

  // Numbers
  if (typeof a === 'number') return typeof b === 'number' ? compareNSB(a, b) : -1
  if (typeof b === 'number') return typeof a === 'number' ? compareNSB(a, b) : 1

  // Strings
  if (typeof a === 'string') return typeof b === 'string' ? compareStrings(a, b) : -1
  if (typeof b === 'string') return typeof a === 'string' ? compareStrings(a, b) : 1

  // Booleans
  if (typeof a === 'boolean') return typeof b === 'boolean' ? compareNSB(a, b) : -1
  if (typeof b === 'boolean') return typeof a === 'boolean' ? compareNSB(a, b) : 1

  // Dates
  if (isDate(a)) return isDate(b) ? compareNSB(a.getTime(), b.getTime()) : -1
  if (isDate(b)) return isDate(a) ? compareNSB(a.getTime(), b.getTime()) : 1

  // Arrays (first element is most significant and so on)
  if (Array.isArray(a)) return Array.isArray(b) ? compareArrays(a, b) : -1
  if (Array.isArray(b)) return Array.isArray(a) ? compareArrays(a, b) : 1

  // Objects
  const aKeys = Object.keys(a).sort()
  const bKeys = Object.keys(b).sort()

  for (let i = 0; i < Math.min(aKeys.length, bKeys.length); i += 1) {
    const comp = compareThings(a[aKeys[i]], b[bKeys[i]])

    if (comp !== 0) return comp
  }

  return compareNSB(aKeys.length, bKeys.length)
}

// ==============================================================
// Updating documents
// ==============================================================

/**
 * @callback modifierFunction
 * The signature of modifier functions is as follows
 * Their structure is always the same: recursively follow the dot notation while creating
 * the nested documents if needed, then apply the "last step modifier"
 * @param {Object} obj The model to modify
 * @param {String} field Can contain dots, in that case that means we will set a subfield recursively
 * @param {document} value
 */

/**
 * Create the complete modifier function
 * @param {modifierFunction} lastStepModifierFunction a lastStepModifierFunction
 * @param {boolean} [unset = false] Bad looking specific fix, needs to be generalized modifiers that behave like $unset are implemented
 * @return {modifierFunction}
 * @private
 */
const createModifierFunction = (lastStepModifierFunction, unset = false) => {
  const func = (obj, field, value) => {
    const fieldParts = typeof field === 'string' ? field.split('.') : field

    if (fieldParts.length === 1) lastStepModifierFunction(obj, field, value)
    else {
      if (obj[fieldParts[0]] === undefined) {
        if (unset) return
        obj[fieldParts[0]] = {}
      }
      func(obj[fieldParts[0]], fieldParts.slice(1), value)
    }
  }
  return func
}

const $addToSetPartial = (obj, field, value) => {
  // Create the array if it doesn't exist
  if (!Object.prototype.hasOwnProperty.call(obj, field)) { obj[field] = [] }

  if (!Array.isArray(obj[field])) throw new Error('Can\'t $addToSet an element on non-array values')

  if (value !== null && typeof value === 'object' && value.$each) {
    if (Object.keys(value).length > 1) throw new Error('Can\'t use another field in conjunction with $each')
    if (!Array.isArray(value.$each)) throw new Error('$each requires an array value')

    value.$each.forEach(v => {
      $addToSetPartial(obj, field, v)
    })
  } else {
    let addToSet = true
    obj[field].forEach(v => {
      if (compareThings(v, value) === 0) addToSet = false
    })
    if (addToSet) obj[field].push(value)
  }
}

/**
 * @enum {modifierFunction}
 */
const modifierFunctions = {
  /**
   * Set a field to a new value
   */
  $set: createModifierFunction((obj, field, value) => {
    obj[field] = value
  }),
  /**
   * Unset a field
   */
  $unset: createModifierFunction((obj, field, value) => {
    delete obj[field]
  }, true),
  /**
   * Updates the value of the field, only if specified field is smaller than the current value of the field
   */
  $min: createModifierFunction((obj, field, value) => {
    if (typeof obj[field] === 'undefined') obj[field] = value
    else if (value < obj[field]) obj[field] = value
  }),
  /**
   * Updates the value of the field, only if specified field is greater than the current value of the field
   */
  $max: createModifierFunction((obj, field, value) => {
    if (typeof obj[field] === 'undefined') obj[field] = value
    else if (value > obj[field]) obj[field] = value
  }),
  /**
   * Increment a numeric field's value
   */
  $inc: createModifierFunction((obj, field, value) => {
    if (typeof value !== 'number') throw new Error(`${value} must be a number`)

    if (typeof obj[field] !== 'number') {
      if (!Object.prototype.hasOwnProperty.call(obj, field)) obj[field] = value
      else throw new Error('Don\'t use the $inc modifier on non-number fields')
    } else obj[field] += value
  }),
  /**
   * Removes all instances of a value from an existing array
   */
  $pull: createModifierFunction((obj, field, value) => {
    if (!Array.isArray(obj[field])) throw new Error('Can\'t $pull an element from non-array values')

    const arr = obj[field]
    for (let i = arr.length - 1; i >= 0; i -= 1) {
      if (match(arr[i], value)) arr.splice(i, 1)
    }
  }),
  /**
   * Remove the first or last element of an array
   */
  $pop: createModifierFunction((obj, field, value) => {
    if (!Array.isArray(obj[field])) throw new Error('Can\'t $pop an element from non-array values')
    if (typeof value !== 'number') throw new Error(`${value} isn't an integer, can't use it with $pop`)
    if (value === 0) return

    if (value > 0) obj[field] = obj[field].slice(0, obj[field].length - 1)
    else obj[field] = obj[field].slice(1)
  }),
  /**
   * Add an element to an array field only if it is not already in it
   * No modification if the element is already in the array
   * Note that it doesn't check whether the original array contains duplicates
   */
  $addToSet: createModifierFunction($addToSetPartial),
  /**
   * Push an element to the end of an array field
   * Optional modifier $each instead of value to push several values
   * Optional modifier $slice to slice the resulting array, see https://docs.mongodb.org/manual/reference/operator/update/slice/
   * Difference with MongoDB: if $slice is specified and not $each, we act as if value is an empty array
   */
  $push: createModifierFunction((obj, field, value) => {
    // Create the array if it doesn't exist
    if (!Object.prototype.hasOwnProperty.call(obj, field)) obj[field] = []

    if (!Array.isArray(obj[field])) throw new Error('Can\'t $push an element on non-array values')

    if (
      value !== null &&
      typeof value === 'object' &&
      value.$slice &&
      value.$each === undefined
    ) value.$each = []

    if (value !== null && typeof value === 'object' && value.$each) {
      if (
        Object.keys(value).length >= 3 ||
        (Object.keys(value).length === 2 && value.$slice === undefined)
      ) throw new Error('Can only use $slice in cunjunction with $each when $push to array')
      if (!Array.isArray(value.$each)) throw new Error('$each requires an array value')

      value.$each.forEach(v => {
        obj[field].push(v)
      })

      if (value.$slice === undefined || typeof value.$slice !== 'number') return

      if (value.$slice === 0) obj[field] = []
      else {
        let start
        let end
        const n = obj[field].length
        if (value.$slice < 0) {
          start = Math.max(0, n + value.$slice)
          end = n
        } else if (value.$slice > 0) {
          start = 0
          end = Math.min(n, value.$slice)
        }
        obj[field] = obj[field].slice(start, end)
      }
    } else {
      obj[field].push(value)
    }
  })

}

/**
 * Modify a DB object according to an update query
 * @param {document} obj
 * @param {query} updateQuery
 * @return {document}
 * @alias module:model.modify
 */
const modify = (obj, updateQuery) => {
  const keys = Object.keys(updateQuery)
  const firstChars = keys.map(item => item[0])
  const dollarFirstChars = firstChars.filter(c => c === '$')
  let newDoc
  let modifiers

  if (keys.indexOf('_id') !== -1 && updateQuery._id !== obj._id) throw new Error('You cannot change a document\'s _id')

  if (dollarFirstChars.length !== 0 && dollarFirstChars.length !== firstChars.length) throw new Error('You cannot mix modifiers and normal fields')

  if (dollarFirstChars.length === 0) {
    // Simply replace the object with the update query contents
    newDoc = deepCopy(updateQuery)
    newDoc._id = obj._id
  } else {
    // Apply modifiers
    modifiers = uniq(keys)
    newDoc = deepCopy(obj)
    modifiers.forEach(m => {
      if (!modifierFunctions[m]) throw new Error(`Unknown modifier ${m}`)

      // Can't rely on Object.keys throwing on non objects since ES6
      // Not 100% satisfying as non objects can be interpreted as objects but no false negatives so we can live with it
      if (typeof updateQuery[m] !== 'object') throw new Error(`Modifier ${m}'s argument must be an object`)

      const keys = Object.keys(updateQuery[m])
      keys.forEach(k => {
        modifierFunctions[m](newDoc, k, updateQuery[m][k])
      })
    })
  }

  // Check result is valid and return it
  checkObject(newDoc)

  if (obj._id !== newDoc._id) throw new Error('You can\'t change a document\'s _id')
  return newDoc
}

// ==============================================================
// Finding documents
// ==============================================================

/**
 * Get a value from object with dot notation
 * @param {object} obj
 * @param {string} field
 * @return {*}
 * @alias module:model.getDotValue
 */
const getDotValue = (obj, field) => {
  const fieldParts = typeof field === 'string' ? field.split('.') : field

  if (!obj) return undefined // field cannot be empty so that means we should return undefined so that nothing can match

  if (fieldParts.length === 0) return obj

  if (fieldParts.length === 1) return obj[fieldParts[0]]

  if (Array.isArray(obj[fieldParts[0]])) {
    // If the next field is an integer, return only this item of the array
    const i = parseInt(fieldParts[1], 10)
    if (typeof i === 'number' && !isNaN(i)) return getDotValue(obj[fieldParts[0]][i], fieldParts.slice(2))

    // Return the array of values
    return obj[fieldParts[0]].map(el => getDotValue(el, fieldParts.slice(1)))
  } else return getDotValue(obj[fieldParts[0]], fieldParts.slice(1))
}

/**
 * Get dot values for either a bunch of fields or just one.
 */
const getDotValues = (obj, fields) => {
  if (!Array.isArray(fields)) throw new Error('fields must be an Array')
  if (fields.length > 1) {
    const key = {}
    for (const field of fields) {
      key[field] = getDotValue(obj, field)
    }
    return key
  } else return getDotValue(obj, fields[0])
}

/**
 * Check whether 'things' are equal
 * Things are defined as any native types (string, number, boolean, null, date) and objects
 * In the case of object, we check deep equality
 * Returns true if they are, false otherwise
 * @param {*} a
 * @param {*} a
 * @return {boolean}
 * @alias module:model.areThingsEqual
 */
const areThingsEqual = (a, b) => {
  // Strings, booleans, numbers, null
  if (
    a === null ||
    typeof a === 'string' ||
    typeof a === 'boolean' ||
    typeof a === 'number' ||
    b === null ||
    typeof b === 'string' ||
    typeof b === 'boolean' ||
    typeof b === 'number'
  ) return a === b

  // Dates
  if (isDate(a) || isDate(b)) return isDate(a) && isDate(b) && a.getTime() === b.getTime()

  // Arrays (no match since arrays are used as a $in)
  // undefined (no match since they mean field doesn't exist and can't be serialized)
  if (
    (!(Array.isArray(a) && Array.isArray(b)) && (Array.isArray(a) || Array.isArray(b))) ||
    a === undefined || b === undefined
  ) return false

  // General objects (check for deep equality)
  // a and b should be objects at this point
  let aKeys
  let bKeys
  try {
    aKeys = Object.keys(a)
    bKeys = Object.keys(b)
  } catch (e) {
    return false
  }

  if (aKeys.length !== bKeys.length) return false
  for (const el of aKeys) {
    if (bKeys.indexOf(el) === -1) return false
    if (!areThingsEqual(a[el], b[el])) return false
  }
  return true
}

/**
 * Check that two values are comparable
 * @param {*} a
 * @param {*} a
 * @return {boolean}
 * @private
 */
const areComparable = (a, b) => {
  if (
    typeof a !== 'string' &&
    typeof a !== 'number' &&
    !isDate(a) &&
    typeof b !== 'string' &&
    typeof b !== 'number' &&
    !isDate(b)
  ) return false

  if (typeof a !== typeof b) return false

  return true
}

/**
 * @callback comparisonOperator
 * Arithmetic and comparison operators
 * @param {*} a Value in the object
 * @param {*} b Value in the query
 * @return {boolean}
 */

/**
 * @enum {comparisonOperator}
 */
const comparisonFunctions = {
  /** Lower than */
  $lt: (a, b) => areComparable(a, b) && a < b,
  /** Lower than or equals */
  $lte: (a, b) => areComparable(a, b) && a <= b,
  /** Greater than */
  $gt: (a, b) => areComparable(a, b) && a > b,
  /** Greater than or equals */
  $gte: (a, b) => areComparable(a, b) && a >= b,
  /** Does not equal */
  $ne: (a, b) => a === undefined || !areThingsEqual(a, b),
  /** Is in Array */
  $in: (a, b) => {
    if (!Array.isArray(b)) throw new Error('$in operator called with a non-array')

    for (const el of b) {
      if (areThingsEqual(a, el)) return true
    }

    return false
  },
  /** Is not in Array */
  $nin: (a, b) => {
    if (!Array.isArray(b)) throw new Error('$nin operator called with a non-array')

    return !comparisonFunctions.$in(a, b)
  },
  /** Matches Regexp */
  $regex: (a, b) => {
    if (!isRegExp(b)) throw new Error('$regex operator called with non regular expression')

    if (typeof a !== 'string') return false
    else return b.test(a)
  },
  /** Returns true if field exists */
  $exists: (a, b) => {
    // This will be true for all values of stat except false, null, undefined and 0
    // That's strange behaviour (we should only use true/false) but that's the way Mongo does it...
    if (b || b === '') b = true
    else b = false

    if (a === undefined) return !b
    else return b
  },
  /** Specific to Arrays, returns true if a length equals b */
  $size: (a, b) => {
    if (!Array.isArray(a)) return false
    if (b % 1 !== 0) throw new Error('$size operator called without an integer')

    return a.length === b
  },
  /** Specific to Arrays, returns true if some elements of a match the query b */
  $elemMatch: (a, b) => {
    if (!Array.isArray(a)) return false
    return a.some(el => match(el, b))
  }
}

const arrayComparisonFunctions = { $size: true, $elemMatch: true }

/**
 * @enum
 */
const logicalOperators = {
  /**
   * Match any of the subqueries
   * @param {document} obj
   * @param {query[]} query
   * @return {boolean}
   */
  $or: (obj, query) => {
    if (!Array.isArray(query)) throw new Error('$or operator used without an array')

    for (let i = 0; i < query.length; i += 1) {
      if (match(obj, query[i])) return true
    }

    return false
  },
  /**
   * Match all of the subqueries
   * @param {document} obj
   * @param {query[]} query
   * @return {boolean}
   */
  $and: (obj, query) => {
    if (!Array.isArray(query)) throw new Error('$and operator used without an array')

    for (let i = 0; i < query.length; i += 1) {
      if (!match(obj, query[i])) return false
    }

    return true
  },
  /**
   * Inverted match of the query
   * @param {document} obj
   * @param {query} query
   * @return {boolean}
   */
  $not: (obj, query) => !match(obj, query),

  /**
   * @callback whereCallback
   * @param {document} obj
   * @return {boolean}
   */

  /**
   * Use a function to match
   * @param {document} obj
   * @param {whereCallback} fn
   * @return {boolean}
   */
  $where: (obj, fn) => {
    if (typeof fn !== 'function') throw new Error('$where operator used without a function')

    const result = fn.call(obj)
    if (typeof result !== 'boolean') throw new Error('$where function must return boolean')

    return result
  }
}

/**
 * Tell if a given document matches a query
 * @param {document} obj Document to check
 * @param {query} query
 * @return {boolean}
 * @alias module:model.match
 */
const match = (obj, query) => {
  // Primitive query against a primitive type
  // This is a bit of a hack since we construct an object with an arbitrary key only to dereference it later
  // But I don't have time for a cleaner implementation now
  if (isPrimitiveType(obj) || isPrimitiveType(query)) return matchQueryPart({ needAKey: obj }, 'needAKey', query)

  // Normal query
  for (const queryKey in query) {
    if (Object.prototype.hasOwnProperty.call(query, queryKey)) {
      const queryValue = query[queryKey]
      if (queryKey[0] === '$') {
        if (!logicalOperators[queryKey]) throw new Error(`Unknown logical operator ${queryKey}`)
        if (!logicalOperators[queryKey](obj, queryValue)) return false
      } else if (!matchQueryPart(obj, queryKey, queryValue)) return false
    }
  }

  return true
}

/**
 * Match an object against a specific { key: value } part of a query
 * if the treatObjAsValue flag is set, don't try to match every part separately, but the array as a whole
 * @param {object} obj
 * @param {string} queryKey
 * @param {*} queryValue
 * @param {boolean} [treatObjAsValue=false]
 * @return {boolean}
 * @private
 */
function matchQueryPart (obj, queryKey, queryValue, treatObjAsValue) {
  const objValue = getDotValue(obj, queryKey)

  // Check if the value is an array if we don't force a treatment as value
  if (Array.isArray(objValue) && !treatObjAsValue) {
    // If the queryValue is an array, try to perform an exact match
    if (Array.isArray(queryValue)) return matchQueryPart(obj, queryKey, queryValue, true)

    // Check if we are using an array-specific comparison function
    if (queryValue !== null && typeof queryValue === 'object' && !isRegExp(queryValue)) {
      for (const key in queryValue) {
        if (Object.prototype.hasOwnProperty.call(queryValue, key) && arrayComparisonFunctions[key]) { return matchQueryPart(obj, queryKey, queryValue, true) }
      }
    }

    // If not, treat it as an array of { obj, query } where there needs to be at least one match
    for (const el of objValue) {
      if (matchQueryPart({ k: el }, 'k', queryValue)) return true // k here could be any string
    }
    return false
  }

  // queryValue is an actual object. Determine whether it contains comparison operators
  // or only normal fields. Mixed objects are not allowed
  if (queryValue !== null && typeof queryValue === 'object' && !isRegExp(queryValue) && !Array.isArray(queryValue)) {
    const keys = Object.keys(queryValue)
    const firstChars = keys.map(item => item[0])
    const dollarFirstChars = firstChars.filter(c => c === '$')

    if (dollarFirstChars.length !== 0 && dollarFirstChars.length !== firstChars.length) throw new Error('You cannot mix operators and normal fields')

    // queryValue is an object of this form: { $comparisonOperator1: value1, ... }
    if (dollarFirstChars.length > 0) {
      for (const key of keys) {
        if (!comparisonFunctions[key]) throw new Error(`Unknown comparison function ${key}`)

        if (!comparisonFunctions[key](objValue, queryValue[key])) return false
      }
      return true
    }
  }

  // Using regular expressions with basic querying
  if (isRegExp(queryValue)) return comparisonFunctions.$regex(objValue, queryValue)

  // queryValue is either a native value or a normal object
  // Basic matching is possible
  return areThingsEqual(objValue, queryValue)
}

// Interface
module.exports.serialize = serialize
module.exports.deserialize = deserialize
module.exports.deepCopy = deepCopy
module.exports.checkObject = checkObject
module.exports.isPrimitiveType = isPrimitiveType
module.exports.modify = modify
module.exports.getDotValue = getDotValue
module.exports.getDotValues = getDotValues
module.exports.match = match
module.exports.areThingsEqual = areThingsEqual
module.exports.compareThings = compareThings

}, {"./utils.js":7})

// lib/utils.js
__DEFINE__(7, function (__LOCAL_REQUIRE__, module, exports) {
/**
 * Utility functions for all environments.
 * This replaces the underscore dependency.
 *
 * @module utils
 * @private
 */

/**
 * @callback IterateeFunction
 * @param {*} arg
 * @return {*}
 */

/**
 * Produces a duplicate-free version of the array, using === to test object equality. In particular only the first
 * occurrence of each value is kept. If you want to compute unique items based on a transformation, pass an iteratee
 * function.
 *
 * Heavily inspired by {@link https://underscorejs.org/#uniq}.
 * @param {Array} array
 * @param {IterateeFunction} [iteratee] transformation applied to every element before checking for duplicates. This will not
 * transform the items in the result.
 * @return {Array}
 * @alias module:utils.uniq
 */
const uniq = (array, iteratee) => {
  if (iteratee) return [...(new Map(array.map(x => [iteratee(x), x]))).values()]
  else return [...new Set(array)]
}
/**
 * Returns true if arg is an Object. Note that JavaScript arrays and functions are objects, while (normal) strings
 * and numbers are not.
 *
 * Heavily inspired by {@link https://underscorejs.org/#isObject}.
 * @param {*} arg
 * @return {boolean}
 */
const isObject = arg => typeof arg === 'object' && arg !== null

/**
 * Returns true if d is a Date.
 *
 * Heavily inspired by {@link https://underscorejs.org/#isDate}.
 * @param {*} d
 * @return {boolean}
 * @alias module:utils.isDate
 */
const isDate = d => isObject(d) && Object.prototype.toString.call(d) === '[object Date]'

/**
 * Returns true if re is a RegExp.
 *
 * Heavily inspired by {@link https://underscorejs.org/#isRegExp}.
 * @param {*} re
 * @return {boolean}
 * @alias module:utils.isRegExp
 */
const isRegExp = re => isObject(re) && Object.prototype.toString.call(re) === '[object RegExp]'

/**
 * Return a copy of the object filtered using the given keys.
 *
 * @param {object} object
 * @param {string[]} keys
 * @return {object}
 */
const pick = (object, keys) => {
  return keys.reduce((obj, key) => {
    if (object && Object.prototype.hasOwnProperty.call(object, key)) {
      obj[key] = object[key]
    }
    return obj
  }, {})
}

const filterIndexNames = (indexNames) => ([k, v]) => !!(typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || isDate(v) || v === null) &&
indexNames.includes(k)

module.exports.uniq = uniq
module.exports.isDate = isDate
module.exports.isRegExp = isRegExp
module.exports.pick = pick
module.exports.filterIndexNames = filterIndexNames

}, {})

// browser-version/lib/customUtils.js
__DEFINE__(8, function (__LOCAL_REQUIRE__, module, exports) {
/**
 * Utility functions that need to be reimplemented for each environment.
 * This is the version for the browser & React-Native
 * @module customUtilsBrowser
 * @private
 */

/**
 * Taken from the crypto-browserify module
 * https://github.com/dominictarr/crypto-browserify
 * NOTE: Math.random() does not guarantee "cryptographic quality" but we actually don't need it
 * @param {number} size in bytes
 * @return {Array<number>}
 */
const randomBytes = size => {
  const bytes = new Array(size)

  for (let i = 0, r; i < size; i++) {
    if ((i & 0x03) === 0) r = Math.random() * 0x100000000
    bytes[i] = r >>> ((i & 0x03) << 3) & 0xff
  }

  return bytes
}

/**
 * Taken from the base64-js module
 * https://github.com/beatgammit/base64-js/
 * @param {Array} uint8
 * @return {string}
 */
const byteArrayToBase64 = uint8 => {
  const lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
  const extraBytes = uint8.length % 3 // if we have 1 byte left, pad 2 bytes
  let output = ''
  let temp

  const tripletToBase64 = num => lookup[num >> 18 & 0x3F] + lookup[num >> 12 & 0x3F] + lookup[num >> 6 & 0x3F] + lookup[num & 0x3F]

  // go through the array every three bytes, we'll deal with trailing stuff later
  for (let i = 0, length = uint8.length - extraBytes; i < length; i += 3) {
    temp = (uint8[i] << 16) + (uint8[i + 1] << 8) + (uint8[i + 2])
    output += tripletToBase64(temp)
  }

  // pad the end with zeros, but make sure to not forget the extra bytes
  if (extraBytes === 1) {
    temp = uint8[uint8.length - 1]
    output += lookup[temp >> 2]
    output += lookup[(temp << 4) & 0x3F]
    output += '=='
  } else if (extraBytes === 2) {
    temp = (uint8[uint8.length - 2] << 8) + (uint8[uint8.length - 1])
    output += lookup[temp >> 10]
    output += lookup[(temp >> 4) & 0x3F]
    output += lookup[(temp << 2) & 0x3F]
    output += '='
  }

  return output
}

/**
 * Return a random alphanumerical string of length len
 * There is a very small probability (less than 1/1,000,000) for the length to be less than len
 * (il the base64 conversion yields too many pluses and slashes) but
 * that's not an issue here
 * The probability of a collision is extremely small (need 3*10^12 documents to have one chance in a million of a collision)
 * See http://en.wikipedia.org/wiki/Birthday_problem
 * @param {number} len
 * @return {string}
 * @alias module:customUtilsNode.uid
 */
const uid = len => byteArrayToBase64(randomBytes(Math.ceil(Math.max(8, len * 2)))).replace(/[+/]/g, '').slice(0, len)

module.exports.uid = uid

}, {})

// lib/executor.js
__DEFINE__(9, function (__LOCAL_REQUIRE__, module, exports) {
const Waterfall = __LOCAL_REQUIRE__(10)

/**
 * Executes operations sequentially.
 * Has an option for a buffer that can be triggered afterwards.
 * @private
 */
class Executor {
  /**
   * Instantiates a new Executor.
   */
  constructor () {
    /**
     * If this.ready is `false`, then every task pushed will be buffered until this.processBuffer is called.
     * @type {boolean}
     * @private
     */
    this.ready = false
    /**
     * The main queue
     * @type {Waterfall}
     * @private
     */
    this.queue = new Waterfall()
    /**
     * The buffer queue
     * @type {Waterfall}
     * @private
     */
    this.buffer = null
    /**
     * Method to trigger the buffer processing.
     *
     * Do not be use directly, use `this.processBuffer` instead.
     * @function
     * @private
     */
    this._triggerBuffer = null
    this.resetBuffer()
  }

  /**
   * If executor is ready, queue task (and process it immediately if executor was idle)
   * If not, buffer task for later processing
   * @param {AsyncFunction} task Function to execute
   * @param {boolean} [forceQueuing = false] Optional (defaults to false) force executor to queue task even if it is not ready
   * @return {Promise<*>}
   * @async
   * @see Executor#push
   */
  pushAsync (task, forceQueuing = false) {
    if (this.ready || forceQueuing) return this.queue.waterfall(task)()
    else return this.buffer.waterfall(task)()
  }

  /**
   * Queue all tasks in buffer (in the same order they came in)
   * Automatically sets executor as ready
   */
  processBuffer () {
    this.ready = true
    this._triggerBuffer()
    this.queue.waterfall(() => this.buffer.guardian)
  }

  /**
   * Removes all tasks queued up in the buffer
   */
  resetBuffer () {
    this.buffer = new Waterfall()
    this.buffer.chain(new Promise(resolve => {
      this._triggerBuffer = resolve
    }))
    if (this.ready) this._triggerBuffer()
  }
}

// Interface
module.exports = Executor

}, {"./waterfall":10})

// lib/waterfall.js
__DEFINE__(10, function (__LOCAL_REQUIRE__, module, exports) {
/**
 * Responsible for sequentially executing actions on the database
 * @private
 */
class Waterfall {
  /**
   * Instantiate a new Waterfall.
   */
  constructor () {
    /**
     * This is the internal Promise object which resolves when all the tasks of the `Waterfall` are done.
     *
     * It will change any time `this.waterfall` is called.
     *
     * @type {Promise}
     */
    this.guardian = Promise.resolve()
  }

  /**
   *
   * @param {AsyncFunction} func
   * @return {AsyncFunction}
   */
  waterfall (func) {
    return (...args) => {
      this.guardian = this.guardian.then(() => {
        return func(...args)
          .then(result => ({ error: false, result }), result => ({ error: true, result }))
      })
      return this.guardian.then(({ error, result }) => {
        if (error) return Promise.reject(result)
        else return Promise.resolve(result)
      })
    }
  }

  /**
   * Shorthand for chaining a promise to the Waterfall
   * @param {Promise} promise
   * @return {Promise}
   */
  chain (promise) {
    return this.waterfall(() => promise)()
  }
}

module.exports = Waterfall

}, {})

// lib/indexes.js
__DEFINE__(11, function (__LOCAL_REQUIRE__, module, exports) {
const BinarySearchTree = __LOCAL_REQUIRE__(12).AVLTree
const model = __LOCAL_REQUIRE__(6)
const { uniq, isDate } = __LOCAL_REQUIRE__(7)

/**
 * Two indexed pointers are equal if they point to the same place
 * @param {*} a
 * @param {*} b
 * @return {boolean}
 * @private
 */
const checkValueEquality = (a, b) => a === b

/**
 * Type-aware projection
 * @param {*} elt
 * @return {string|*}
 * @private
 */
const projectForUnique = elt => {
  if (elt === null) return '$null'
  if (typeof elt === 'string') return '$string' + elt
  if (typeof elt === 'boolean') return '$boolean' + elt
  if (typeof elt === 'number') return '$number' + elt
  if (isDate(elt)) return '$date' + elt.getTime()

  return elt // Arrays and objects, will check for pointer equality
}

/**
 * Indexes on field names, with atomic operations and which can optionally enforce a unique constraint or allow indexed
 * fields to be undefined
 * @private
 */
class Index {
  /**
   * Create a new index
   * All methods on an index guarantee that either the whole operation was successful and the index changed
   * or the operation was unsuccessful and an error is thrown while the index is unchanged
   * @param {object} options
   * @param {string} options.fieldName On which field should the index apply, can use dot notation to index on sub fields, can use comma-separated notation to use compound indexes
   * @param {boolean} [options.unique = false] Enforces a unique constraint
   * @param {boolean} [options.sparse = false] Allows a sparse index (we can have documents for which fieldName is `undefined`)
   */
  constructor (options) {
    /**
     * On which field the index applies to, can use dot notation to index on sub fields, can use comma-separated notation to use compound indexes.
     * @type {string}
     */
    this.fieldName = options.fieldName

    if (typeof this.fieldName !== 'string') throw new Error('fieldName must be a string')

    /**
     * Internal property which is an Array representing the fieldName split with `,`, useful only for compound indexes.
     * @type {string[]}
     * @private
     */
    this._fields = this.fieldName.split(',')

    /**
     * Defines if the index enforces a unique constraint for this index.
     * @type {boolean}
     */
    this.unique = options.unique || false
    /**
     * Defines if we can have documents for which fieldName is `undefined`
     * @type {boolean}
     */
    this.sparse = options.sparse || false

    /**
     * Options object given to the underlying BinarySearchTree.
     * @type {{unique: boolean, checkValueEquality: (function(*, *): boolean), compareKeys: ((function(*, *, compareStrings): (number|number))|*)}}
     */
    this.treeOptions = { unique: this.unique, compareKeys: model.compareThings, checkValueEquality }

    /**
     * Underlying BinarySearchTree for this index. Uses an AVLTree for optimization.
     * @type {AVLTree}
     */
    this.tree = new BinarySearchTree(this.treeOptions)
  }

  /**
   * Reset an index
   * @param {?document|?document[]} [newData] Data to initialize the index with. If an error is thrown during
   * insertion, the index is not modified.
   */
  reset (newData) {
    this.tree = new BinarySearchTree(this.treeOptions)

    if (newData) this.insert(newData)
  }

  /**
   * Insert a new document in the index
   * If an array is passed, we insert all its elements (if one insertion fails the index is not modified)
   * O(log(n))
   * @param {document|document[]} doc The document, or array of documents, to insert.
   */
  insert (doc) {
    let keys
    let failingIndex
    let error

    if (Array.isArray(doc)) {
      this.insertMultipleDocs(doc)
      return
    }

    const key = model.getDotValues(doc, this._fields)

    // We don't index documents that don't contain the field if the index is sparse
    if ((key === undefined || (typeof key === 'object' && key !== null && Object.values(key).every(el => el === undefined))) && this.sparse) return

    if (!Array.isArray(key)) this.tree.insert(key, doc)
    else {
      // If an insert fails due to a unique constraint, roll back all inserts before it
      keys = uniq(key, projectForUnique)

      for (let i = 0; i < keys.length; i += 1) {
        try {
          this.tree.insert(keys[i], doc)
        } catch (e) {
          error = e
          failingIndex = i
          break
        }
      }

      if (error) {
        for (let i = 0; i < failingIndex; i += 1) {
          this.tree.delete(keys[i], doc)
        }

        throw error
      }
    }
  }

  /**
   * Insert an array of documents in the index
   * If a constraint is violated, the changes should be rolled back and an error thrown
   * @param {document[]} docs Array of documents to insert.
   * @private
   */
  insertMultipleDocs (docs) {
    let error
    let failingIndex

    for (let i = 0; i < docs.length; i += 1) {
      try {
        this.insert(docs[i])
      } catch (e) {
        error = e
        failingIndex = i
        break
      }
    }

    if (error) {
      for (let i = 0; i < failingIndex; i += 1) {
        this.remove(docs[i])
      }

      throw error
    }
  }

  /**
   * Removes a document from the index.
   * If an array is passed, we remove all its elements
   * The remove operation is safe with regards to the 'unique' constraint
   * O(log(n))
   * @param {document[]|document} doc The document, or Array of documents, to remove.
   */
  remove (doc) {
    if (Array.isArray(doc)) {
      doc.forEach(d => { this.remove(d) })
      return
    }

    const key = model.getDotValues(doc, this._fields)
    if (key === undefined && this.sparse) return

    if (!Array.isArray(key)) {
      this.tree.delete(key, doc)
    } else {
      uniq(key, projectForUnique).forEach(_key => {
        this.tree.delete(_key, doc)
      })
    }
  }

  /**
   * Update a document in the index
   * If a constraint is violated, changes are rolled back and an error thrown
   * Naive implementation, still in O(log(n))
   * @param {document|Array.<{oldDoc: document, newDoc: document}>} oldDoc Document to update, or an `Array` of
   * `{oldDoc, newDoc}` pairs.
   * @param {document} [newDoc] Document to replace the oldDoc with. If the first argument is an `Array` of
   * `{oldDoc, newDoc}` pairs, this second argument is ignored.
   */
  update (oldDoc, newDoc) {
    if (Array.isArray(oldDoc)) {
      this.updateMultipleDocs(oldDoc)
      return
    }

    this.remove(oldDoc)

    try {
      this.insert(newDoc)
    } catch (e) {
      this.insert(oldDoc)
      throw e
    }
  }

  /**
   * Update multiple documents in the index
   * If a constraint is violated, the changes need to be rolled back
   * and an error thrown
   * @param {Array.<{oldDoc: document, newDoc: document}>} pairs
   *
   * @private
   */
  updateMultipleDocs (pairs) {
    let failingIndex
    let error

    for (let i = 0; i < pairs.length; i += 1) {
      this.remove(pairs[i].oldDoc)
    }

    for (let i = 0; i < pairs.length; i += 1) {
      try {
        this.insert(pairs[i].newDoc)
      } catch (e) {
        error = e
        failingIndex = i
        break
      }
    }

    // If an error was raised, roll back changes in the inverse order
    if (error) {
      for (let i = 0; i < failingIndex; i += 1) {
        this.remove(pairs[i].newDoc)
      }

      for (let i = 0; i < pairs.length; i += 1) {
        this.insert(pairs[i].oldDoc)
      }

      throw error
    }
  }

  /**
   * Revert an update
   * @param {document|Array.<{oldDoc: document, newDoc: document}>} oldDoc Document to revert to, or an `Array` of `{oldDoc, newDoc}` pairs.
   * @param {document} [newDoc] Document to revert from. If the first argument is an Array of {oldDoc, newDoc}, this second argument is ignored.
   */
  revertUpdate (oldDoc, newDoc) {
    const revert = []

    if (!Array.isArray(oldDoc)) this.update(newDoc, oldDoc)
    else {
      oldDoc.forEach(pair => {
        revert.push({ oldDoc: pair.newDoc, newDoc: pair.oldDoc })
      })
      this.update(revert)
    }
  }

  /**
   * Get all documents in index whose key match value (if it is a Thing) or one of the elements of value (if it is an array of Things)
   * @param {Array.<*>|*} value Value to match the key against
   * @return {document[]}
   */
  getMatching (value) {
    if (!Array.isArray(value)) return this.tree.search(value)
    else {
      const _res = {}
      const res = []

      value.forEach(v => {
        this.getMatching(v).forEach(doc => {
          _res[doc._id] = doc
        })
      })

      Object.keys(_res).forEach(_id => {
        res.push(_res[_id])
      })

      return res
    }
  }

  /**
   * Get all documents in index whose key is between bounds are they are defined by query
   * Documents are sorted by key
   * @param {object} query An object with at least one matcher among $gt, $gte, $lt, $lte.
   * @param {*} [query.$gt] Greater than matcher.
   * @param {*} [query.$gte] Greater than or equal matcher.
   * @param {*} [query.$lt] Lower than matcher.
   * @param {*} [query.$lte] Lower than or equal matcher.
   * @return {document[]}
   */
  getBetweenBounds (query) {
    return this.tree.betweenBounds(query)
  }

  /**
   * Get all elements in the index
   * @return {document[]}
   */
  getAll () {
    const res = []

    this.tree.executeOnEveryNode(node => {
      res.push(...node.data)
    })

    return res
  }
}

// Interface
module.exports = Index

}, {"@seald-io/binary-search-tree":12,"./model.js":6,"./utils.js":7})

// node_modules/@seald-io/binary-search-tree/index.js
__DEFINE__(12, function (__LOCAL_REQUIRE__, module, exports) {
module.exports.BinarySearchTree = __LOCAL_REQUIRE__(13)
module.exports.AVLTree = __LOCAL_REQUIRE__(15)

}, {"./lib/bst":13,"./lib/avltree":15})

// node_modules/@seald-io/binary-search-tree/lib/bst.js
__DEFINE__(13, function (__LOCAL_REQUIRE__, module, exports) {
/**
 * Simple binary search tree
 */
const customUtils = __LOCAL_REQUIRE__(14)

class BinarySearchTree {
  /**
   * Constructor
   * @param {Object} options Optional
   * @param {Boolean}  options.unique Whether to enforce a 'unique' constraint on the key or not
   * @param {Key}      options.key Initialize this BST's key with key
   * @param {Value}    options.value Initialize this BST's data with [value]
   * @param {Function} options.compareKeys Initialize this BST's compareKeys
   */
  constructor (options) {
    options = options || {}

    this.left = null
    this.right = null
    this.parent = options.parent !== undefined ? options.parent : null
    if (Object.prototype.hasOwnProperty.call(options, 'key')) { this.key = options.key }
    this.data = Object.prototype.hasOwnProperty.call(options, 'value') ? [options.value] : []
    this.unique = options.unique || false

    this.compareKeys = options.compareKeys || customUtils.defaultCompareKeysFunction
    this.checkValueEquality = options.checkValueEquality || customUtils.defaultCheckValueEquality
  }

  /**
   * Get the descendant with max key
   */
  getMaxKeyDescendant () {
    if (this.right) return this.right.getMaxKeyDescendant()
    else return this
  }

  /**
   * Get the maximum key
   */
  getMaxKey () {
    return this.getMaxKeyDescendant().key
  }

  /**
   * Get the descendant with min key
   */
  getMinKeyDescendant () {
    if (this.left) return this.left.getMinKeyDescendant()
    else return this
  }

  /**
   * Get the minimum key
   */
  getMinKey () {
    return this.getMinKeyDescendant().key
  }

  /**
   * Check that all nodes (incl. leaves) fullfil condition given by fn
   * test is a function passed every (key, data) and which throws if the condition is not met
   */
  checkAllNodesFullfillCondition (test) {
    if (!Object.prototype.hasOwnProperty.call(this, 'key')) return

    test(this.key, this.data)
    if (this.left) this.left.checkAllNodesFullfillCondition(test)
    if (this.right) this.right.checkAllNodesFullfillCondition(test)
  }

  /**
   * Check that the core BST properties on node ordering are verified
   * Throw if they aren't
   */
  checkNodeOrdering () {
    if (!Object.prototype.hasOwnProperty.call(this, 'key')) return

    if (this.left) {
      this.left.checkAllNodesFullfillCondition(k => {
        if (this.compareKeys(k, this.key) >= 0) throw new Error(`Tree with root ${this.key} is not a binary search tree`)
      })
      this.left.checkNodeOrdering()
    }

    if (this.right) {
      this.right.checkAllNodesFullfillCondition(k => {
        if (this.compareKeys(k, this.key) <= 0) throw new Error(`Tree with root ${this.key} is not a binary search tree`)
      })
      this.right.checkNodeOrdering()
    }
  }

  /**
   * Check that all pointers are coherent in this tree
   */
  checkInternalPointers () {
    if (this.left) {
      if (this.left.parent !== this) throw new Error(`Parent pointer broken for key ${this.key}`)
      this.left.checkInternalPointers()
    }

    if (this.right) {
      if (this.right.parent !== this) throw new Error(`Parent pointer broken for key ${this.key}`)
      this.right.checkInternalPointers()
    }
  }

  /**
   * Check that a tree is a BST as defined here (node ordering and pointer references)
   */
  checkIsBST () {
    this.checkNodeOrdering()
    this.checkInternalPointers()
    if (this.parent) throw new Error("The root shouldn't have a parent")
  }

  /**
   * Get number of keys inserted
   */
  getNumberOfKeys () {
    let res

    if (!Object.prototype.hasOwnProperty.call(this, 'key')) return 0

    res = 1
    if (this.left) res += this.left.getNumberOfKeys()
    if (this.right) res += this.right.getNumberOfKeys()

    return res
  }

  /**
   * Create a BST similar (i.e. same options except for key and value) to the current one
   * Use the same constructor (i.e. BinarySearchTree, AVLTree etc)
   * @param {Object} options see constructor
   */
  createSimilar (options) {
    options = options || {}
    options.unique = this.unique
    options.compareKeys = this.compareKeys
    options.checkValueEquality = this.checkValueEquality

    return new this.constructor(options)
  }

  /**
   * Create the left child of this BST and return it
   */
  createLeftChild (options) {
    const leftChild = this.createSimilar(options)
    leftChild.parent = this
    this.left = leftChild

    return leftChild
  }

  /**
   * Create the right child of this BST and return it
   */
  createRightChild (options) {
    const rightChild = this.createSimilar(options)
    rightChild.parent = this
    this.right = rightChild

    return rightChild
  }

  /**
   * Insert a new element
   */
  insert (key, value) {
    // Empty tree, insert as root
    if (!Object.prototype.hasOwnProperty.call(this, 'key')) {
      this.key = key
      this.data.push(value)
      return
    }

    // Same key as root
    if (this.compareKeys(this.key, key) === 0) {
      if (this.unique) {
        const err = new Error(`Can't insert key ${JSON.stringify(key)}, it violates the unique constraint`)
        err.key = key
        err.errorType = 'uniqueViolated'
        throw err
      } else this.data.push(value)
      return
    }

    if (this.compareKeys(key, this.key) < 0) {
      // Insert in left subtree
      if (this.left) this.left.insert(key, value)
      else this.createLeftChild({ key: key, value: value })
    } else {
      // Insert in right subtree
      if (this.right) this.right.insert(key, value)
      else this.createRightChild({ key: key, value: value })
    }
  }

  /**
   * Search for all data corresponding to a key
   */
  search (key) {
    if (!Object.prototype.hasOwnProperty.call(this, 'key')) return []

    if (this.compareKeys(this.key, key) === 0) return this.data

    if (this.compareKeys(key, this.key) < 0) {
      if (this.left) return this.left.search(key)
      else return []
    } else {
      if (this.right) return this.right.search(key)
      else return []
    }
  }

  /**
   * Return a function that tells whether a given key matches a lower bound
   */
  getLowerBoundMatcher (query) {
    // No lower bound
    if (!Object.prototype.hasOwnProperty.call(query, '$gt') && !Object.prototype.hasOwnProperty.call(query, '$gte')) return () => true

    if (Object.prototype.hasOwnProperty.call(query, '$gt') && Object.prototype.hasOwnProperty.call(query, '$gte')) {
      if (this.compareKeys(query.$gte, query.$gt) === 0) return key => this.compareKeys(key, query.$gt) > 0

      if (this.compareKeys(query.$gte, query.$gt) > 0) return key => this.compareKeys(key, query.$gte) >= 0
      else return key => this.compareKeys(key, query.$gt) > 0
    }

    if (Object.prototype.hasOwnProperty.call(query, '$gt')) return key => this.compareKeys(key, query.$gt) > 0
    else return key => this.compareKeys(key, query.$gte) >= 0
  }

  /**
   * Return a function that tells whether a given key matches an upper bound
   */
  getUpperBoundMatcher (query) {
    // No lower bound
    if (!Object.prototype.hasOwnProperty.call(query, '$lt') && !Object.prototype.hasOwnProperty.call(query, '$lte')) return () => true

    if (Object.prototype.hasOwnProperty.call(query, '$lt') && Object.prototype.hasOwnProperty.call(query, '$lte')) {
      if (this.compareKeys(query.$lte, query.$lt) === 0) return key => this.compareKeys(key, query.$lt) < 0

      if (this.compareKeys(query.$lte, query.$lt) < 0) return key => this.compareKeys(key, query.$lte) <= 0
      else return key => this.compareKeys(key, query.$lt) < 0
    }

    if (Object.prototype.hasOwnProperty.call(query, '$lt')) return key => this.compareKeys(key, query.$lt) < 0
    else return key => this.compareKeys(key, query.$lte) <= 0
  }

  /**
   * Get all data for a key between bounds
   * Return it in key order
   * @param {Object} query Mongo-style query where keys are $lt, $lte, $gt or $gte (other keys are not considered)
   * @param {Functions} lbm/ubm matching functions calculated at the first recursive step
   */
  betweenBounds (query, lbm, ubm) {
    const res = []

    if (!Object.prototype.hasOwnProperty.call(this, 'key')) return [] // Empty tree

    lbm = lbm || this.getLowerBoundMatcher(query)
    ubm = ubm || this.getUpperBoundMatcher(query)

    if (lbm(this.key) && this.left) append(res, this.left.betweenBounds(query, lbm, ubm))
    if (lbm(this.key) && ubm(this.key)) append(res, this.data)
    if (ubm(this.key) && this.right) append(res, this.right.betweenBounds(query, lbm, ubm))

    return res
  }

  /**
   * Delete the current node if it is a leaf
   * Return true if it was deleted
   */
  deleteIfLeaf () {
    if (this.left || this.right) return false

    // The leaf is itself a root
    if (!this.parent) {
      delete this.key
      this.data = []
      return true
    }

    if (this.parent.left === this) this.parent.left = null
    else this.parent.right = null

    return true
  }

  /**
   * Delete the current node if it has only one child
   * Return true if it was deleted
   */
  deleteIfOnlyOneChild () {
    let child

    if (this.left && !this.right) child = this.left
    if (!this.left && this.right) child = this.right
    if (!child) return false

    // Root
    if (!this.parent) {
      this.key = child.key
      this.data = child.data

      this.left = null
      if (child.left) {
        this.left = child.left
        child.left.parent = this
      }

      this.right = null
      if (child.right) {
        this.right = child.right
        child.right.parent = this
      }

      return true
    }

    if (this.parent.left === this) {
      this.parent.left = child
      child.parent = this.parent
    } else {
      this.parent.right = child
      child.parent = this.parent
    }

    return true
  }

  /**
   * Delete a key or just a value
   * @param {Key} key
   * @param {Value} value Optional. If not set, the whole key is deleted. If set, only this value is deleted
   */
  delete (key, value) {
    const newData = []
    let replaceWith

    if (!Object.prototype.hasOwnProperty.call(this, 'key')) return

    if (this.compareKeys(key, this.key) < 0) {
      if (this.left) this.left.delete(key, value)
      return
    }

    if (this.compareKeys(key, this.key) > 0) {
      if (this.right) this.right.delete(key, value)
      return
    }

    if (!this.compareKeys(key, this.key) === 0) return

    // Delete only a value
    if (this.data.length > 1 && value !== undefined) {
      this.data.forEach(d => {
        if (!this.checkValueEquality(d, value)) newData.push(d)
      })
      this.data = newData
      return
    }

    // Delete the whole node
    if (this.deleteIfLeaf()) return

    if (this.deleteIfOnlyOneChild()) return

    // We are in the case where the node to delete has two children
    if (Math.random() >= 0.5) { // Randomize replacement to avoid unbalancing the tree too much
      // Use the in-order predecessor
      replaceWith = this.left.getMaxKeyDescendant()

      this.key = replaceWith.key
      this.data = replaceWith.data

      if (this === replaceWith.parent) { // Special case
        this.left = replaceWith.left
        if (replaceWith.left) replaceWith.left.parent = replaceWith.parent
      } else {
        replaceWith.parent.right = replaceWith.left
        if (replaceWith.left) replaceWith.left.parent = replaceWith.parent
      }
    } else {
      // Use the in-order successor
      replaceWith = this.right.getMinKeyDescendant()

      this.key = replaceWith.key
      this.data = replaceWith.data

      if (this === replaceWith.parent) { // Special case
        this.right = replaceWith.right
        if (replaceWith.right) replaceWith.right.parent = replaceWith.parent
      } else {
        replaceWith.parent.left = replaceWith.right
        if (replaceWith.right) replaceWith.right.parent = replaceWith.parent
      }
    }
  }

  /**
   * Execute a function on every node of the tree, in key order
   * @param {Function} fn Signature: node. Most useful will probably be node.key and node.data
   */
  executeOnEveryNode (fn) {
    if (this.left) this.left.executeOnEveryNode(fn)
    fn(this)
    if (this.right) this.right.executeOnEveryNode(fn)
  }

  /**
   * Pretty print a tree
   * @param {Boolean} printData To print the nodes' data along with the key
   */
  prettyPrint (printData, spacing) {
    spacing = spacing || ''

    console.log(`${spacing}* ${this.key}`)
    if (printData) console.log(`${spacing}* ${this.data}`)

    if (!this.left && !this.right) return

    if (this.left) this.left.prettyPrint(printData, `${spacing}  `)
    else console.log(`${spacing}  *`)

    if (this.right) this.right.prettyPrint(printData, `${spacing}  `)
    else console.log(`${spacing}  *`)
  }
}

// ================================
// Methods used to test the tree
// ================================

// ============================================
// Methods used to actually work on the tree
// ============================================

// Append all elements in toAppend to array
function append (array, toAppend) {
  for (let i = 0; i < toAppend.length; i += 1) {
    array.push(toAppend[i])
  }
}

// Interface
module.exports = BinarySearchTree

}, {"./customUtils":14})

// node_modules/@seald-io/binary-search-tree/lib/customUtils.js
__DEFINE__(14, function (__LOCAL_REQUIRE__, module, exports) {
/**
 * Return an array with the numbers from 0 to n-1, in a random order
 */
const getRandomArray = n => {
  if (n === 0) return []
  if (n === 1) return [0]

  const res = getRandomArray(n - 1)
  const next = Math.floor(Math.random() * n)
  res.splice(next, 0, n - 1) // Add n-1 at a random position in the array

  return res
}

module.exports.getRandomArray = getRandomArray

/*
 * Default compareKeys function will work for numbers, strings and dates
 */
const defaultCompareKeysFunction = (a, b) => {
  if (a < b) return -1
  if (a > b) return 1
  if (a === b) return 0

  const err = new Error("Couldn't compare elements")
  err.a = a
  err.b = b
  throw err
}

module.exports.defaultCompareKeysFunction = defaultCompareKeysFunction

/**
 * Check whether two values are equal (used in non-unique deletion)
 */
const defaultCheckValueEquality = (a, b) => a === b

module.exports.defaultCheckValueEquality = defaultCheckValueEquality

}, {})

// node_modules/@seald-io/binary-search-tree/lib/avltree.js
__DEFINE__(15, function (__LOCAL_REQUIRE__, module, exports) {
/**
 * Self-balancing binary search tree using the AVL implementation
 */
const BinarySearchTree = __LOCAL_REQUIRE__(13)
const customUtils = __LOCAL_REQUIRE__(14)

class AVLTree {
  /**
   * Constructor
   * We can't use a direct pointer to the root node (as in the simple binary search tree)
   * as the root will change during tree rotations
   * @param {Boolean}  options.unique Whether to enforce a 'unique' constraint on the key or not
   * @param {Function} options.compareKeys Initialize this BST's compareKeys
   */
  constructor (options) {
    this.tree = new _AVLTree(options)
  }

  checkIsAVLT () { this.tree.checkIsAVLT() }

  // Insert in the internal tree, update the pointer to the root if needed
  insert (key, value) {
    const newTree = this.tree.insert(key, value)

    // If newTree is undefined, that means its structure was not modified
    if (newTree) { this.tree = newTree }
  }

  // Delete a value
  delete (key, value) {
    const newTree = this.tree.delete(key, value)

    // If newTree is undefined, that means its structure was not modified
    if (newTree) { this.tree = newTree }
  }
}

class _AVLTree extends BinarySearchTree {
  /**
   * Constructor of the internal AVLTree
   * @param {Object} options Optional
   * @param {Boolean}  options.unique Whether to enforce a 'unique' constraint on the key or not
   * @param {Key}      options.key Initialize this BST's key with key
   * @param {Value}    options.value Initialize this BST's data with [value]
   * @param {Function} options.compareKeys Initialize this BST's compareKeys
   */
  constructor (options) {
    super()
    options = options || {}

    this.left = null
    this.right = null
    this.parent = options.parent !== undefined ? options.parent : null
    if (Object.prototype.hasOwnProperty.call(options, 'key')) this.key = options.key
    this.data = Object.prototype.hasOwnProperty.call(options, 'value') ? [options.value] : []
    this.unique = options.unique || false

    this.compareKeys = options.compareKeys || customUtils.defaultCompareKeysFunction
    this.checkValueEquality = options.checkValueEquality || customUtils.defaultCheckValueEquality
  }

  /**
   * Check the recorded height is correct for every node
   * Throws if one height doesn't match
   */
  checkHeightCorrect () {
    if (!Object.prototype.hasOwnProperty.call(this, 'key')) { return } // Empty tree

    if (this.left && this.left.height === undefined) { throw new Error('Undefined height for node ' + this.left.key) }
    if (this.right && this.right.height === undefined) { throw new Error('Undefined height for node ' + this.right.key) }
    if (this.height === undefined) { throw new Error('Undefined height for node ' + this.key) }

    const leftH = this.left ? this.left.height : 0
    const rightH = this.right ? this.right.height : 0

    if (this.height !== 1 + Math.max(leftH, rightH)) { throw new Error('Height constraint failed for node ' + this.key) }
    if (this.left) { this.left.checkHeightCorrect() }
    if (this.right) { this.right.checkHeightCorrect() }
  }

  /**
   * Return the balance factor
   */
  balanceFactor () {
    const leftH = this.left ? this.left.height : 0
    const rightH = this.right ? this.right.height : 0
    return leftH - rightH
  }

  /**
   * Check that the balance factors are all between -1 and 1
   */
  checkBalanceFactors () {
    if (Math.abs(this.balanceFactor()) > 1) { throw new Error('Tree is unbalanced at node ' + this.key) }

    if (this.left) { this.left.checkBalanceFactors() }
    if (this.right) { this.right.checkBalanceFactors() }
  }

  /**
   * When checking if the BST conditions are met, also check that the heights are correct
   * and the tree is balanced
   */
  checkIsAVLT () {
    super.checkIsBST()
    this.checkHeightCorrect()
    this.checkBalanceFactors()
  }

  /**
   * Perform a right rotation of the tree if possible
   * and return the root of the resulting tree
   * The resulting tree's nodes' heights are also updated
   */
  rightRotation () {
    const q = this
    const p = this.left

    if (!p) return q // No change

    const b = p.right

    // Alter tree structure
    if (q.parent) {
      p.parent = q.parent
      if (q.parent.left === q) q.parent.left = p
      else q.parent.right = p
    } else {
      p.parent = null
    }
    p.right = q
    q.parent = p
    q.left = b
    if (b) { b.parent = q }

    // Update heights
    const ah = p.left ? p.left.height : 0
    const bh = b ? b.height : 0
    const ch = q.right ? q.right.height : 0
    q.height = Math.max(bh, ch) + 1
    p.height = Math.max(ah, q.height) + 1

    return p
  }

  /**
   * Perform a left rotation of the tree if possible
   * and return the root of the resulting tree
   * The resulting tree's nodes' heights are also updated
   */
  leftRotation () {
    const p = this
    const q = this.right

    if (!q) { return this } // No change

    const b = q.left

    // Alter tree structure
    if (p.parent) {
      q.parent = p.parent
      if (p.parent.left === p) p.parent.left = q
      else p.parent.right = q
    } else {
      q.parent = null
    }
    q.left = p
    p.parent = q
    p.right = b
    if (b) { b.parent = p }

    // Update heights
    const ah = p.left ? p.left.height : 0
    const bh = b ? b.height : 0
    const ch = q.right ? q.right.height : 0
    p.height = Math.max(ah, bh) + 1
    q.height = Math.max(ch, p.height) + 1

    return q
  }

  /**
   * Modify the tree if its right subtree is too small compared to the left
   * Return the new root if any
   */
  rightTooSmall () {
    if (this.balanceFactor() <= 1) return this // Right is not too small, don't change

    if (this.left.balanceFactor() < 0) this.left.leftRotation()

    return this.rightRotation()
  }

  /**
   * Modify the tree if its left subtree is too small compared to the right
   * Return the new root if any
   */
  leftTooSmall () {
    if (this.balanceFactor() >= -1) { return this } // Left is not too small, don't change

    if (this.right.balanceFactor() > 0) this.right.rightRotation()

    return this.leftRotation()
  }

  /**
   * Rebalance the tree along the given path. The path is given reversed (as he was calculated
   * in the insert and delete functions).
   * Returns the new root of the tree
   * Of course, the first element of the path must be the root of the tree
   */
  rebalanceAlongPath (path) {
    let newRoot = this
    let rotated
    let i

    if (!Object.prototype.hasOwnProperty.call(this, 'key')) {
      delete this.height
      return this
    } // Empty tree

    // Rebalance the tree and update all heights
    for (i = path.length - 1; i >= 0; i -= 1) {
      path[i].height = 1 + Math.max(path[i].left ? path[i].left.height : 0, path[i].right ? path[i].right.height : 0)

      if (path[i].balanceFactor() > 1) {
        rotated = path[i].rightTooSmall()
        if (i === 0) newRoot = rotated
      }

      if (path[i].balanceFactor() < -1) {
        rotated = path[i].leftTooSmall()
        if (i === 0) newRoot = rotated
      }
    }

    return newRoot
  }

  /**
   * Insert a key, value pair in the tree while maintaining the AVL tree height constraint
   * Return a pointer to the root node, which may have changed
   */
  insert (key, value) {
    const insertPath = []
    let currentNode = this

    // Empty tree, insert as root
    if (!Object.prototype.hasOwnProperty.call(this, 'key')) {
      this.key = key
      this.data.push(value)
      this.height = 1
      return this
    }

    // Insert new leaf at the right place
    while (true) {
      // Same key: no change in the tree structure
      if (currentNode.compareKeys(currentNode.key, key) === 0) {
        if (currentNode.unique) {
          const err = new Error(`Can't insert key ${JSON.stringify(key)}, it violates the unique constraint`)
          err.key = key
          err.errorType = 'uniqueViolated'
          throw err
        } else currentNode.data.push(value)
        return this
      }

      insertPath.push(currentNode)

      if (currentNode.compareKeys(key, currentNode.key) < 0) {
        if (!currentNode.left) {
          insertPath.push(currentNode.createLeftChild({ key: key, value: value }))
          break
        } else currentNode = currentNode.left
      } else {
        if (!currentNode.right) {
          insertPath.push(currentNode.createRightChild({ key: key, value: value }))
          break
        } else currentNode = currentNode.right
      }
    }

    return this.rebalanceAlongPath(insertPath)
  }

  /**
   * Delete a key or just a value and return the new root of the tree
   * @param {Key} key
   * @param {Value} value Optional. If not set, the whole key is deleted. If set, only this value is deleted
   */
  delete (key, value) {
    const newData = []
    let replaceWith
    let currentNode = this
    const deletePath = []

    if (!Object.prototype.hasOwnProperty.call(this, 'key')) return this // Empty tree

    // Either no match is found and the function will return from within the loop
    // Or a match is found and deletePath will contain the path from the root to the node to delete after the loop
    while (true) {
      if (currentNode.compareKeys(key, currentNode.key) === 0) { break }

      deletePath.push(currentNode)

      if (currentNode.compareKeys(key, currentNode.key) < 0) {
        if (currentNode.left) {
          currentNode = currentNode.left
        } else return this // Key not found, no modification
      } else {
        // currentNode.compareKeys(key, currentNode.key) is > 0
        if (currentNode.right) {
          currentNode = currentNode.right
        } else return this // Key not found, no modification
      }
    }

    // Delete only a value (no tree modification)
    if (currentNode.data.length > 1 && value !== undefined) {
      currentNode.data.forEach(function (d) {
        if (!currentNode.checkValueEquality(d, value)) newData.push(d)
      })
      currentNode.data = newData
      return this
    }

    // Delete a whole node

    // Leaf
    if (!currentNode.left && !currentNode.right) {
      if (currentNode === this) { // This leaf is also the root
        delete currentNode.key
        currentNode.data = []
        delete currentNode.height
        return this
      } else {
        if (currentNode.parent.left === currentNode) currentNode.parent.left = null
        else currentNode.parent.right = null
        return this.rebalanceAlongPath(deletePath)
      }
    }

    // Node with only one child
    if (!currentNode.left || !currentNode.right) {
      replaceWith = currentNode.left ? currentNode.left : currentNode.right

      if (currentNode === this) { // This node is also the root
        replaceWith.parent = null
        return replaceWith // height of replaceWith is necessarily 1 because the tree was balanced before deletion
      } else {
        if (currentNode.parent.left === currentNode) {
          currentNode.parent.left = replaceWith
          replaceWith.parent = currentNode.parent
        } else {
          currentNode.parent.right = replaceWith
          replaceWith.parent = currentNode.parent
        }

        return this.rebalanceAlongPath(deletePath)
      }
    }

    // Node with two children
    // Use the in-order predecessor (no need to randomize since we actively rebalance)
    deletePath.push(currentNode)
    replaceWith = currentNode.left

    // Special case: the in-order predecessor is right below the node to delete
    if (!replaceWith.right) {
      currentNode.key = replaceWith.key
      currentNode.data = replaceWith.data
      currentNode.left = replaceWith.left
      if (replaceWith.left) { replaceWith.left.parent = currentNode }
      return this.rebalanceAlongPath(deletePath)
    }

    // After this loop, replaceWith is the right-most leaf in the left subtree
    // and deletePath the path from the root (inclusive) to replaceWith (exclusive)
    while (true) {
      if (replaceWith.right) {
        deletePath.push(replaceWith)
        replaceWith = replaceWith.right
      } else break
    }

    currentNode.key = replaceWith.key
    currentNode.data = replaceWith.data

    replaceWith.parent.right = replaceWith.left
    if (replaceWith.left) replaceWith.left.parent = replaceWith.parent

    return this.rebalanceAlongPath(deletePath)
  }
}

/**
 * Keep a pointer to the internal tree constructor for testing purposes
 */
AVLTree._AVLTree = _AVLTree;

/**
 * Other functions we want to use on an AVLTree as if it were the internal _AVLTree
 */
['getNumberOfKeys', 'search', 'betweenBounds', 'prettyPrint', 'executeOnEveryNode'].forEach(function (fn) {
  AVLTree.prototype[fn] = function () {
    return this.tree[fn].apply(this.tree, arguments)
  }
})

// Interface
module.exports = AVLTree

}, {"./bst":13,"./customUtils":14})

// lib/persistence.js
__DEFINE__(16, function (__LOCAL_REQUIRE__, module, exports) {
const { deprecate } = __LOCAL_REQUIRE__(4)
const byline = __LOCAL_REQUIRE__(17)
const Index = __LOCAL_REQUIRE__(11)
const model = __LOCAL_REQUIRE__(6)
const storage = __LOCAL_REQUIRE__(18)
const Waterfall = __LOCAL_REQUIRE__(10)

const DEFAULT_DIR_MODE = 0o755
const DEFAULT_FILE_MODE = 0o644

/**
 * Under the hood, NeDB's persistence uses an append-only format, meaning that all
 * updates and deletes actually result in lines added at the end of the datafile,
 * for performance reasons. The database is automatically compacted (i.e. put back
 * in the one-line-per-document format) every time you load each database within
 * your application.
 *
 * Persistence handles the compaction exposed in the Datastore {@link Datastore#compactDatafileAsync},
 * {@link Datastore#setAutocompactionInterval}.
 *
 * Since version 3.0.0, using {@link Datastore.persistence} methods manually is deprecated.
 *
 * Compaction takes a bit of time (not too much: 130ms for 50k
 * records on a typical development machine) and no other operation can happen when
 * it does, so most projects actually don't need to use it.
 *
 * Compaction will also immediately remove any documents whose data line has become
 * corrupted, assuming that the total percentage of all corrupted documents in that
 * database still falls below the specified `corruptAlertThreshold` option's value.
 *
 * Durability works similarly to major databases: compaction forces the OS to
 * physically flush data to disk, while appends to the data file do not (the OS is
 * responsible for flushing the data). That guarantees that a server crash can
 * never cause complete data loss, while preserving performance. The worst that can
 * happen is a crash between two syncs, causing a loss of all data between the two
 * syncs. Usually syncs are 30 seconds appart so that's at most 30 seconds of
 * data. [This post by Antirez on Redis persistence](http://oldblog.antirez.com/post/redis-persistence-demystified.html)
 * explains this in more details, NeDB being very close to Redis AOF persistence
 * with `appendfsync` option set to `no`.
 */
class Persistence {
  /**
   * Create a new Persistence object for database options.db
   * @param {Datastore} options.db
   * @param {Number} [options.corruptAlertThreshold] Optional, threshold after which an alert is thrown if too much data is corrupt
   * @param {serializationHook} [options.beforeDeserialization] Hook you can use to transform data after it was serialized and before it is written to disk.
   * @param {serializationHook} [options.afterSerialization] Inverse of `afterSerialization`.
   * @param {object} [options.modes] Modes to use for FS permissions. Will not work on Windows.
   * @param {number} [options.modes.fileMode=0o644] Mode to use for files.
   * @param {number} [options.modes.dirMode=0o755] Mode to use for directories.
   */
  constructor (options) {
    this.db = options.db
    this.inMemoryOnly = this.db.inMemoryOnly
    this.filename = this.db.filename
    this.corruptAlertThreshold = options.corruptAlertThreshold !== undefined ? options.corruptAlertThreshold : 0.1
    this.modes = options.modes !== undefined
      ? options.modes
      : {
          fileMode: DEFAULT_FILE_MODE,
          dirMode: DEFAULT_DIR_MODE
        }
    if (this.modes.fileMode === undefined) this.modes.fileMode = DEFAULT_FILE_MODE
    if (this.modes.dirMode === undefined) this.modes.dirMode = DEFAULT_DIR_MODE
    if (
      !this.inMemoryOnly &&
      this.filename &&
      this.filename.charAt(this.filename.length - 1) === '~'
    ) throw new Error('The datafile name can\'t end with a ~, which is reserved for crash safe backup files')

    // After serialization and before deserialization hooks with some basic sanity checks
    if (
      options.afterSerialization &&
      !options.beforeDeserialization
    ) throw new Error('Serialization hook defined but deserialization hook undefined, cautiously refusing to start NeDB to prevent dataloss')
    if (
      !options.afterSerialization &&
      options.beforeDeserialization
    ) throw new Error('Serialization hook undefined but deserialization hook defined, cautiously refusing to start NeDB to prevent dataloss')

    // They are wrapped with an async function to ensure that if the hooks are synchronous they won't trigger an
    // uncaught exception at runtime
    this.afterSerialization = async (s) => (options.afterSerialization || (x => x))(s)
    this.beforeDeserialization = async (s) => (options.beforeDeserialization || (x => x))(s)
  }

  /**
   * Internal version without using the {@link Datastore#executor} of {@link Datastore#compactDatafileAsync}, use it instead.
   * @return {Promise<void>}
   * @private
   */
  async persistCachedDatabaseAsync () {
    const lines = []

    if (this.inMemoryOnly) return

    for (const doc of this.db.getAllData()) {
      lines.push(await this.afterSerialization(model.serialize(doc)))
    }
    for (const fieldName of Object.keys(this.db.indexes)) {
      if (fieldName !== '_id') { // The special _id index is managed by datastore.js, the others need to be persisted
        lines.push(await this.afterSerialization(model.serialize({
          $$indexCreated: {
            fieldName: this.db.indexes[fieldName].fieldName,
            unique: this.db.indexes[fieldName].unique,
            sparse: this.db.indexes[fieldName].sparse
          }
        })))
      }
    }

    await storage.crashSafeWriteFileLinesAsync(this.filename, lines, this.modes)
    this.db.emit('compaction.done')
  }

  /**
   * @see Datastore#compactDatafile
   * @deprecated
   * @param {NoParamCallback} [callback = () => {}]
   * @see Persistence#compactDatafileAsync
   */
  compactDatafile (callback) {
    deprecate(_callback => this.db.compactDatafile(_callback), '@seald-io/nedb: calling Datastore#persistence#compactDatafile is deprecated, please use Datastore#compactDatafile, it will be removed in the next major version.')(callback)
  }

  /**
   * @see Datastore#setAutocompactionInterval
   * @deprecated
   */
  setAutocompactionInterval (interval) {
    deprecate(_interval => this.db.setAutocompactionInterval(_interval), '@seald-io/nedb: calling Datastore#persistence#setAutocompactionInterval is deprecated, please use Datastore#setAutocompactionInterval, it will be removed in the next major version.')(interval)
  }

  /**
   * @see Datastore#stopAutocompaction
   * @deprecated
   */
  stopAutocompaction () {
    deprecate(() => this.db.stopAutocompaction(), '@seald-io/nedb: calling Datastore#persistence#stopAutocompaction is deprecated, please use Datastore#stopAutocompaction, it will be removed in the next major version.')()
  }

  /**
   * Persist new state for the given newDocs (can be insertion, update or removal)
   * Use an append-only format
   *
   * Do not use directly, it should only used by a {@link Datastore} instance.
   * @param {document[]} newDocs Can be empty if no doc was updated/removed
   * @return {Promise}
   * @private
   */
  async persistNewStateAsync (newDocs) {
    let toPersist = ''

    // In-memory only datastore
    if (this.inMemoryOnly) return

    for (const doc of newDocs) {
      toPersist += await this.afterSerialization(model.serialize(doc)) + '\n'
    }

    if (toPersist.length === 0) return

    await storage.appendFileAsync(this.filename, toPersist, { encoding: 'utf8', mode: this.modes.fileMode })
  }

  /**
   * @typedef rawIndex
   * @property {string} fieldName
   * @property {boolean} [unique]
   * @property {boolean} [sparse]
   */

  /**
   * From a database's raw data, return the corresponding machine understandable collection.
   *
   * Do not use directly, it should only used by a {@link Datastore} instance.
   * @param {string} rawData database file
   * @return {{data: document[], indexes: Object.<string, rawIndex>}}
   * @private
   */
  async treatRawData (rawData) {
    const data = rawData
      .split('\n')
      .filter(datum => datum !== '')
      .map(async datum => model.deserialize(await this.beforeDeserialization(datum)))
    const dataById = {}
    const indexes = {}
    const dataLength = data.length

    // Last line of every data file is usually blank so not really corrupt
    let corruptItems = 0

    for (const docToAwait of data) {
      try {
        const doc = await docToAwait
        if (doc._id) {
          if (doc.$$deleted === true) delete dataById[doc._id]
          else dataById[doc._id] = doc
        } else if (doc.$$indexCreated && doc.$$indexCreated.fieldName != null) indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated
        else if (typeof doc.$$indexRemoved === 'string') delete indexes[doc.$$indexRemoved]
      } catch (e) {
        corruptItems += 1
      }
    }

    // A bit lenient on corruption
    if (dataLength > 0) {
      const corruptionRate = corruptItems / dataLength
      if (corruptionRate > this.corruptAlertThreshold) {
        const error = new Error(`${Math.floor(100 * corruptionRate)}% of the data file is corrupt, more than given corruptAlertThreshold (${Math.floor(100 * this.corruptAlertThreshold)}%). Cautiously refusing to start NeDB to prevent dataloss.`)
        error.corruptionRate = corruptionRate
        error.corruptItems = corruptItems
        error.dataLength = dataLength
        throw error
      }
    }

    const tdata = Object.values(dataById)

    return { data: tdata, indexes }
  }

  /**
   * From a database's raw data stream, return the corresponding machine understandable collection
   * Is only used by a {@link Datastore} instance.
   *
   * Is only used in the Node.js version, since [React-Native]{@link module:storageReactNative} &
   * [browser]{@link module:storageBrowser} storage modules don't provide an equivalent of
   * {@link module:storage.readFileStream}.
   *
   * Do not use directly, it should only used by a {@link Datastore} instance.
   * @param {Readable} rawStream
   * @return {Promise<{data: document[], indexes: Object.<string, rawIndex>}>}
   * @async
   * @private
   */
  treatRawStreamAsync (rawStream) {
    return new Promise((resolve, reject) => {
      const dataById = {}

      const indexes = {}

      let corruptItems = 0

      const lineStream = byline(rawStream)
      let dataLength = 0

      const waterfall = new Waterfall()

      lineStream.on('data', (line) => {
        const deserializedPromise = this.beforeDeserialization(line) // allows to run the deserialization hook in advance to optimize
        return waterfall.waterfall(async () => { // waterfall is used to preserve the order of lines
          if (line === '') return
          try {
            const doc = model.deserialize(await deserializedPromise)
            if (doc._id) {
              if (doc.$$deleted === true) delete dataById[doc._id]
              else dataById[doc._id] = doc
            } else if (doc.$$indexCreated && doc.$$indexCreated.fieldName != null) indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated
            else if (typeof doc.$$indexRemoved === 'string') delete indexes[doc.$$indexRemoved]
          } catch (e) {
            corruptItems += 1
          }

          dataLength++
        })()
      })

      lineStream.on('end', async () => {
        await waterfall.guardian // await the promises from the on('data') callbacks
        // A bit lenient on corruption
        if (dataLength > 0) {
          const corruptionRate = corruptItems / dataLength
          if (corruptionRate > this.corruptAlertThreshold) {
            const error = new Error(`${Math.floor(100 * corruptionRate)}% of the data file is corrupt, more than given corruptAlertThreshold (${Math.floor(100 * this.corruptAlertThreshold)}%). Cautiously refusing to start NeDB to prevent dataloss.`)
            error.corruptionRate = corruptionRate
            error.corruptItems = corruptItems
            error.dataLength = dataLength
            reject(error, null)
            return
          }
        }
        const data = Object.values(dataById)

        resolve({ data, indexes })
      })

      lineStream.on('error', function (err) {
        reject(err, null)
      })
    })
  }

  /**
   * Load the database
   * 1) Create all indexes
   * 2) Insert all data
   * 3) Compact the database
   *
   * This means pulling data out of the data file or creating it if it doesn't exist
   * Also, all data is persisted right away, which has the effect of compacting the database file
   * This operation is very quick at startup for a big collection (60ms for ~10k docs)
   *
   * Do not use directly as it does not use the [Executor]{@link Datastore.executor}, use {@link Datastore#loadDatabaseAsync} instead.
   * @return {Promise<void>}
   * @private
   */
  async loadDatabaseAsync () {
    this.db._resetIndexes()

    // In-memory only datastore
    if (this.inMemoryOnly) return
    await Persistence.ensureParentDirectoryExistsAsync(this.filename, this.modes.dirMode)
    await storage.ensureDatafileIntegrityAsync(this.filename, this.modes.fileMode)

    let treatedData
    if (storage.readFileStream) {
      // Server side
      const fileStream = storage.readFileStream(this.filename, { encoding: 'utf8', mode: this.modes.fileMode })
      treatedData = await this.treatRawStreamAsync(fileStream)
    } else {
      // Browser
      const rawData = await storage.readFileAsync(this.filename, { encoding: 'utf8', mode: this.modes.fileMode })
      treatedData = await this.treatRawData(rawData)
    }
    // Recreate all indexes in the datafile
    Object.keys(treatedData.indexes).forEach(key => {
      this.db.indexes[key] = new Index(treatedData.indexes[key])
    })

    // Fill cached database (i.e. all indexes) with data
    try {
      this.db._resetIndexes(treatedData.data)
    } catch (e) {
      this.db._resetIndexes() // Rollback any index which didn't fail
      throw e
    }

    await this.db.persistence.persistCachedDatabaseAsync()
    this.db.executor.processBuffer()
  }

  /**
   * See {@link Datastore#dropDatabaseAsync}. This function uses {@link Datastore#executor} internally. Decorating this
   * function with an {@link Executor#pushAsync} will result in a deadlock.
   * @return {Promise<void>}
   * @private
   * @see Datastore#dropDatabaseAsync
   */
  async dropDatabaseAsync () {
    this.db.stopAutocompaction() // stop autocompaction
    this.db.executor.ready = false // prevent queuing new tasks
    this.db.executor.resetBuffer() // remove pending buffered tasks
    await this.db.executor.queue.guardian // wait for the ongoing tasks to end
    // remove indexes (which means remove data from memory)
    this.db.indexes = {}
    // add back _id index, otherwise it will fail
    this.db.indexes._id = new Index({ fieldName: '_id', unique: true })
    // reset TTL on indexes
    this.db.ttlIndexes = {}

    // remove datastore file
    if (!this.db.inMemoryOnly) {
      await this.db.executor.pushAsync(async () => {
        if (await storage.existsAsync(this.filename)) await storage.unlinkAsync(this.filename)
      }, true)
    }
  }

  /**
   * Check if a directory stat and create it on the fly if it is not the case.
   * @param {string} dir
   * @param {number} [mode=0o777]
   * @return {Promise<void>}
   * @private
   */
  static async ensureParentDirectoryExistsAsync (dir, mode = DEFAULT_DIR_MODE) {
    return storage.ensureParentDirectoryExistsAsync(dir, mode)
  }
}

// Interface
module.exports = Persistence

}, {"util":4,"./byline":17,"./indexes.js":11,"./model.js":6,"./storage.js":18,"./waterfall.js":10})

// browser-version/lib/byline.js
__DEFINE__(17, function (__LOCAL_REQUIRE__, module, exports) {
module.exports = {}

}, {})

// browser-version/lib/storage.wx.js
__DEFINE__(18, function (__LOCAL_REQUIRE__, module, exports) {
/* global wx */
/// <reference path="../../node_modules/miniprogram-api-typings/index.d.ts" />
/**
 * Way data is stored for this database.
 *
 * This version is the Weixin Mini App version and uses wx.getFileSystemManager().
 * It intentionally does not provide readFileStream, so persistence uses buffered
 * readFileAsync/treatRawData instead of Node streams.
 *
 * @module storageWx
 * @see module:storage
 * @see module:storageBrowser
 * @see module:storageReactNative
 * @private
 */

const fsm = wx.getFileSystemManager()

const DEFAULT_FILE_MODE = 0o644
const LOCAL_PATH_REGEXP = /^(?:[a-z][a-z0-9+.-]*:\/\/|\/)/i

const normalizeFilename = filename => {
  if (LOCAL_PATH_REGEXP.test(filename)) return filename
  return `${wx.env.USER_DATA_PATH.replace(/\/$/, '')}/${filename.replace(/^\/+/, '')}`
}

const dirname = filename => {
  const normalized = normalizeFilename(filename)
  const index = normalized.lastIndexOf('/')

  if (index < 0) return '.'
  if (index === 0) return '/'
  return normalized.slice(0, index)
}

const wxError = error => {
  const err = new Error(error && error.errMsg ? error.errMsg : 'Weixin file system operation failed')
  err.wxError = error
  return err
}

const callFs = (method, options) => new Promise((resolve, reject) => {
  fsm[method](Object.assign({}, options, {
    success: resolve,
    fail: error => reject(wxError(error))
  }))
})

const encodingFromOptions = options => {
  if (typeof options === 'string') return options
  return options && options.encoding ? options.encoding : 'utf8'
}

/**
 * Returns true if file exists.
 * @param {string} file
 * @return {Promise<boolean>}
 * @async
 * @alias module:storageWx.existsAsync
 */
const existsAsync = file => callFs('access', { path: normalizeFilename(file) }).then(() => true, () => false)

/**
 * Moves a file from one path to another.
 * @param {string} oldPath
 * @param {string} newPath
 * @return {Promise<void>}
 * @alias module:storageWx.renameAsync
 * @async
 */
const renameAsync = async (oldPath, newPath) => {
  const normalizedOldPath = normalizeFilename(oldPath)
  const normalizedNewPath = normalizeFilename(newPath)

  try {
    await callFs('rename', { oldPath: normalizedOldPath, newPath: normalizedNewPath })
  } catch (error) {
    if (!await existsAsync(normalizedOldPath) || !await existsAsync(normalizedNewPath)) throw error
    await unlinkAsync(normalizedNewPath)
    await callFs('rename', { oldPath: normalizedOldPath, newPath: normalizedNewPath })
  }
}

/**
 * Writes data at a path.
 * @param {string} file
 * @param {string|ArrayBuffer} data
 * @param {object|string} [options]
 * @return {Promise<void>}
 * @alias module:storageWx.writeFileAsync
 * @async
 */
const writeFileAsync = async (file, data, options) => {
  await callFs('writeFile', {
    filePath: normalizeFilename(file),
    data,
    encoding: encodingFromOptions(options)
  })
}

/**
 * Append to a file at a path.
 * @param {string} filename
 * @param {string|ArrayBuffer} toAppend
 * @param {object|string} [options]
 * @return {Promise<void>}
 * @alias module:storageWx.appendFileAsync
 * @async
 */
const appendFileAsync = async (filename, toAppend, options) => {
  try {
    await callFs('appendFile', {
      filePath: normalizeFilename(filename),
      data: toAppend,
      encoding: encodingFromOptions(options)
    })
  } catch (error) {
    if (await existsAsync(filename)) throw error
    await writeFileAsync(filename, toAppend, options)
  }
}

/**
 * Read data at a path.
 * @param {string} filename
 * @param {object|string} [options]
 * @return {Promise<string|ArrayBuffer>}
 * @alias module:storageWx.readFileAsync
 * @async
 */
const readFileAsync = async (filename, options) => {
  const filePath = normalizeFilename(filename)
  const statRes = await callFs('stat', { path: filePath })
  if (!statRes.stats || statRes.stats.size === 0) return ''

  const res = await callFs('readFile', {
    filePath,
    encoding: encodingFromOptions(options)
  })
  return res.data || ''
}

/**
 * Remove a file if it exists.
 * @param {string} filename
 * @return {Promise<void>}
 * @async
 * @alias module:storageWx.unlinkAsync
 */
const unlinkAsync = async filename => {
  await callFs('unlink', { filePath: normalizeFilename(filename) })
}

/**
 * Create a directory.
 * @param {string} dir
 * @param {object} [options]
 * @return {Promise<void>}
 * @alias module:storageWx.mkdirAsync
 * @async
 */
const mkdirAsync = async (dir, options) => {
  await callFs('mkdir', {
    dirPath: normalizeFilename(dir),
    recursive: options && options.recursive
  })
}

/**
 * Removes file if it exists.
 * @param {string} file
 * @return {Promise<void>}
 * @alias module:storageWx.ensureFileDoesntExistAsync
 * @async
 */
const ensureFileDoesntExistAsync = async file => {
  if (await existsAsync(file)) await unlinkAsync(file)
}

/**
 * Weixin FileSystemManager has no fsync equivalent.
 * @return {Promise<void>}
 * @alias module:storageWx.flushToStorageAsync
 * @async
 */
const flushToStorageAsync = async () => {}

/**
 * Fully write or rewrite the datafile.
 * @param {string} filename
 * @param {string[]} lines
 * @param {number} [mode=0o644]
 * @return {Promise<void>}
 * @alias module:storageWx.writeFileLinesAsync
 * @async
 */
const writeFileLinesAsync = async (filename, lines, mode = DEFAULT_FILE_MODE) => {
  await writeFileAsync(filename, lines.concat('').join('\n'), { encoding: 'utf8', mode })
}

/**
 * Fully write or rewrite the datafile using temp-file recovery.
 * @param {string} filename
 * @param {string[]} lines
 * @return {Promise<void>}
 * @alias module:storageWx.crashSafeWriteFileLinesAsync
 * @async
 */
const crashSafeWriteFileLinesAsync = async (filename, lines) => {
  const tempFilename = filename + '~'

  await writeFileLinesAsync(tempFilename, lines)
  await renameAsync(tempFilename, filename)
}

/**
 * Ensure the datafile contains data after an interrupted full file write.
 * @param {string} filename
 * @param {number} [mode=0o644]
 * @return {Promise<void>}
 * @alias module:storageWx.ensureDatafileIntegrityAsync
 * @async
 */
const ensureDatafileIntegrityAsync = async (filename, mode = DEFAULT_FILE_MODE) => {
  const tempFilename = filename + '~'

  if (await existsAsync(filename)) return

  if (await existsAsync(tempFilename)) await renameAsync(tempFilename, filename)
  else await writeFileAsync(filename, '', { encoding: 'utf8', mode })
}

/**
 * Check if a file's parent directory exists and create it on the fly if it is not the case.
 * @param {string} filename
 * @param {number} mode
 * @return {Promise<void>}
 * @private
 */
const ensureParentDirectoryExistsAsync = async (filename, mode) => {
  const dir = dirname(filename)

  if (dir === '.' || dir === wx.env.USER_DATA_PATH) return
  if (await existsAsync(dir)) return

  await mkdirAsync(dir, { recursive: true, mode })
}

// Interface
module.exports.existsAsync = existsAsync

module.exports.renameAsync = renameAsync

module.exports.writeFileAsync = writeFileAsync

module.exports.writeFileLinesAsync = writeFileLinesAsync

module.exports.crashSafeWriteFileLinesAsync = crashSafeWriteFileLinesAsync

module.exports.appendFileAsync = appendFileAsync

module.exports.readFileAsync = readFileAsync

module.exports.unlinkAsync = unlinkAsync

module.exports.mkdirAsync = mkdirAsync

module.exports.flushToStorageAsync = flushToStorageAsync

module.exports.ensureDatafileIntegrityAsync = ensureDatafileIntegrityAsync

module.exports.ensureFileDoesntExistAsync = ensureFileDoesntExistAsync

module.exports.ensureParentDirectoryExistsAsync = ensureParentDirectoryExistsAsync

}, {})

return __REQUIRE__(1)
})()

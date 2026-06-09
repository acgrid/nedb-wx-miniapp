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

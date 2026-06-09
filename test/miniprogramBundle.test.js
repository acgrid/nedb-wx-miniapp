/* eslint-env mocha */
const assert = require('assert').strict
const childProcess = require('child_process')
const fs = require('fs')
const path = require('path')
const vm = require('vm')

const root = path.resolve(__dirname, '..')
const bundlePath = path.join(root, 'dist', 'nedb.wx.js')
const buildScriptPath = path.join(root, 'tools', 'build-miniprogram-bundle.mjs')
const packageJson = require('../package.json')

const forbiddenRequirePattern = /require\(['"](?:crypto|fs|path|stream|timers|buffer|util|events|localforage|@react-native-async-storage\/async-storage|@seald-io\/binary-search-tree)['"]\)/g

const wxStub = {
  env: { USER_DATA_PATH: '/tmp/user-data' },
  getFileSystemManager: () => ({
    access: () => {},
    stat: () => {},
    rename: () => {},
    writeFile: () => {},
    appendFile: () => {},
    readFile: () => {},
    unlink: () => {},
    mkdir: () => {}
  })
}

describe('Weixin Mini Program bundle', function () {
  this.timeout(10000)

  it('uses the generated wx bundle as the package entrypoint', function () {
    assert.equal(packageJson.main, 'dist/nedb.wx.js')
    assert.equal(packageJson.scripts['build:miniprogram'], 'node tools/build-miniprogram-bundle.mjs')
  })

  it('publishes only the Mini Program bundle with no runtime dependencies', function () {
    assert.deepEqual(packageJson.dependencies || {}, {})
    assert.deepEqual(packageJson.files, [
      'dist/**/*.js',
      'index.d.ts',
      'README.md',
      'LICENSE.md'
    ])
    assert.equal(Object.prototype.hasOwnProperty.call(packageJson, 'browser'), false)
    assert.equal(Object.prototype.hasOwnProperty.call(packageJson, 'react-native'), false)
    assert.equal(Object.prototype.hasOwnProperty.call(packageJson, 'weixin-miniprogram'), false)
  })

  it('builds a standalone bundle that can load without external requires', function () {
    childProcess.execFileSync(process.execPath, [buildScriptPath], { cwd: root, stdio: 'pipe' })

    const source = fs.readFileSync(bundlePath, 'utf8')
    assert.deepEqual(source.match(forbiddenRequirePattern), null)
    assert.deepEqual(source.match(/\brequire\(/g), null)

    const externalRequires = []
    const sandbox = {
      module: { exports: {} },
      exports: {},
      require: request => {
        externalRequires.push(request)
        throw new Error('Unexpected external require: ' + request)
      },
      wx: wxStub,
      console,
      setTimeout,
      clearTimeout
    }
    sandbox.exports = sandbox.module.exports

    vm.runInNewContext(source, sandbox, { filename: bundlePath })

    assert.equal(typeof sandbox.module.exports, 'function')
    assert.deepEqual(externalRequires, [])
  })

  it('loads an empty datafile without calling readFile on Mini Program clients', async function () {
    childProcess.execFileSync(process.execPath, [buildScriptPath], { cwd: root, stdio: 'pipe' })

    const source = fs.readFileSync(bundlePath, 'utf8')
    const calls = []
    const files = new Set()
    const fsm = {
      access: ({ path, success, fail }) => {
        calls.push(['access', path])
        if (files.has(path)) success({})
        else fail({ errMsg: `access:fail no such file or directory ${path}` })
      },
      mkdir: ({ dirPath, success }) => {
        calls.push(['mkdir', dirPath])
        success({})
      },
      writeFile: ({ filePath, data, success }) => {
        calls.push(['writeFile', filePath, data])
        files.add(filePath)
        success({})
      },
      stat: ({ path, success }) => {
        calls.push(['stat', path])
        success({ stats: { size: 0 } })
      },
      readFile: ({ filePath, fail }) => {
        calls.push(['readFile', filePath])
        fail({ errMsg: 'readFile:fail the value of "position" is out of range', errno: 1301009 })
      },
      rename: ({ oldPath, newPath, success }) => {
        calls.push(['rename', oldPath, newPath])
        files.delete(oldPath)
        files.add(newPath)
        success({})
      },
      appendFile: ({ filePath, data, success }) => {
        calls.push(['appendFile', filePath, data])
        files.add(filePath)
        success({})
      },
      unlink: ({ filePath, success }) => {
        calls.push(['unlink', filePath])
        files.delete(filePath)
        success({})
      }
    }
    const sandbox = {
      module: { exports: {} },
      exports: {},
      require: request => {
        throw new Error('Unexpected external require: ' + request)
      },
      wx: {
        env: { USER_DATA_PATH: '/tmp/user-data' },
        getFileSystemManager: () => fsm
      },
      console,
      setTimeout,
      clearTimeout
    }
    sandbox.exports = sandbox.module.exports

    vm.runInNewContext(source, sandbox, { filename: bundlePath })

    const Datastore = sandbox.module.exports
    const db = new Datastore({ filename: 'empty.db' })
    await db.loadDatabaseAsync()

    assert.equal(calls.some(([method]) => method === 'readFile'), false)
  })
})

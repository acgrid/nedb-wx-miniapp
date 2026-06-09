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
})

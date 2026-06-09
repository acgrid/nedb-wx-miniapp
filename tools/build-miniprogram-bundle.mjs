import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(__dirname, '..')
const entryFile = path.join(root, 'index.js')
const outputFile = path.join(root, 'dist/nedb.wx.js')

const bundledRequests = new Map([
  ['events', path.join(root, 'node_modules/events/events.js')],
  ['util', path.join(root, 'browser-version/lib/util.wx.js')],
  ['@seald-io/binary-search-tree', path.join(root, 'node_modules/@seald-io/binary-search-tree/index.js')]
])

const aliases = new Map([
  [path.join(root, 'lib/customUtils.js'), path.join(root, 'browser-version/lib/customUtils.js')],
  [path.join(root, 'lib/storage.js'), path.join(root, 'browser-version/lib/storage.wx.js')],
  [path.join(root, 'lib/byline.js'), path.join(root, 'browser-version/lib/byline.js')]
])

const modules = []
const idsByFile = new Map()

const normalizeFile = file => {
  let resolved = path.resolve(file)
  if (!path.extname(resolved)) resolved += '.js'
  resolved = path.normalize(resolved)
  return aliases.get(resolved) || resolved
}

const findBundledRequires = source => {
  const result = []
  const pattern = /require\(['"]([^'"]*)['"]\)/g
  let match

  while ((match = pattern.exec(source))) {
    const request = match[1]
    if (request.startsWith('.') || bundledRequests.has(request)) result.push(request)
  }

  return result
}

const rewriteBundledRequires = (source, map) => source.replace(
  /require\(['"]([^'"]*)['"]\)/g,
  (statement, request) => Object.prototype.hasOwnProperty.call(map, request) ? `__LOCAL_REQUIRE__(${map[request]})` : statement
)

const resolveRequest = (fromFile, request) => {
  if (bundledRequests.has(request)) return bundledRequests.get(request)
  return path.join(path.dirname(fromFile), request)
}

const addModule = file => {
  const normalized = normalizeFile(file)
  if (idsByFile.has(normalized)) return idsByFile.get(normalized)

  const id = modules.length + 1
  idsByFile.set(normalized, id)

  const source = fs.readFileSync(normalized, 'utf8')
  const requires = findBundledRequires(source)
  const map = {}

  modules.push({ id, file: normalized, source, map })

  for (const request of requires) {
    map[request] = addModule(resolveRequest(normalized, request))
  }

  return id
}

const entryId = addModule(entryFile)
const output = []

output.push("'use strict'")
output.push('')
output.push('module.exports = (function () {')
output.push('var __MODS__ = {}')
output.push('var __DEFINE__ = function (id, factory, map) { __MODS__[id] = { factory: factory, map: map, exports: {}, loaded: false } }')
output.push('var __REQUIRE__ = function (id, source) {')
output.push('  if (!__MODS__[id]) throw new Error("Cannot find bundled module: " + source)')
output.push('  var mod = __MODS__[id]')
output.push('  if (!mod.loaded) {')
output.push('    mod.loaded = true')
output.push('    var __LOCAL_REQUIRE__ = function (request) { return typeof request === "number" ? __REQUIRE__(request) : __REQUIRE__(mod.map[request], request) }')
output.push('    mod.factory(__LOCAL_REQUIRE__, mod, mod.exports)')
output.push('  }')
output.push('  return mod.exports')
output.push('}')
output.push('')

for (const mod of modules) {
  const relativeName = path.relative(root, mod.file).replace(/\\/g, '/')
  output.push(`// ${relativeName}`)
  output.push(`__DEFINE__(${mod.id}, function (__LOCAL_REQUIRE__, module, exports) {`)
  output.push(rewriteBundledRequires(mod.source, mod.map))
  output.push(`}, ${JSON.stringify(mod.map)})`)
  output.push('')
}

output.push(`return __REQUIRE__(${entryId})`)
output.push('})()')
output.push('')

fs.mkdirSync(path.dirname(outputFile), { recursive: true })
fs.writeFileSync(outputFile, output.join('\n'), 'utf8')
console.log(`Wrote ${path.relative(root, outputFile).replace(/\\/g, '/')}`)

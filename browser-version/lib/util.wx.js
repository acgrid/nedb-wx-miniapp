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

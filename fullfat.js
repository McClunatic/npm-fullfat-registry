var follow = require('follow')
var fs = require('fs')
var EE = require('events').EventEmitter
var util = require('util')
var url = require('url')
var path = require('path')
var tmp = path.resolve(__dirname, 'tmp')
var mkdirp = require('mkdirp')
var rimraf = require('rimraf')
var assert = require('assert')
var stream = require('stream')
var util = require('util')
var crypto = require('crypto')
var once = require('once')
var parse = require('parse-json-response')
var hh = require('http-https')
var request = require('requestretry')

var version = require('./package.json').version
var ua = 'npm FullFat/' + version + ' node/' + process.version
var readmeTrim = require('npm-registry-readme-trim')

util.inherits(FullFat, EE)

function maybeEncodeURI(str) {
  if (str.startsWith("_design")) {
    return str;
  } else {
    return str.replace(/\//g, '%2f');
  }
}

module.exports = FullFat

function FullFat(conf) {
  if (!conf.skim || !conf.fat) {
    throw new Error('skim and fat database urls required')
  }

  this.skim = url.parse(conf.skim).href
  this.skim = this.skim.replace(/\/+$/, '')

  var f = url.parse(conf.fat)
  this.fat = f.href
  this.fat = this.fat.replace(/\/+$/, '')
  delete f.auth
  this.publicFat = url.format(f)
  this.publicFat = this.publicFat.replace(/\/+$/, '')

  this.registry = null
  if (conf.registry) {
    this.registry = url.parse(conf.registry).href
    this.registry = this.registry.replace(/\/+$/, '')
  }

  this.ua = conf.ua || ua
  this.inactivity_ms = conf.inactivity_ms || 1000 * 60 * 60
  this.seqFile = conf.seq_file
  this.writingSeq = false
  this.error = false
  this.since = 0
  this.follow = null

  // set to true to log missing attachments only.
  // otherwise, emits an error.
  this.missingLog = conf.missing_log || false

  this.whitelist = conf.whitelist || [ /.*/ ]

  if (typeof this.whitelist === 'string') {
    this.whitelist = JSON.parse(fs.readFileSync(this.whitelist, 'utf8'))
                     .map(s => new RegExp(s));
  }

  this.tmp = conf.tmp
  if (!this.tmp) {
    var rand = crypto.randomBytes(6).toString('hex')
    this.tmp = path.resolve('npm-fullfat-tmp-' + process.pid + '-' + rand)
  }

  this.boundary = 'npmFullFat-' + crypto.randomBytes(6).toString('base64')

  this.readSeq(this.seqFile)
}

FullFat.prototype.readSeq = function(file) {
  console.log('readSeq')
  if (!this.seqFile)
    process.nextTick(this.start.bind(this))
  else
    fs.readFile(file, 'ascii', this.gotSeq.bind(this))
}

FullFat.prototype.gotSeq = function(er, data) {
  console.log('gotSeq')
  if (er && er.code === 'ENOENT')
    data = '0'
  else if (er)
    return this.emit('error', er)

  data = +data || 0
  this.since = data
  this.start()
}

FullFat.prototype.start = function() {
  console.log('start')
  if (this.follow)
    return this.emit('error', new Error('already started'))

  this.emit('start')
  this.follow = follow({
    db: this.skim,
    since: this.since,
    inactivity_ms: this.inactivity_ms
  }, this.onchange.bind(this))
  this.follow.on('error', this.emit.bind(this, 'error'))
}

FullFat.prototype._emit = function(ev, arg) {
  // Don't emit errors while writing seq
  if (ev === 'error' && this.writingSeq) {
    this.error = arg
  } else {
    EventEmitter.prototype.emit.apply(this, arguments)
  }
}

FullFat.prototype.writeSeq = function() {
  console.log('writeSeq')
  var seq = +this.since
  if (this.seqFile && !this.writingSeq && seq > 0) {
    var data = seq + '\n'
    var file = this.seqFile + '.' + seq
    this.writingSeq = true
    fs.writeFile(file, data, 'ascii', function(writeEr) {
      var er = this.error
      if (er)
        this.emit('error', er)
      else if (!writeEr) {
        fs.rename(file, this.seqFile, function(mvEr) {
          this.writingSeq = false
          var er = this.error
          if (er)
            this.emit('error', er)
          else if (!mvEr)
            this.emit('sequence', seq)
        }.bind(this))
      }
    }.bind(this))
  }
}

FullFat.prototype.onchange = function(er, change) {
  console.log('onchange')
  if (er)
    return this.emit('error', er)

  if (!change.id)
    return

  this.pause()
  this.since = change.seq

  this.emit('change', change)

  if (change.deleted)
    this.delete(change)
  else
    this.getDoc(change)
}

FullFat.prototype.getDoc = function(change) {
  console.log('getDoc')
  var q = '?revs=true&att_encoding_info=true'
  var uri = url.parse(this.skim + '/' + maybeEncodeURI(change.id) + q)
  var opt = {
    uri: uri,
    method: 'GET',
    headers: {
      'user-agent': this.ua,
      'connection': 'close'
    },
  }

  var req = request(opt)
  req.on('response', parse(this.ongetdoc.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'));
}

FullFat.prototype.ongetdoc = function(change, er, data, res) {
  console.log('ongetDoc')
  if (er)
    this.emit('error', er)
  else {
    change.doc = data
    if (change.id.match(/^_design\//))
      this.putDesign(change)
    else if (data.time && data.time.unpublished)
      this.unpublish(change)
    else
      this.putDoc(change)
  }
}

FullFat.prototype.unpublish = function(change) {
  console.log('unpublish')
  change.fat = change.doc
  this.put(change, [])
}

FullFat.prototype.putDoc = function(change) {
  console.log('putDoc')
  var q = '?revs=true&att_encoding_info=true'
  var uri = url.parse(this.fat + '/' + maybeEncodeURI(change.id) + q)
  var opt = {
    uri: uri,
    method: 'GET',
    headers: {
      'user-agent': this.ua,
      'connection': 'close'
    },
  }
  var req = request(opt)
  req.on('response', parse(this.onfatget.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'))
}

FullFat.prototype.putDesign = function(change) {
  console.log('putDesign')
  var doc = change.doc
  this.pause()
  var uri = url.parse(this.fat + '/' + maybeEncodeURI(change.id) +
                      '?new_edits=false')
  var b = new Buffer(JSON.stringify(doc), 'utf8')
  var opt = {
    uri: uri,
    method: 'PUT',
    headers: {
      'user-agent': this.ua,
      'content-type': 'application/json',
      'content-length': b.length,
      'connection': 'close'
    },
  }

  var req = request(opt)
  req.on('response', parse(this.onputdesign.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'));
  req.end(b)
}

FullFat.prototype.onputdesign = function(change, er, data, res) {
  console.log('onputDesign')
  if (er)
    return this.emit('error', er)
  this.emit('putDesign', change, data)
  this.resume()
}

FullFat.prototype.delete = function(change) {
  console.log('delete')
  var name = change.id

  var uri = url.parse(this.fat + '/' + name)
  var opt = {
    uri: uri,
    method: 'HEAD',
    headers: {
      'user-agent': this.ua,
      'connection': 'close'
    },
  }

  var req = request(opt)
  req.on('response', this.ondeletehead.bind(this, change))
  req.on('error', this.emit.bind(this, 'error'));
  req.end()
}

FullFat.prototype.ondeletehead = function(change, res) {
  console.log('ondeletehead')
  // already gone?  totally fine.  move on, nothing to delete here.
  if (res.statusCode === 404)
    return this.afterDelete(change)

  var rev = res.headers.etag.replace(/^"|"$/g, '')
  var uri = url.parse(this.fat + '/' + maybeEncodeURI(change.id) +
                      '?rev=' + rev)
  var opt = {
    uri: uri,
    method: 'DELETE',
    headers: {
      'user-agent': this.ua,
      'connection': 'close'
    },
  }
  var req = request(opt)
  req.on('response', parse(this.ondelete.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'));
  req.end()
}

FullFat.prototype.ondelete = function(change, er, data, res) {
  console.log('ondelete')
  if (er && er.statusCode === 404)
    this.afterDelete(change)
  else if (er)
    this.emit('error', er)
  else
    // scorch the earth! remove fully! repeat until 404!
    this.delete(change)
}

FullFat.prototype.afterDelete = function(change) {
  console.log('afterDelete')
  this.emit('delete', change)
  this.resume()
}

FullFat.prototype.onfatget = function(change, er, f, res) {
  console.log('onfatget')
  if (er && er.statusCode !== 404)
    return this.emit('error', er)

  if (er)
    f = JSON.parse(JSON.stringify(change.doc))

  f._attachments = f._attachments || {}
  console.log('onfatget, f._attachments = ' + JSON.stringify(f._attachments))
  change.fat = f
  this.merge(change)
}


FullFat.prototype.merge = function(change) {
  console.log('merge')
  var s = change.doc
  var f = change.fat

  // if no versions in the skim record, then nothing to fetch
  if (!s.versions)
    return this.resume()

  // Only fetch attachments if it's on the list.
  var pass = true
  if (this.whitelist.length) {
    pass = false
    for (var i = 0; !pass && i < this.whitelist.length; i++) {
      var w = this.whitelist[i]
      if (typeof w === 'string')
        pass = w === change.id
        if (w === change.id)
          console.log(change.id + ' matched ' + w);
      else
        pass = w.exec(change.id)
        if (w.exec(change.id))
          console.log(change.id + ' matched ' + w);
    }
    if (!pass) {
      console.log('%s was NOT found in whitelist', change.id)
      f._attachments = {}
      return this.fetchSome(change, [], [], 0, 4)
    } else {
      console.log('%s was found in whitelist', change.id)
    }
  }

  var need = []
  var changed = false
  for (var v in s.versions) {
    var tgz = s.versions[v].dist.tarball
    var att = path.basename(url.parse(tgz).pathname)
    var ver = s.versions[v]
    f.versions = f.versions || {}

    if (!f.versions[v] || f.versions[v].dist.shasum !== ver.dist.shasum) {
      f.versions[v] = s.versions[v]
      need.push(v)
      changed = true
    } else if (!f._attachments[att]) {
      need.push(v)
      changed = true
    }
  }

  // remove any versions that s removes, or which lack attachments
  for (var v in f.versions) {
    if (!s.versions[v])
      delete f.versions[v]
  }


  for (var a in f._attachments) {
    var found = false
    for (var v in f.versions) {
      var tgz = f.versions[v].dist.tarball
      var b = path.basename(url.parse(tgz).pathname)
      if (b === a) {
        found = true
        break
      }
    }
    if (!found) {
      delete f._attachments[a]
      changed = true
    }
  }

  for (var k in s) {
    if (k !== '_attachments' && k !== 'versions') {
      if (changed)
        f[k] = s[k]
      else if (JSON.stringify(f[k]) !== JSON.stringify(s[k])) {
        f[k] = s[k]
        changed = true
      }
    }
  }

  changed = readmeTrim(f) || changed

  if (!changed)
    this.resume()
  else
    this.fetchSome(change, need, [], 0, 4)
}

FullFat.prototype.put = function(change, did) {
  console.log('put')
  var f = change.fat
  change.did = did
  // at this point, all the attachments have been fetched into
  // {this.tmp}/{change.id}-{change.seq}/{attachment basename}
  // make a multipart PUT with all of the missing ones set to
  // follows:true
  var boundaries = []
  var boundary = this.boundary
  var bSize = 0

  var attSize = 0
  var atts = f._attachments = f._attachments || {}

  // It's important that we do everything in enumeration order,
  // because couchdb is a jerk, and ignores disposition headers.
  // Still include the filenames, though, so at least we dtrt.
  did.forEach(function(att) {
    atts[att.name] = {
      length: att.length,
      follows: true
    }

    if (att.type)
      atts[att.name].type = att.type
  })

  var send = []
  Object.keys(atts).forEach(function (name) {
    var att = atts[name]

    if (att.follows !== true)
      return

    send.push([name, att])
    attSize += att.length

    var b = '\r\n--' + boundary + '\r\n' +
            'content-length: ' + att.length + '\r\n' +
            'content-disposition: attachment; filename=' +
            JSON.stringify(name) + '\r\n'

    if (att.type)
      b += 'content-type: ' + att.type + '\r\n'

    b += '\r\n'

    boundaries.push(b)
    bSize += b.length
  })

  // one last boundary at the end
  var b = '\r\n--' + boundary + '--'
  bSize += b.length
  boundaries.push(b)

  // put with new_edits=false to retain the same rev
  // this assumes that NOTHING else is writing to this database!
  var uri = url.parse(this.fat + '/' + maybeEncodeURI(f.name) +
                    '?new_edits=false')
  var opt = {
    uri: uri,
    method: 'PUT',
    headers: {
      'user-agent': this.ua,
      'content-type': 'multipart/related;boundary="' + boundary + '"',
      'connection': 'close'
    },
  }

  var doc = new Buffer(JSON.stringify(f), 'utf8')
  var len = 0

  // now, for the document
  var b = '--' + boundary + '\r\n' +
          'content-type: application/json\r\n' +
          'content-length: ' + doc.length + '\r\n\r\n'
  bSize += b.length

  opt.headers['content-length'] = attSize + bSize + doc.length

  // console.log('REQUEST: %s', JSON.stringify(p));
  // process.exit();
  var req = request(opt)
  req.on('response', parse(this.onputres.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'));
  req.write(b, 'ascii')
  req.write(doc)
  this.putAttachments(req, change, boundaries, send)
}

FullFat.prototype.putAttachments = function(req, change, boundaries, send) {
  console.log('putAttachments')
  // send is the ordered list of [[name, attachment object],...]
  var b = boundaries.shift()
  var ns = send.shift()

  // last one!
  if (!ns) {
    req.write(b, 'ascii')
    return req.end()
  }

  var name = ns[0]
  req.write(b, 'ascii')
  var file = path.join(this.tmp, change.id + '-' + change.seq, name)
  var fstr = fs.createReadStream(file)

  fstr.on('end', function() {
    this.emit('upload', {
      change: change,
      name: name
    })
    this.putAttachments(req, change, boundaries, send)
  }.bind(this))

  fstr.on('error', this.emit.bind(this, 'error'))
  fstr.pipe(req, { end: false })
}

FullFat.prototype.onputres = function(change, er, data, res) {
  console.log('onputres')

  if (!change.id)
    throw new Error('wtf?')

  // In some oddball cases, it looks like CouchDB will report stubs that
  // it doesn't in fact have.  It's possible that this is due to old bad
  // data in a past FullfatDB implementation, but whatever the case, we
  // ought to catch such errors and DTRT.  In this case, the "right thing"
  // is to re-try the PUT as if it had NO attachments, so that it no-ops
  // the attachments that ARE there, and fills in the blanks.
  // We do that by faking the onfatget callback with a 404 error.
  if (er && er.statusCode === 412 &&
      0 === er.message.indexOf('{"error":"missing_stub"') &&
      !change.didFake404){
    change.didFake404 = true
    this.onfatget(change, { statusCode: 404 }, {}, {})
  } else if (er)
    this.emit('error', er)
  else {
    this.emit('put', change, data)
    // Just a best-effort cleanup.  No big deal, really.
    rimraf(this.tmp + '/' + maybeEncodeURI(change.id) + '-' + change.seq,
           function() {})
    this.resume()
  }
}

FullFat.prototype.fetchAll = function(change, need, did) {
  console.log('fetchAll')
  var f = change.fat
  var tmp = path.resolve(this.tmp, change.id + '-' + change.seq)
  var len = need.length
  if (!len)
    return this.put(change, did)

  var errState = null

  mkdirp(tmp, function(er) {
    if (er)
      return this.emit('error', er)
    need.forEach(this.fetchOne.bind(this, change, need, did))
  }.bind(this))
}

FullFat.prototype.fetchSome = function(change, need, did, begin, stride) {
  console.log('fetchSome')
  console.log('Fetching %s: %d - %d (%d)', change.id, begin, begin + stride, need.length);
  var f = change.fat
  var tmp = path.resolve(this.tmp, change.id + '-' + change.seq)
  var len = need.length
  if (!len)
    return this.put(change, did)

  var errState = null

  mkdirp(tmp, function(er) {
    if (er)
      return this.emit('error', er)
    need.slice(begin, begin + stride).forEach(
      this.fetchOne.bind(this, change, need, did, begin, stride))
  }.bind(this))
}

FullFat.prototype.fetchOne = function(change, need, did, begin, stride, v) {
  console.log('fetchOne')
  var f = change.fat
  var uri = url.parse(change.doc.versions[v].dist.tarball)
  if (this.registry) {
    var p = '/' + maybeEncodeURI(change.id) + '/-/' +
             path.basename(r.pathname)
    uri = url.parse(this.registry + p)
  }

  var opt = {
    uri: uri,
    method: 'GET',
    headers: {
      'user-agent': this.ua,
      'connection': 'close'
    },
  }

  var req = request(opt)
  req.on('response', this.onattres.bind(this, change, need, did, begin, stride, v, uri))
  req.on('error', this.emit.bind(this, 'error'));
  req.end()
}

FullFat.prototype.onattres = function(change, need, did, begin, stride, v, r, res) {
  console.log('onattres')
  var f = change.fat
  var att = r.href
  var sum = f.versions[v].dist.shasum
  var filename = f.name.replace(/^.*\//, '') + '-' + v + '.tgz'

  var file = path.join(this.tmp, change.id + '-' + change.seq, filename)
  // console.log('ONATTRES, filename: %s', filename)
  // console.log('ONATTRES, file: %s', file)
  // process.exit()

  // TODO: If the file already exists, get its size.
  // If the size matches content-length, get the md5
  // If the md5 matches content-md5, then don't bother downloading!

  function skip() {
    rimraf(file, function() {})
    delete f.versions[v]
    if (f._attachments)
      delete f._attachments[file]
    need.splice(need.indexOf(v), 1)
    maybeDone(null)
  }

  var maybeDone = function maybeDone(a) {
    if (a)
      this.emit('download', a)
    if (need.length === did.length)
      this.put(change, did)
    else if (begin + stride === did.length)
      this.fetchSome(change, need, did, begin + stride, stride)
  }.bind(this)

  // if the attachment can't be found, then skip that version
  // it's uninstallable as of right now, and may or may not get
  // fixed in a future update
  if (res.statusCode !== 200) {
    var er = new Error('Error fetching attachment: ' + att)
    er.statusCode = res.statusCode
    er.code = 'attachment-fetch-fail'
    if (this.missingLog)
      return fs.appendFile(this.missingLog, att + '\n', skip)
    else
      return this.emit('error', er)
  }

  var fstr = fs.createWriteStream(file)

  // check the shasum while we're at it
  var sha = crypto.createHash('sha1')
  var shaOk = false
  var errState = null

  sha.on('data', function(c) {
    c = c.toString('hex')
    if (c === sum)
      shaOk = true
  }.bind(this))

  if (!res.headers['content-length']) {
    var counter = new Counter()
    res.pipe(counter)
  }

  res.pipe(sha)
  res.pipe(fstr)

  fstr.on('error', function(er) {
    er.change = change
    er.version = v
    er.path = file
    er.url = att
    this.emit('error', errState = errState || er)
  }.bind(this))

  fstr.on('close', function() {
    if (errState || !shaOk) {
      // something didn't work, but the error was squashed
      // take that as a signal to just delete this version
      return skip()
    }
    // it worked!  change the dist.tarball url to point to the
    // registry where this is being stored.  It'll be rewritten by
    // the _show/pkg function when going through the rewrites, anyway,
    // but this url will work if the couch itself is accessible.
    //
    // BPM: This may not be right for scoped packages
    var newatt = this.publicFat + '/' + maybeEncodeURI(change.id) +
                 '/' + path.basename(change.id) + '-' + v + '.tgz'
    f.versions[v].dist.tarball = newatt

    if (res.headers['content-length'])
      var cl = +res.headers['content-length']
    else
      var cl = counter.count

    var a = {
      change: change,
      version: v,
      name: path.basename(file),
      length: cl,
      type: res.headers['content-type']
    }
    did.push(a)
    maybeDone(a)

  }.bind(this))
}

FullFat.prototype.destroy = function() {
  console.log('destroy')
  if (this.follow)
    this.follow.die()
}

FullFat.prototype.pause = function() {
  console.log('pause')
  if (this.follow)
    this.follow.pause()
}

FullFat.prototype.resume = function() {
  console.log('resume')
  this.writeSeq()
  if (this.follow)
    this.follow.resume()
}

util.inherits(Counter, stream.Writable)
function Counter(options) {
  stream.Writable.call(this, options)
  this.count = 0
}
Counter.prototype._write = function(chunk, encoding, cb) {
  this.count += chunk.length
  cb()
}

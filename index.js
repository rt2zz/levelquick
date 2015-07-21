var levelup = require('levelup')

module.exports = function(path, options){
  var db = levelup(path, options)
  options = options || {}
  options.delimiter = options.delimiter || '~'
  return new Quick(db, options)
}

function Quick(db, options){
  this.delim = options.delimiter
  this.db = db
  this.locked = {}
}

//Sanitize key parts and join into one string with delimiters
Quick.prototype.makeKey = function(parts){
  if(typeof parts === 'string') return parts
  var key = ''
  var delim = this.delim

  parts.map(function(part){
    if(typeof part != 'string') part = ''+part
    key+=delim+part.replace(delim, '')
  })

  //cut off first delimiter
  return key.substring(1)
}

Quick.prototype.keyParts = function(key){
  return key.split(this.delim)
}

Quick.prototype.lock = function(key){
  this.locked[key] = true
}

Quick.prototype.release = function(key){
  this.locked[key] = false
}

Quick.prototype.whenUnlocked = function(key, cb){
  if(this.locked[key]){
    process.nextTick(this.whenUnlocked(key, cb))
  }
  else{
    cb()
  }
}

Quick.prototype.put = function(kp, data, cb){
  var key = this.makeKey(kp)
  if(this.locked[key]){
    cb(new Error('key is locked'))
    return
  }
  this.db.put(key, data, cb)
}

Quick.prototype.forcePut = function(kp, data, cb){
  var self = this
  var key = this.makeKey(kp)
  this.whenUnlocked(key, function(){
    self.db.put(key, data, cb)
  })
}

Quick.prototype.del = function(kp, cb){
  var key = this.makeKey(kp)
  this.db.del(key, cb)
}

Quick.prototype.get = function(kp, cb){
  var self = this
  var key = this.makeKey(kp)
  this.whenUnlocked(key, function(){
    self.db.get(key, function(err, data){
      if(err && err.notFound) err = null
      cb(err, data)
    })
  })
}

Quick.prototype.batch = function(ops, cb){
  var self = this
  ops.forEach(function(op, index){
    op.key = self.makeKey(op.key)
  })
  this.db.batch(ops, function (err) {
    cb(err)
  })
}

Quick.prototype.lockGet = function(kp, cb){
  var key = this.makeKey(kp)
  this.lock(key)
  this.db.get(key, function(err, data){
    cb(err, data)
  })
}

Quick.prototype.releasePut = function(kp, data, cb){
  var self = this
  var key = this.makeKey(kp)
  this.db.put(key, data, function(err){
    self.release(key)
    cb(err, data)
  })
}

Quick.prototype.stream = function(options){
  options.gt = options.gt ? this.makeKey(options.gt) : null
  options.gte = options.gte ? this.makeKey(options.gte) : null
  options.lt = options.lt ? this.makeKey(options.lt) : null
  options.lte = options.lte ? this.makeKey(options.lte) : null
  return this.db.createReadStream(options)
}

Quick.prototype.streamFrom = function(kp, options){
  var delim = this.delim
  var from = this.makeKey(kp)
  var options = options || {}
  if(!options.reverse){
    options.gt = from
    options.lt = this.makeKey(kp.slice(0, kp.length-1))+delim+delim
  }
  else{
    options.lt = from
    options.gt = this.makeKey(kp.slice(0, kp.length-1))+delim
  }
  var stream = this.db.createReadStream(options)
  stream.toArray = streamToArray
  stream.toObject = streamToObject
  return stream
}

Quick.prototype.streamOver = function(kp, options){
  var delim = this.delim
  var from = this.makeKey(kp)
  var options = options || {}
  options.gt = from+delim
  options.lt = from+delim+delim

  var stream = this.db.createReadStream(options)
  stream.toArray = streamToArray
  stream.toObject = streamToObject
  return stream
}

streamToArray = function(cb){
  var results = []
  this.on('data', function(kv){
    results.push(kv.value)
  })
  this.on('end', function(){
    cb(null, results)
  })
}

streamToObject = function(cb){
  var results = {}
  this.on('data', function(kv){
    results[kv.key] = kv.value
  })
  this.on('end', function(){
    cb(null, results)
  })
}

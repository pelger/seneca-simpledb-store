/*jslint node: true */
/*
 * Copyright (c) 2010-2013 Peter Elger, MIT License 
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

"use strict";
var assert = require("assert");
var _ = require('underscore');
var simpledb = require('../../simpledb');
var name = "simpledb-store";
var MIN_WAIT = 16;
var MAX_WAIT = 65336;



module.exports = function(opts) {
  var seneca = this;
  var desc;

  opts.minwait = opts.minwait || MIN_WAIT;
  opts.maxwait = opts.maxwait || MAX_WAIT;

  var minwait;
  var dbinst  = null;
  var collmap = {};
  var specifications = null;



  /**
   * check and report error conditions
   * fail will execute the callback
   */
  function error(args, err, cb) {
    if (err) {
      seneca.log.debug('error: ' + err);
      seneca.fail({code:'entity/error',store:name}, cb);
    }
    return err;
  }



  /**
   * configure the store - create a new SimpleDB connection object
   * params:
   * spec -  { keyid: 'AWS_KEY',
   *           secret: 'AWS_SECRET',
   *           minwait: 16,
   *           maxwait: 65336 }
   * cb - callback
   */
  function configure(spec, cb) {
    assert(spec);
    assert(cb);
    assert(spec.keyid);
    assert(spec.secret);
    var specifications = spec;
    specifications.nolimit = true;
    dbinst = new simpledb.SimpleDB(specifications /*, logger */);
    seneca.log.debug('init', 'db open', specifications);
    cb(null);
  }



  /**
   * ensure that the nominated simpledb domain exists.
   * if it does not exist it will be created.
   * params:
   * args
   * ent
   * cb - callback
   */ 
  function ensureDomain(args, ent, cb) {
    var canon = ent.canon$({object:true});
    var domainName = (canon.base?canon.base + '_' : '') + canon.name;

    dbinst.listDomains(function(err, domains){
      if (!err) {
        if (!_.find(domains, function(item) { return item === domainName; })) {
          dbinst.createDomain(domainName, function(err, result) {
            cb(err, dbinst, domainName);
          });
        }
        else {
          cb(err, dbinst, domainName);
        }
      }
      else {
        cb(err, null, null);
      }
    });
  }




  /**
   * the simple db store interface returned to seneca
   */
  var store = {
    name:name,



    /**
     * closes the connection, in the case of simple DB the connection object is nulled out
     * there is no connection per say to close.
     * params
     * cb - callback
     */
    close: function(cmd, cb) {
      assert(cb);
      if(dbinst) {
        dbinst = null;
      }
      cb(null);
    },

    

    /**
     * save the data as specified in the entitiy block on the arguments object
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */
    save: function(args, cb) {
      assert(cb);
      assert(args);
      assert(args.ent);
      
      var ent = args.ent;
      var update = !!ent.id;

      ensureDomain(args, ent, function(err, sdb, domainName) {
        if (!error(args, err, cb)) {
          var entp = {};
          var fields = ent.fields$();

          fields.forEach(function(field) {
            if (typeof ent[field] === 'object' || typeof ent[field] === 'boolean') {
              entp[field] = JSON.stringify(ent[field]);
            }
            else {
              entp[field] = ent[field];
            }
          });

          if (!update && void 0 != ent.id$) {
            entp.id = ent.id$;
          }

          if (!update && void 0 == ent.id$) {
            entp.id = makeGuid();
          }

          if (update) {
            sdb.putItem(domainName, entp.id, entp, function(err, result){
              if (!error(args,err,cb)) {
                seneca.log.debug('save/update', ent, desc);
                cb(null, ent);
              }
            });
          }
          else {
            sdb.putItem(domainName, entp.id, entp, function(err, result) {
              if (!error(args, err, cb)) {
                ent.id = entp.id;
                seneca.log.debug('save/insert', ent, desc);
                cb(null,ent);
              }
            });
          }
        }
      });
    },



    /**
     * load first matching item based on id
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */
    load: function(args, cb) {
      assert(cb);
      assert(args);
      assert(args.qent);
      assert(args.q);

      var qent = args.qent;
      var q = args.q;
      var query;
      var queryFunc;

      ensureDomain(args, qent, function(err, sdb, domainName) {
        if (!error(args, err, cb)) {
          if (q.id) {
            query = q.id;
            queryFunc = sdb.getItem;
          }
          else { 
            // handle select style query ??
            assert.fail("", "select style query not implemented for load");
          }

          queryFunc(domainName, query, function(err, entp) {
            if (!error(args, err, cb)) {
              var fent = null;
              if (entp) {
                entp = deserialize(entp);
                fent = qent.make$(entp);
              }
              seneca.log.debug('load', q, fent, desc);
              cb(null, fent);
            }
          });
        }
      });
    },



    /**
     * return a list of object based on the supplied query, if no query is supplied
     * then 'select * from ...'
     * 
     * Notes: trivial implementation and unlikely to perform well due to list copy
     *        also only takes the first page of results from simple DB should in fact
     *        follow paging model
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * a=1, b=2 simple
     * next paging is optional in simpledb
     * limit$ ->
     * use native$
     */
    list: function(args, cb) {
      assert(cb);
      assert(args);
      assert(args.qent);
      assert(args.q);

      var qent = args.qent;
      var q = args.q;

      assert(args);
      assert(q);
      assert(cb);

      ensureDomain(args, qent, function(err, sdb, domainName) {
        var query;
        if (!error(args,err,cb)) {
          query = buildSimpleDbSelect(domainName, q);

          sdb.select(query, function(err, entp, meta) {
            if (!error(args, err, cb)) {
              var list = [];
              _.each(entp, function(item) {
                var fent = null;
                item = deserialize(item);
                fent = qent.make$(item);
                list.push(fent);
              });
              seneca.log.debug('list', q, list.length, list[0], desc);
              cb(null, list);
            }
          });
        }
      });
    },



    /**
     * delete an item - fix this
     * 
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * { 'all$': true }
     */
    remove: function(args, cb) {
      assert(cb);
      assert(args);
      assert(args.qent);
      assert(args.q);

      var qent = args.qent;
      var q = args.q;

      ensureDomain(args, qent, function(err, sdb, domainName) {
        var query;
        if (!error(args,err,cb)) {
          if (q.all$) {
            sdb.deleteDomain(domainName, function(err, entp) {
              if (!error(args, err, cb)) {
                cb(null, entp);
              }
            });
          }
          else {
            query = buildSimpleDbSelect(domainName, q);

            sdb.select(query, function(err, entp, meta) {
              if (!error(args, err, cb)) {
                var list = [];
                _.each(entp, function(item) {
                  sdb.deleteItem(domainName, item.id, null, function(err) {
                    error(args, err, function() {});
                  });
                });
                cb(null, entp);
              }
            });
          }
        }
      });
    },



    /**
     * return the underlying native connection object
     */
    native: function(args, cb) {
      assert(cb);
      assert(args);
      assert(args.ent);

      var qent = args.ent;

      ensureDomain(args, qent, function(err, sdb, domainName) {
        cb(null, sdb);
      });
    }
  };



  /**
   * initialization
   */
  var meta = seneca.store.init(seneca, opts, store);
  desc = meta.desc;
  seneca.add({init:store.name,tag:meta.tag}, function(args,done){
    configure(opts,function(err){
      if (err) {
        return seneca.fail({code:'entity/configure',store:store.name,error:err,desc:desc},done);
      } 
      else done();
    });
  });
  return { name:store.name, tag:meta.tag };
};



/**
 * deserialize data from simple DB: objects and boolans need conversion from string
 * TODO: this should most likely live in the simpledb driver NOT here...
 */
function deserialize(data) {
  var result = {};
  _.each(data, function(value, key, list) {
    if (typeof value === 'string') {

      if (value === 'true') { data[key] = true; }
      if (value === 'false') { data[key] = false; }

      if (value.match(/[\d]+-[\d]+-[\d]+T/g)) { 
        data[key] = new Date(value.replace(/^["\s]+|["\s+]$/g,'')); 
      }

      if (value.match(/^[\{\[].*[\}\]]$/g)) {
        try {
          data[key] = JSON.parse(value);
        }
        catch (e) {
          // not an object continue in any case...
        }
      }
    }
  });
  return data;
}



/**
 * make a GUID
 * Notes: needs better seed generation.
 */
function makeGuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c){
    var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
    return v.toString(16);
  });
}



/**
 * convert a JSON formatted query to simple db where syntax
 * e.g. { p2: 'v2', p1: 'v1x' } => where p2='v2' and p1='v1x'
 */
function convertQueryFromJsonToWhere(qin) {
  _.each(qin, function(value, key, list) {
    if (_.isNumber(qin[key])) {
      qin[key] = "" + qin[key];
    }
  });
  var checked = {};
  _.each(qin, function(value, key, list) {
    checked[escapeStr(key)] = escapeStr(value);
  });
  var s = JSON.stringify(checked);
  s = s.replace(/\"([a-zA-Z0-9_]+)\"\:/g, "$1:");
  s = s.replace(/:/g, '=');
  s = s.replace(/[\{\}]/g, '');
  return 'where ' + s.replace(/,/g, ' and ');
}



/**
 * builds a simple db select statement from a mongo style JSON query
 */
function buildSimpleDbSelect(domainName, q) {
  var query;
  if (_.keys(q).length > 0) {
    var where = convertQueryFromJsonToWhere(q);
    query = "select * from `" + escapeStr(domainName) + "` " + where;
  }
  else {
    query = "select * from `" + escapeStr(domainName) + "`";
  }
  return query;
}



var escapeStr = function(input) {
  var str = "" + input;
  return str.replace(/[\0\b\t\x08\x09\x1a\n\r'"\\\%]/g, function (char) {
    switch (char) {
      case "\0":
        return "\\0";
      case "\x08":
        return "\\b";
      case "\b": 
        return "\\b";
      case "\x09":
        return "\\t";
      case "\t": 
        return "\\t";
      case "\x1a":
        return "\\z";
      case "\n":
        return "\\n";
      case "\r":
        return "\\r";
      case "\"":
      case "'":
      case "\\":
      case "%":
        return "\\"+char; 
    }
  });
};



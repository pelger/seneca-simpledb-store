/*jslint node: true */
/*global describe:true, it:true*/
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

var assert = require('assert');
var seneca = require('../../seneca');
var async = require('async');
var shared = seneca.test.store.shared;
var keys = require('./keys.mine');
var si = seneca();


si.use(require('..'), {keyid: keys.id, secret: keys.secret});
si.__testcount = 0;
var testcount = 0;


describe('simpledb', function(){
  it('basic', function(done){
    this.timeout(0);
    testcount++;
    shared.basictest(si, done);
  });

  it('extra', function(done){
    testcount++;
    extratest(si, done);
  });

  it('close', function(done){
    this.timeout(0);
    shared.closetest(si, testcount, done);
  });
});



function extratest(si, done) {
  console.log('EXTRA');
  async.series({
    native: function(cb) {
      var foo = si.make$('foo');
      foo.native$(function(err, sdb) {
        assert.ok(null === err);
        sdb.select("select * from foo", function(err, entp, meta) {
          assert.ok(null === err);
          cb();
        });
      });
    }
  },
  function (err, out) {
    si.__testcount++;
    done();
  });
  si.__testcount++;
}



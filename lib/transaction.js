'use strict';

const _ = require('lodash');

const TransactionBase = function (docClient, log, opts) {
  this.docClient = docClient;
  this.log = log;

  this._request = Object.assign({}, opts);
};

TransactionBase.prototype.items = function (items) {
  this._request = _.merge(this._request, {
    TransactItems: items,
  });

  return this;
};

TransactionBase.prototype._exec = function (method, callback) {
  const self = this;
  const params = this._request;

  let driver;
  if (_.isFunction(self.docClient[method])) {
    driver = self.docClient;
  } else if (_.isFunction(self.docClient.service[method])) {
    driver = self.docClient.service;
  }

  const startTime = Date.now();

  self.log.info({ params: params }, 'dynogels %s request', method.toUpperCase());
  driver[method].call(driver, params, (err, data) => {
    const elapsed = Date.now() - startTime;

    if (err) {
      self.log.warn({ err: err }, 'dynogels %s error', method.toUpperCase());
      return callback(err);
    } else {
      self.log.info({ data: data }, 'dynogels %s response - %sms', method.toUpperCase(), elapsed);
      return callback(null, data);
    }
  });
};

const TransactionWrite = function (docClient, log, opts) {
  TransactionBase.call(this, docClient, log, opts);
};
TransactionWrite.prototype = Object.create(TransactionBase.prototype);

const TransactionGet = function (docClient, log, opts) {
  TransactionBase.call(this, docClient, log, opts);
};
TransactionGet.prototype = Object.create(TransactionBase.prototype);

TransactionWrite.prototype.conditionCheck = function (params) {
  this._request = _.merge(this._request, {
    ConditionCheck: params || {},
  });

  return this;
};

TransactionWrite.prototype.exec = function (callback) {
  return this._exec('transactWrite', callback);
};

TransactionGet.prototype.exec = function (callback) {
  return this._exec('transactGet', callback);
};

const Transaction = function (docClientGetter, log) {
  return {
    write: function (opts) {
      return new TransactionWrite(docClientGetter(), log(), opts);
    },
    get: function (opts) {
      return new TransactionGet(docClientGetter(), log(), opts);
    },
  };
};

module.exports = {
  Transaction,
  TransactionGet,
  TransactionWrite,
};

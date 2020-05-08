'use strict';

const _ = require('lodash');

const TransactionBase = function (docClient, log, opts) {
  this.docClient = docClient;
  this.log = log;

  this._request = Object.assign({}, opts);
};

TransactionBase.prototype.items = function (items) {
  if (this._items) {
    throw new Error('items already specified for transaction');
  }

  this._items = items;

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

  const transactItems = [];

  this._items.forEach((item) => {
    if (typeof item.callHooks === 'function') {
      item.callHooks('_before', (callback) => {
        const data = item.buildData();

        callback(null, data);
      }, (err, data) => {
        if (err) {
          return callback(err);
        }

        const request = item.buildRequest(data);

        transactItems.push(request);
      });
    } else {
      const request = item.buildRequest(item.buildData());

      transactItems.push(request);
    }
  });

  self._request = _.merge(self._request, {
    TransactItems: transactItems,
  });

  driver[method].call(driver, params, (err, data) => {
    const elapsed = Date.now() - startTime;
    const asItems = {};

    if (err) {
      self.log.warn({ err: err }, 'dynogels %s error', method.toUpperCase());

      return callback(err);
    } else {
      self.log.info({ data: data }, 'dynogels %s response - %sms', method.toUpperCase(), elapsed);

      if (method === 'transactWrite') {
        for (let i = 0; i < this._items.length; i += 1) {
          const currItem = this._items[i];
          const currTransactItem = transactItems[i].Put || transactItems[i].Update || transactItems[i].Delete;
          const asItem = currItem.table.initItem(currTransactItem.Item || currTransactItem.Key);

          if (!asItems[currItem.table.tableName()]) {
            asItems[currItem.table.tableName()] = [];
          }

          asItems[currItem.table.tableName()].push(asItem);

          if (typeof currItem.callHooks === 'function') {
            currItem.callHooks('_after', (callback) => {
              callback(null, asItem);
            });
          }
        }

        return callback(null, asItems);
      } else {
        const asItems = [];

        for (let i = 0; i < this._items.length; i += 1) {
          const currItem = this._items[i];
          const asItem = currItem.table.initItem(data.Responses[i].Item);

          asItems.push(asItem);
        }

        return callback(null, asItems);
      }
    }
  });
};

const TransactionWrite = function (docClient, log, opts) {
  return TransactionBase.call(this, docClient, log, opts);
};
TransactionWrite.prototype = Object.create(TransactionBase.prototype);

const TransactionGet = function (docClient, log, opts) {
  return TransactionBase.call(this, docClient, log, opts);
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

const Transaction = function (getDocClient, getLogger) {
  return {
    write: function (opts) {
      return new TransactionWrite(getDocClient(), getLogger(), opts);
    },
    get: function (opts) {
      return new TransactionGet(getDocClient(), getLogger(), opts);
    },
  };
};

module.exports = {
  Transaction,
  TransactionGet,
  TransactionWrite,
};

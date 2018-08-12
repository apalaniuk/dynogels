
const _ = require('lodash');
const async = require('async');

const internals = {};

internals.buildWriteItemsRequest = (tableName, items, options) => {
  const request = {};

  request[tableName] = _.merge({}, items, options);

  return { RequestItems: request };
};

internals.batchWriteItems = (request, table, callback) => {
  const responses = [];

  const moreItemsToProcessFunc = () => request !== null && !_.isEmpty(request);

  const doFunc = (callback) => {
    table.runBatchWriteItems(request, (err, resp) => {
      if (err && err.retryable) {
        return callback();
      } else if (err) {
        return callback(err);
      }

      request = resp.UnprocessedItems;
      if (moreItemsToProcessFunc()) {
        request = { RequestItems: request };
      }
      responses.push(resp);

      return callback();
    });
  };

  const resultsFunc = (err) => {
    if (err) {
      return callback(err);
    }

    callback(null, responses);
  };

  async.doWhilst(doFunc, moreItemsToProcessFunc, resultsFunc);
};

internals.buckets = (items) => {
  const buckets = [];

  while (items.length) {
    buckets.push(items.splice(0, 25));
  }

  return buckets;
};

internals.initialBatchWriteItems = (items, table, serializer, options, callback) => {
  const serializedItems = items.map((i) => {
    const req = {};

    const serializedItem = serializer.serializeItem(table.schema, i.item);

    req[i.type] = {
      Item: serializedItem
    };

    return req;
  });

  const request = internals.buildWriteItemsRequest(table.tableName(), serializedItems, options);

  internals.batchWriteItems(request, table, callback)
};

internals.exec = (table, serializer, items) => (options, callback) => {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  async.map(internals.buckets(_.clone(items)), (req, callback) => {
    internals.initialBatchWriteItems(req, table, serializer, options, callback);
  }, (err, results) => {
    if (err) {
      return callback(err);
    }

    return callback(null, _.flatten(results));
  });
};

function WriteItems(table, serializer) {
  this.table = table;
  this.serializer = serializer;

  this.options = {};
  this.requestItems = [];
};

WriteItems.prototype.constructor = WriteItems;

WriteItems.prototype.put = function (items) {
  const putItemsRequests = items.map(item => ({
    type: 'PutRequest',
    item: item
  }));

  this.requestItems.push(...putItemsRequests);

  return this;
};

WriteItems.prototype.delete = function (items) {
  const deleteItemRequests = items.map(item => ({
    type: 'DeleteRequest',
    item: item
  }));

  this.requestItems.push(...deleteItemRequests);

  return this;
};

WriteItems.prototype.exec = function (callback) {
  return (internals.exec(this.table, this.serializer, this.requestItems))( callback);
};

/*
module.exports = function (table, serializer) {
  return new WriteItems(table, serializer);
};
*/

module.exports = WriteItems;

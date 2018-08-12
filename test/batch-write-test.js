'use strict';

const helper = require('./test-helper');
const chai = require('chai');
const Schema = require('../lib/schema');
const WriteItems = require('../lib/batchWrite');
const Item = require('../lib/item');
const Serializer = require('../lib/serializer');
const Joi = require('joi');
const _ = require('lodash');

const expect = chai.expect;

describe('Batch Wrute', () => {
  let serializer;
  let table;

  beforeEach(() => {
    serializer = helper.mockSerializer();

    table = helper.mockTable();
    table.serializer = Serializer;
    table.tableName = () => 'accounts';

    const config = {
      hashKey: 'name',
      rangeKey: 'email',
      schema: {
        name: Joi.string(),
        email: Joi.string(),
        age: Joi.number()
      }
    };

    table.schema = new Schema(config);
  });

  describe('#writeItems', () => {
    it('should return an object with put and delete function', (done) => {
      const writeItems = new WriteItems(table, Serializer);

      writeItems
        .put(new Array(100).fill({name: 'someName'}).map(i => {
          i.email = `${Math.random().toString()}@xyz.com`;

          return i;
        }))
        .exec((err, result) => {
          console.log(err);
          console.log(result);

          done();
        });

      console.log(writeItems);
    });
  });
});

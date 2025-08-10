import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import SchemaParser from '../core/SchemaParser.js'
import ValueNormalizer from '../utils/ValueNormalizer.js'

class MultiFieldQueryEngine {
  constructor(collection) {
    this.collection = collection;
  }

  async findByAnd(conditions, options = {}) {
    const { limit = 1000, offset = 0 } = options;
    
    const strategy = await this._planAndQuery(conditions);
    
    if (strategy.useIndex) {
      return await this._executeIndexedAndQuery(conditions, strategy, limit, offset);
    } else {
      return await this._executeTableScanAndQuery(conditions, limit, offset);
    }
  }

  async findByOr(conditions, options = {}) {
    const { limit = 1000, offset = 0 } = options;
    
    const strategy = await this._planOrQuery(conditions);
    
    if (strategy.useIndex) {
      return await this._executeIndexedOrQuery(conditions, strategy, limit, offset);
    } else {
      return await this._executeTableScanOrQuery(conditions, limit, offset);
    }
  }

  async findByComplex(query, options = {}) {
    return await this._executeComplexQuery(query, options);
  }

  async _planAndQuery(conditions) {
    const fields = Object.keys(conditions);
    const indexedFields = SchemaParser.getIndexedFields(this.collection.schema);
    
    const selectivity = {};
    for (const field of fields) {
      if (indexedFields.includes(field)) {
        const stats = await this._getFieldSelectivity(field, conditions[field]);
        selectivity[field] = stats;
      }
    }
    
    if (Object.keys(selectivity).length === 0) {
      return { useIndex: false, reason: 'No indexed fields in query' };
    }
    
    const mostSelective = Object.entries(selectivity)
      .sort(([,a], [,b]) => a.estimatedResults - b.estimatedResults)[0];
    
    return {
      useIndex: true,
      startField: mostSelective[0],
      selectivity: selectivity,
      strategy: 'INDEX_INTERSECT'
    };
  }

  async _planOrQuery(conditions) {
    const fields = Object.keys(conditions);
    const indexedFields = SchemaParser.getIndexedFields(this.collection.schema);
    
    const indexedConditions = [];
    const nonIndexedConditions = [];
    
    for (const field of fields) {
      if (indexedFields.includes(field)) {
        indexedConditions.push(field);
      } else {
        nonIndexedConditions.push(field);
      }
    }
    
    if (indexedConditions.length === 0) {
      return { useIndex: false, reason: 'No indexed fields in OR query' };
    }
    
    return {
      useIndex: true,
      indexedFields: indexedConditions,
      nonIndexedFields: nonIndexedConditions,
      strategy: 'INDEX_UNION'
    };
  }

  async _getFieldSelectivity(field, value) {
    if (this.collection.shardedIndices.has(field)) {
      const shardedIndex = this.collection.shardedIndices.get(field);
      const results = await shardedIndex.get(value);
      return { estimatedResults: results.length };
    } else if (this.collection.loadedIndices.has(field)) {
      const index = this.collection.loadedIndices.get(field);
      const key = ValueNormalizer.normalize(value);
      const results = index.get(key) || [];
      return { estimatedResults: results.length };
    }
    return { estimatedResults: 10000 }; // High estimate for non-indexed
  }

  async _executeIndexedAndQuery(conditions, strategy, limit, offset) {
    const startField = strategy.startField;
    const startValue = conditions[startField];
    
    const candidates = await this.collection.findByField(startField, startValue, { 
      limit: 10000 
    });
    
    const remainingConditions = { ...conditions };
    delete remainingConditions[startField];
    
    let filtered = candidates;
    for (const [field, value] of Object.entries(remainingConditions)) {
      filtered = filtered.filter(doc => this._matchesCondition(doc, field, value));
    }
    
    return filtered.slice(offset, offset + limit);
  }

  async _executeIndexedOrQuery(conditions, strategy, limit, offset) {
    const resultMap = new Map();
    
    for (const field of strategy.indexedFields) {
      const value = conditions[field];
      const results = await this.collection.findByField(field, value, { limit: 5000 });
      
      for (const doc of results) {
        resultMap.set(doc.id, doc);
      }
    }
    
    if (strategy.nonIndexedFields.length > 0) {
      const nonIndexedConditions = {};
      for (const field of strategy.nonIndexedFields) {
        nonIndexedConditions[field] = conditions[field];
      }
      
      const tableScanResults = await this._executeTableScanOrQuery(nonIndexedConditions, 5000, 0);
      
      for (const doc of tableScanResults) {
        resultMap.set(doc.id, doc);
      }
    }
    
    const results = Array.from(resultMap.values());
    return results.slice(offset, offset + limit);
  }

  async _executeTableScanAndQuery(conditions, limit, offset) {
    const results = [];
    let skipped = 0;
    
    const allDocs = await this.collection.storage.getAllDocuments();
    
    for (const doc of allDocs) {
      if (this._documentMatchesConditions(doc, conditions, 'AND')) {
        if (skipped < offset) {
          skipped++;
          continue;
        }
        results.push(doc);
        if (results.length >= limit) break;
      }
    }
    
    return results;
  }

  async _executeTableScanOrQuery(conditions, limit, offset) {
    const resultMap = new Map();
    
    const allDocs = await this.collection.storage.getAllDocuments();
    
    for (const doc of allDocs) {
      if (this._documentMatchesConditions(doc, conditions, 'OR')) {
        resultMap.set(doc.id, doc);
      }
    }
    
    const results = Array.from(resultMap.values());
    return results.slice(offset, offset + limit);
  }

  async _executeComplexQuery(query, options = {}) {
    if (query.$and) {
      return await this._executeAndArray(query.$and, options);
    }
    
    if (query.$or) {
      return await this._executeOrArray(query.$or, options);
    }
    
    return await this.findByAnd(query, options);
  }

  async _executeAndArray(conditions, options) {
    let results = null;
    
    for (const condition of conditions) {
      let conditionResults;
      
      if (condition.$or) {
        conditionResults = await this._executeOrArray(condition.$or, { limit: 10000 });
      } else if (condition.$and) {
        conditionResults = await this._executeAndArray(condition.$and, { limit: 10000 });
      } else {
        const field = Object.keys(condition)[0];
        const value = condition[field];
        conditionResults = await this.collection.findByField(field, value, { limit: 10000 });
      }
      
      if (results === null) {
        results = conditionResults;
      } else {
        const resultMap = new Map(results.map(doc => [doc.id, doc]));
        results = conditionResults.filter(doc => resultMap.has(doc.id));
      }
    }
    
    const { limit = 1000, offset = 0 } = options;
    return results.slice(offset, offset + limit);
  }

  async _executeOrArray(conditions, options) {
    const resultMap = new Map();
    
    for (const condition of conditions) {
      let conditionResults;
      
      if (condition.$and) {
        conditionResults = await this._executeAndArray(condition.$and, { limit: 10000 });
      } else if (condition.$or) {
        conditionResults = await this._executeOrArray(condition.$or, { limit: 10000 });
      } else {
        const field = Object.keys(condition)[0];
        const value = condition[field];
        conditionResults = await this.collection.findByField(field, value, { limit: 10000 });
      }
      
      for (const doc of conditionResults) {
        resultMap.set(doc.id, doc);
      }
    }
    
    const results = Array.from(resultMap.values());
    const { limit = 1000, offset = 0 } = options;
    return results.slice(offset, offset + limit);
  }

  _documentMatchesConditions(doc, conditions, operator) {
    const matches = [];
    
    for (const [field, value] of Object.entries(conditions)) {
      const match = this._matchesCondition(doc, field, value);
      matches.push(match);
    }
    
    if (operator === 'AND') {
      return matches.every(m => m);
    } else if (operator === 'OR') {
      return matches.some(m => m);
    }
    
    return false;
  }

  _matchesCondition(doc, field, expectedValue) {
    const docValue = doc[field];
    
    if (docValue === undefined || docValue === null) {
      return false;
    }
    
    if (Array.isArray(docValue)) {
      return docValue.some(item => 
        ValueNormalizer.normalize(item) === ValueNormalizer.normalize(expectedValue)
      );
    }
    
    if (field.includes('.')) {
      const nestedValue = this._getNestedValue(doc, field);
      return ValueNormalizer.normalize(nestedValue) === ValueNormalizer.normalize(expectedValue);
    }
    
    return ValueNormalizer.normalize(docValue) === ValueNormalizer.normalize(expectedValue);
  }

  _getNestedValue(obj, path) {
    return path.split('.').reduce((current, key) => current && current[key], obj);
  }

  async explain(query) {
    const conditions = query;
    const fields = Object.keys(conditions);
    const indexedFields = SchemaParser.getIndexedFields(this.collection.schema);
    
    return {
      type: 'SIMPLE_MULTI_FIELD',
      fields: fields,
      indexedFields: fields.filter(f => indexedFields.includes(f)),
      nonIndexedFields: fields.filter(f => !indexedFields.includes(f)),
      strategy: fields.filter(f => indexedFields.includes(f)).length > 0 ? 'INDEX_INTERSECT' : 'TABLE_SCAN'
    };
  }
}

export default MultiFieldQueryEngine;


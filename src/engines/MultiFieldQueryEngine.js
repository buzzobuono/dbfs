import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import SchemaParser from '../core/SchemaParser.js'
import ValueNormalizer from '../utils/ValueNormalizer.js'

class MultiFieldQueryEngine {
  
  constructor(collection) {
    this.collection = collection;
  }

  async findByField(field, value) {
    const indexedFields = SchemaParser.getIndexedFields(this.collection.schema);
    
    if (indexedFields.includes(field)) {
      return await this._indexScan(field, value);
    } else {
      return await this._tableScan(field, value);
    }
  }

  async _indexScan(field, value) {
    if (this.collection.shardedIndices.has(field)) {
      const shardedIndex = this.collection.shardedIndices.get(field);
      const ids = await shardedIndex.get(value);
      return await this.collection._loadDocuments(ids);
    }
    return [];
  }

  async _tableScan(field, value) {
    const results = [];
    
    const allDocs = await this.storage.getAllDocuments();
    
    for (const doc of allDocs) {
      if (this._documentMatches(doc, field, value)) {
        results.push(doc);
      }
    }
    
    return results;
  }
  async findByAnd(conditions) {
    const strategy = await this._planAndQuery(conditions);
    
    console.log('Plan And Query\n Conditions:');
    console.log(conditions);
    console.log('Strategy:');
    console.log(strategy)
    
    if (strategy.useIndex) {
      return await this._executeIndexedAndQuery(conditions, strategy);
    } else {
      return await this._executeTableScanAndQuery(conditions);
    }
  }

  async findByOr(conditions) {
    const strategy = await this._planOrQuery(conditions);
    
    console.log('Plan Or Query\n Conditions:');
    console.log(conditions);
    console.log('Strategy:');
    console.log(strategy)
    
    if (strategy.useIndex) {
      return await this._executeIndexedOrQuery(conditions, strategy);
    } else {
      return await this._executeTableScanOrQuery(conditions);
    }
  }

  async findByComplex(query, options = {}) {
    const { limit, offset = 0 } = options;
    const results = await this._executeComplexQuery(query);
    return typeof limit === 'number' ? results.slice(offset, offset + limit) : results;
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

  async _executeCompositeIndexQuery(conditions, strategy) {
    const { compositeIndex, matchedFields, unmatchedFields } = strategy;
    
    // Extract values for matched fields in correct order
    const matchedValues = matchedFields.map(field => conditions[field]);
    
    let candidateIds;
    
    if (unmatchedFields.length === 0) {
      // Perfect match - use exact query
      console.log(`🎯 Exact composite match on [${matchedFields.join(', ')}]`);
      candidateIds = await compositeIndex.getExact(matchedValues);
    } else {
      // Partial match - use prefix query
      console.log(`🎯 Prefix composite match on [${matchedFields.join(', ')}], filtering [${unmatchedFields.join(', ')}]`);
      candidateIds = await compositeIndex.getPrefix(matchedValues);
    }
    
    if (candidateIds.length === 0) {
      return [];
    }
    
    // Load candidate documents
    const candidates = await this.collection.loadDocumentsBatch(candidateIds);
    
    // Apply remaining conditions not covered by composite index
    let filtered = candidates;
    if (unmatchedFields.length > 0) {
      const remainingConditions = {};
      for (const field of unmatchedFields) {
        remainingConditions[field] = conditions[field];
      }
      
      results = candidates.filter(doc =>
      Object.entries(remainingConditions).every(([field, value]) =>
      this._matchesCondition(doc, field, value)
      )
      );
    }
    
    return results;
  }
  
  async _executeIndexedAndQuery(conditions, strategy) {
    if (strategy.strategy === 'COMPOSITE_INDEX') {
      return await this._executeCompositeIndexQuery(conditions, strategy, limit, offset);
    }
    
    const startField = strategy.startField;
    const startValue = conditions[startField];
    
    const candidates = await this.findByField(startField, startValue);
    
    const remainingConditions = { ...conditions };
    delete remainingConditions[startField];
    
    let filtered = candidates;
    for (const [field, value] of Object.entries(remainingConditions)) {
      filtered = filtered.filter(doc => this._matchesCondition(doc, field, value));
    }
    
    return filtered;
  }

  async _executeIndexedOrQuery(conditions, strategy) {
    const resultMap = new Map();
    
    for (const field of strategy.indexedFields) {
      const value = conditions[field];
      const results = await this.findByField(field, value, { limit: 5000 });
      
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
    return results;
  }

  async _executeTableScanAndQuery(conditions, limit, offset) {
    const results = [];
    
    const allDocs = await this.collection.storage.getAllDocuments();
    
    for (const doc of allDocs) {
      if (this._documentMatchesConditions(doc, conditions, 'AND')) {
        results.push(doc);
      }
    }
    
    return results;
  }

  async _executeTableScanOrQuery(conditions) {
    const resultMap = new Map();
    
    const allDocs = await this.collection.storage.getAllDocuments();
    
    for (const doc of allDocs) {
      if (this._documentMatchesConditions(doc, conditions, 'OR')) {
        resultMap.set(doc.id, doc);
      }
    }
    
    const results = Array.from(resultMap.values());
    return results;
  }

  async _executeComplexQuery(query) {
    if (query.$and) {
      return await this._executeAndArray(query.$and);
    }
    
    if (query.$or) {
      return await this._executeOrArray(query.$or);
    }
    
    return await this.findByAnd(query);
  }

  async _executeAndArray(conditions) {
    let results = null;
    
    var remainingAndCondition = {};
    
    for (const condition of conditions) {
      let conditionResults = null;
      
      if (condition.$or) {
        conditionResults = await this._executeOrArray(condition.$or);
      } else if (condition.$and) {
        conditionResults = await this._executeAndArray(condition.$and);
      } else {
        const field = Object.keys(condition)[0];
        const value = condition[field];
        remainingAndCondition[field] = value;
        continue;
      }
      
      if (results === null) {
        results = conditionResults;
      } else {
        const resultMap = new Map(results.map(doc => [doc.id, doc]));
        results = conditionResults.filter(doc => resultMap.has(doc.id));
      }
    }
         
    if (Object.keys(remainingAndCondition).length != 0) {
      let conditionResults = await this.collection.findByAnd(remainingAndCondition);
      
      if (results === null) {
        results = conditionResults;
      } else {
        const resultMap = new Map(results.map(doc => [doc.id, doc]));
        results = conditionResults.filter(doc => resultMap.has(doc.id));
      }
    }
    
    return results;
  }

  async _executeOrArray(conditions) {
    const resultMap = new Map();
    
    var remainingOrCondition = {};
    
    for (const condition of conditions) {
      
      let conditionResults;
      
      if (condition.$and) {
        conditionResults = await this._executeAndArray(condition.$and);
      } else if (condition.$or) {
        conditionResults = await this._executeOrArray(condition.$or);
      } else {
        const field = Object.keys(condition)[0];
        const value = condition[field];
        remainingOrCondition[field] = value;
        continue;
      }
      
      for (const doc of conditionResults) {
        resultMap.set(doc.id, doc);
      }
    }
    
    if (Object.keys(remainingOrCondition).length != 0) {
      let conditionResults = await this.collection.findByOr(remainingOrCondition);
     
      for (const doc of conditionResults) {
        resultMap.set(doc.id, doc);
      }
    }
    
    const results = Array.from(resultMap.values());
    return results;
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


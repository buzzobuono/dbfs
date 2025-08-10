import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import SchemaParser from '../core/SchemaParser.js'
import TopNHeap from '../utils/TopNHeap.js'
import ValueNormalizer from '../utils/ValueNormalizer.js'

class OrderByEngine {
  constructor(collection) {
    this.collection = collection;
    this.sortCache = new Map();
  }

  async findWithOrderBy(query = {}, orderBy, options = {}) {
    const { limit = 1000, offset = 0 } = options;
    
    const strategy = await this._planOrderByQuery(query, orderBy, limit, offset);
    
    switch (strategy.type) {
      case 'INDEX_SCAN_ORDERED':
        return await this._executeIndexScanOrdered(query, orderBy, strategy, limit, offset);
      
      case 'LOAD_AND_SORT':
        return await this._executeLoadAndSort(query, orderBy, strategy, limit, offset);
      
      case 'TOP_N_OPTIMIZATION':
        return await this._executeTopN(query, orderBy, strategy, limit, offset);
      
      default:
        return await this._executeLoadAndSort(query, orderBy, strategy, limit, offset);
    }
  }

  async _planOrderByQuery(query, orderBy, limit, offset) {
    const orderFields = this._parseOrderBy(orderBy);
    const indexedFields = SchemaParser.getIndexedFields(this.collection.schema);
    const queryFields = Object.keys(query);
    
    const strategy = {
      type: 'LOAD_AND_SORT',
      orderFields: orderFields,
      canUseIndex: false,
      indexField: null,
      estimatedRows: 0,
      estimatedCost: 10000
    };
    
    const firstOrderField = orderFields[0];
    if (indexedFields.includes(firstOrderField.field)) {
      strategy.canUseIndex = true;
      strategy.indexField = firstOrderField.field;
      
      if (queryFields.length === 0) {
        strategy.type = 'INDEX_SCAN_ORDERED';
        strategy.estimatedCost = 1000;
      } else if (queryFields.length === 1 && queryFields[0] === firstOrderField.field) {
        strategy.type = 'INDEX_SCAN_ORDERED'; 
        strategy.estimatedCost = 100;
      }
    }
    
    if (limit <= 100 && orderFields.length === 1) {
      strategy.type = 'TOP_N_OPTIMIZATION';
      strategy.estimatedCost = 500;
    }
    
    return strategy;
  }

  _parseOrderBy(orderBy) {
    if (typeof orderBy === 'string') {
      const parts = orderBy.trim().split(/\s+/);
      return [{
        field: parts[0],
        direction: (parts[1] || 'ASC').toUpperCase()
      }];
    }
    
    if (Array.isArray(orderBy)) {
      return orderBy.map(item => {
        if (typeof item === 'string') {
          const parts = item.trim().split(/\s+/);
          return {
            field: parts[0],
            direction: (parts[1] || 'ASC').toUpperCase()
          };
        }
        return item;
      });
    }
    
    if (typeof orderBy === 'object') {
      return Object.entries(orderBy).map(([field, direction]) => ({
        field,
        direction: direction.toUpperCase()
      }));
    }
    
    throw new Error('Invalid orderBy format');
  }

  async _executeIndexScanOrdered(query, orderBy, strategy, limit, offset) {
    const orderField = strategy.orderFields[0];
    const isDescending = orderField.direction === 'DESC';
    
    const sortedKeys = await this._getSortedKeysFromIndex(strategy.indexField, isDescending);
    
    let results = [];
    let processed = 0;
    let skipped = 0;
    
    for (const { key, docIds } of sortedKeys) {
      if (Object.keys(query).length > 0 && query[strategy.indexField] !== undefined) {
        const queryValue = ValueNormalizer.normalize(query[strategy.indexField]);
        if (key !== queryValue) continue;
      }
      
      const docs = await this.collection._loadDocuments(docIds);
      
      for (const doc of docs) {
        if (!this._documentMatchesQuery(doc, query)) continue;
        
        if (skipped < offset) {
          skipped++;
          continue;
        }
        
        results.push(doc);
        if (results.length >= limit) break;
      }
      
      if (results.length >= limit) break;
      processed++;
    }
    
    if (strategy.orderFields.length > 1) {
      results = this._applySortInMemory(results, strategy.orderFields);
    }
    
    return results;
  }

  async _executeLoadAndSort(query, orderBy, strategy, limit, offset) {
    let docs;
    if (Object.keys(query).length === 0) {
      docs = await this.collection.storage.getAllDocuments();
    } else {
      docs = await this._executeQuery(query);
    }
    
    const sorted = this._applySortInMemory(docs, strategy.orderFields);
    return sorted.slice(offset, offset + limit);
  }

  async _executeTopN(query, orderBy, strategy, limit, offset) {
    const topN = new TopNHeap(limit + offset, strategy.orderFields);
    
    const allDocs = await this.collection.storage.getAllDocuments();
    
    for (const doc of allDocs) {
      if (this._documentMatchesQuery(doc, query)) {
        topN.add(doc);
      }
    }
    
    const results = topN.getSorted().slice(offset, offset + limit);
    return results;
  }

  async _getSortedKeysFromIndex(field, descending = false) {
    let index;
    
    if (this.collection.shardedIndices.has(field)) {
      index = await this.collection.shardedIndices.get(field)._getAllKeys();
    } else if (this.collection.loadedIndices.has(field)) {
      index = this.collection.loadedIndices.get(field);
    }
    
    if (!index) return [];
    
    const keyArray = Array.from(index.entries()).map(([key, docIds]) => ({
      key,
      docIds,
      sortValue: this._getSortValue(key)
    }));
    
    keyArray.sort((a, b) => {
      const comparison = this._compareValues(a.sortValue, b.sortValue);
      return descending ? -comparison : comparison;
    });
    
    return keyArray;
  }

  _applySortInMemory(docs, orderFields) {
    return docs.sort((a, b) => {
      for (const { field, direction } of orderFields) {
        const aVal = this._getFieldValue(a, field);
        const bVal = this._getFieldValue(b, field);
        
        const comparison = this._compareValues(aVal, bVal);
        
        if (comparison !== 0) {
          return direction === 'DESC' ? -comparison : comparison;
        }
      }
      return 0;
    });
  }

  _getFieldValue(doc, field) {
    if (field.includes('.')) {
      return this._getNestedValue(doc, field);
    }
    return doc[field];
  }

  _getNestedValue(obj, path) {
    return path.split('.').reduce((current, key) => current && current[key], obj);
  }

  _getSortValue(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === 'string') return value.toLowerCase();
    if (typeof value === 'number') return value;
    if (value instanceof Date) return value.getTime();
    return String(value).toLowerCase();
  }

  _compareValues(a, b) {
    if (a === null || a === undefined) return b === null || b === undefined ? 0 : -1;
    if (b === null || b === undefined) return 1;
    
    if (typeof a === 'number' && typeof b === 'number') {
      return a - b;
    }
    
    if (a instanceof Date && b instanceof Date) {
      return a.getTime() - b.getTime();
    }
    
    const aStr = String(a);
    const bStr = String(b);
    return aStr.localeCompare(bStr);
  }

  async _executeQuery(query) {
    const fields = Object.keys(query);
    
    if (fields.length === 1) {
      const field = fields[0];
      const value = query[field];
      return await this.collection.findByField(field, value, { limit: 100000 });
    } else {
      return await this.collection.multiFieldEngine.findByAnd(query, { limit: 100000 });
    }
  }

  _documentMatchesQuery(doc, query) {
    for (const [field, value] of Object.entries(query)) {
      const docValue = this._getFieldValue(doc, field);
      const normalizedDocValue = ValueNormalizer.normalize(docValue);
      const normalizedQueryValue = ValueNormalizer.normalize(value);
      
      if (normalizedDocValue !== normalizedQueryValue) {
        return false;
      }
    }
    return true;
  }
}

export default OrderByEngine;




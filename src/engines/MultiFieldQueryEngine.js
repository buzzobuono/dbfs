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
      let condition = {};
      condition[field] = value;
      return await this._executeTableScanAndQuery([condition]);
    }
  }

  async _indexScan(field, value) {
    console.log('Index scan for field ' + field);
    if (this.collection.shardedIndices.has(field)) {
      const shardedIndex = this.collection.shardedIndices.get(field);
      const ids = await shardedIndex.getExact([value]);
      return await this.collection._loadDocuments(ids);
    }
    return [];
  }
  
  async findByAnd(conditions) {
    const strategy = await this._planAndQuery(conditions);

    console.log('### Best Plan And Query ###\n Conditions:');
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

    console.log('### Plan Or Query\n Conditions:');
    console.log(conditions);
    console.log('Strategy:');
    console.log(strategy)

    if (strategy.useIndex) {
      return await this._executeIndexedOrQuery(conditions, strategy);
    } else {
      return await this._executeTableScanOrQuery(conditions);
    }
  }

  async find(query) {
    const results = await this._executeComplexQuery(query);
    return results;
  }

  async _planAndQuery(conditions) {
    const matches = [];
    const compositeMatches = await this._evaluateCompositeMatches(conditions);
    matches.push(...compositeMatches);
    const intersectMatch = await this._evaluateIntersectMatch(conditions);
    matches.push(intersectMatch);
    const fullScanMatch = {
      useIndex: false,
      strategy: 'FULL_SCAN',
      estimatedSelectivity: 1
    }
    matches.push(fullScanMatch);
    
    console.log(conditions);
    for (const match of matches) {
      console.log('Candidate strategy');
      console.log(match);
    }
    
    const mostSelectiveMatch = matches
       .sort((a, b) => a.estimatedSelectivity - b.estimatedSelectivity)[0];
    
    console.log('Most selective');
    console.log(mostSelectiveMatch);
    return mostSelectiveMatch;
  }
  
  async _evaluateCompositeMatches(conditions) {
    const candidateIndices = [];

    // Check schema-defined composite indices
    if (this.collection.schema.indices) {
      for (const [indexName, fields] of Object.entries(this.collection.schema.indices)) {
        candidateIndices.push({
          indexName,
          fields,
          manager: this.collection.shardedIndices.get(indexName) || null
        });
      }
    }

    let matches = [];
    for (const candidate of candidateIndices) {
      const match = this._evaluateCompositeMatch(candidate, conditions);
      matches.push(match);
    }
  
    return matches;
  }
  
  _evaluateCompositeMatch(candidate, conditions) {
    const { indexName, fields: indexFields, manager } = candidate;

    if (!manager) return null; // Index not available
    
    const indexedFields = [];
    const nonIndexedFields = [];
    
    let indexPosition = 0;
    let queryFields = [];
    for (const condition of conditions) {
      let queryField = Object.keys(condition)[0];
      queryFields.push(queryField);
    }
    
    // Trova il prefisso consecutivo più lungo
    for (let i = 0; i < queryFields.length; i++) {
      const queryField = queryFields[i];
      
      if (indexPosition < indexFields.length && queryField === indexFields[indexPosition]) {
        // Campo trovato nella posizione consecutiva corretta
        indexedFields.push(queryField);
        indexPosition++;
      } else {
        // Interruzione del prefisso - tutti i restanti vanno in non-indexed
        nonIndexedFields.push(...queryFields.slice(i));
        break;
      }
    }
    
    const prefixLength = indexedFields.length;
    const indexCoverage = queryFields.length > 0 ? prefixLength / queryFields.length : 0;
    
    // Determina il tipo di match possibile
    const canUseExactMatch = prefixLength === queryFields.length
      && prefixLength === indexFields.length;
    
    const canUsePrefixMatch = prefixLength > 0
      && prefixLength < indexFields.length;
    
    const canUseIndexSeek = prefixLength > 0;
    
    let useIndex = canUseExactMatch || canUsePrefixMatch || canUseIndexSeek;
    let strategy;
    if (canUseExactMatch) {
      strategy = 'EXACT_MATCH';
    } else if (canUsePrefixMatch) {
      strategy = 'PREFIX_MATCH';
    } else if (canUseIndexSeek) {
      strategy = 'INDEX_SEEK_FILTER';
    }  else {
      strategy = 'NONE';
    }
    
    let match = {
      useIndex,
      strategy,
      indexManager: manager,
      indexedFields,
      nonIndexedFields,
      prefixLength,
      indexCoverage: Math.round(indexCoverage * 100) / 100,
      canUseIndexSeek,
      canUseExactMatch,
      canUsePrefixMatch,
      estimatedSelectivity: Math.pow(0.1, prefixLength),
      indexUtilization: indexFields.length > 0 ? prefixLength / indexFields.length : 0
    };
    return match;
  }
  
  async _evaluateIntersectMatch(conditions) {
    const indexedFields = SchemaParser.getIndexedFields(this.collection.schema);
    const fields = [];
    for (const condition of conditions) {
      fields.push(Object.keys(condition)[0]);
    }
    
    let canUseIndexIntersect = false;
    const indexedConditions = [];
    const nonIndexedConditions = [];
    const indexManagers = [];
    
    for (const field of fields) {
      if (indexedFields.includes(field)) {
        indexedConditions.push(field);
        canUseIndexIntersect = true;
      } else {
        nonIndexedConditions.push(field);
      }
    }

    const estimatedResults = {};
    
    for (const field of indexedConditions) {
      const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
      const value = await this._getEstimatedResults(field, condition[field]);
      estimatedResults[field] = value;
    }
    
    indexedConditions.sort((a, b) => estimatedResults[a] - estimatedResults[b]);
     
    for (const field of indexedConditions) {
      indexManagers.push(this.collection.shardedIndices.get(field));
    }

    let useIndex = canUseIndexIntersect;
    
    let strategy = 'NONE';
    if (canUseIndexIntersect) {
      strategy = 'INDEX_INTERSECT';
    }
    
    let match = {
      useIndex,
      strategy,
      indexedFields: indexedConditions,
      nonIndexedFields: nonIndexedConditions,
      numIndices: indexedConditions.length,
      indexManagers,
      canUseIndexIntersect,
      estimatedSelectivity: ( 0.1 / indexedConditions.length)
    };
    
    return match;
  }
  
  async _planOrQuery(conditions) {
    const fields = [];
    for (const condition of conditions) {
      fields.push(Object.keys(condition)[0]);
    }
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
      strategy: 'INDEX_UNION',
      useIndex: true,
      indexedFields: indexedConditions,
      nonIndexedFields: nonIndexedConditions
    };
  }
  
  async _getEstimatedResults(field, value) {
    if (this.collection.shardedIndices.has(field)) {
      const shardedIndex = this.collection.shardedIndices.get(field);
      const results = await shardedIndex.getExact([value]);
      return results.length ;
    }
    return 10000; // High estimate for non-indexed
  }
    
  async _executeIndexedAndQuery(conditions, strategy) {
    if (strategy.strategy === 'EXACT_MATCH') {
      return await this._executeExactMatch(conditions, strategy);
    } else if (strategy.strategy === 'PREFIX_MATCH') {
      return await this._executePrefixMatch(conditions, strategy);
    } else if (strategy.strategy === 'INDEX_INTERSECT') {
      return await this._executeIndexIntersect(conditions, strategy);
    } else if (strategy.strategy === 'INDEX_SEEK_FILTER') {
      return await this._executeIndexSeekFilter(conditions, strategy);
    } else {
      throw new Error("No valid strategy found");
    }
  }
  
  async _executeExactMatch(conditions, strategy) {
    const { indexedFields, nonIndexedFields, indexManager } = strategy;
    
    console.log(`Executing Exact Match on [${indexedFields.join(', ')}]`);
    
    if (nonIndexedFields.length != 0) {
      throw new Error("Incoerent input for Exact Match strategy");
    }
    
    const values = [];
    for (const condition of conditions) {
      let field = Object.keys(condition)[0];
      values.push(condition[field]);
    }
    const resultIds = await indexManager.getExact(values);
    const results = await this.collection._loadDocuments(resultIds);
    return results;
  }
  
  async _executePrefixMatch(conditions, strategy) {
    const { indexedFields, nonIndexedFields, indexManager } = strategy;
    
    console.log(`Executing Prefix Match on [${indexedFields.join(', ')}]`);
    
    if (nonIndexedFields.length != 0) {
      throw new Error("Incoerent input for Prefix Match strategy");
    }
    
    const values = [];
    for (let i = 0; i < indexedFields.length; i++) {
      const field = indexedFields[i];
      const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
      values.push(condition[field]);
    }

    const resultIds = await indexManager.getPrefix(values);
    const results = await this.collection._loadDocuments(resultIds);
    return results;
  }
  
  async _executeIndexSeekFilter(conditions, strategy) {
    const { indexedFields, nonIndexedFields, indexManager } = strategy;

    console.log(`Executing Index Seek Filter on [${indexedFields.join(', ')}], filtering [${nonIndexedFields.join(', ')}]`);
    
    const values = [];
    for (let i = 0; i < indexedFields.length; i++) {
      const field = indexedFields[i];
      const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
      values.push(condition[field]);
    }
    const resultIds = await indexManager.getExact(values);

    let results = await this.collection._loadDocuments(resultIds);

    if (nonIndexedFields.length > 0) {
      results = await this._executeInMemoryFilter(results, conditions, strategy);      
    }
    return results;
  }
  
  async _executeIndexIntersect(conditions, strategy) {
    const { indexedFields, nonIndexedFields, indexManagers } = strategy;
    
    console.log(`Executing Index Intersect for fields [${indexedFields.join(', ')}]`);
    
    let resultIds = null;
    let isFirstIteration = true;
    
    for (let i = 0; i < indexedFields.length; i++) {
      const field = indexedFields[i];
      const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
      const value = condition[field];
    
      const indexManager = indexManagers[i];
      const ids = await indexManager.getExact([value]);
      
      if (isFirstIteration) {
        resultIds = ids;
        isFirstIteration = false;
      } else {
        resultIds = resultIds.filter(id => ids.includes(id));
      }
      
      if (resultIds.length === 0) {
        return [];
      }
    }
    
    let results = await this.collection._loadDocuments(resultIds);
    
    if (nonIndexedFields.length > 0) {
      results = await this._executeInMemoryFilter(results, conditions, strategy);      
    }
    
    return results || [];
  }
  
  async _executeInMemoryFilter(results, conditions, strategy) {
    const { nonIndexedFields } = strategy;

    console.log(`Executing InMemory Filter for fields [${nonIndexedFields.join(', ')}]`);
    
    for (const field of nonIndexedFields) {
      const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
      const value = condition[field];
        
      results = results.filter(doc => this._matchesCondition(doc, field, value));
        
      // Early termination
      if (results.length === 0) {
        return [];
      }
    }
    
    return results;
    
  }
  
  async _executeIndexedOrQuery(conditions, strategy) {
    const resultMap = new Map();

    for (const field of strategy.indexedFields) {
      const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
      const value = condition[field]
      const results = await this.findByField(field, value);
      for (const doc of results) {
        resultMap.set(doc.id, doc);
      }
    }

    if (strategy.nonIndexedFields.length > 0) {
      const nonIndexedConditions = [];
      for (const field of strategy.nonIndexedFields) {
        const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
        nonIndexedConditions.push(condition);
      }

      const tableScanResults = await this._executeTableScanOrQuery(nonIndexedConditions);

      for (const doc of tableScanResults) {
        resultMap.set(doc.id, doc);
      }
    }

    const results = Array.from(resultMap.values());
    return results;
  }
  
  async _executeTableScanAndQuery(conditions) {
    console.log('Table scan for fields [' + conditions.map(cond => Object.keys(cond)[0]).join(',') + '], operator AND');
    
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
    console.log('Table scan for fields [' + conditions.map(cond => Object.keys(cond)[0]).join(',') + '], operator OR');
   
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

    var remainingAndCondition = [];

    for (const condition of conditions) {
      let conditionResults = null;

      if (condition.$or) {
        conditionResults = await this._executeOrArray(condition.$or);
      } else if (condition.$and) {
        conditionResults = await this._executeAndArray(condition.$and);
      } else {
        remainingAndCondition.push(condition);
        continue;
      }

      if (results === null) {
        results = conditionResults;
      } else {
        const resultMap = new Map(results.map(doc => [doc.id, doc]));
        results = conditionResults.filter(doc => resultMap.has(doc.id));
      }
    }

    if (remainingAndCondition.length != 0) {
      let conditionResults = await this.findByAnd(remainingAndCondition);

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

    var remainingOrCondition = [];

    for (const condition of conditions) {

      let conditionResults;

      if (condition.$and) {
        conditionResults = await this._executeAndArray(condition.$and);
      } else if (condition.$or) {
        conditionResults = await this._executeOrArray(condition.$or);
      } else {
        remainingOrCondition.push(condition);
        continue;
      }

      for (const doc of conditionResults) {
        resultMap.set(doc.id, doc);
      }
    }

    if (remainingOrCondition.length != 0) {
      let conditionResults = await this.findByOr(remainingOrCondition);

      for (const doc of conditionResults) {
        resultMap.set(doc.id, doc);
      }
    }

    const results = Array.from(resultMap.values());
    return results;
  }

  _documentMatchesConditions(doc, conditions, operator) {
    if (operator === 'AND') {
      return conditions.every(cond => {
        const [field, value] = Object.entries(cond)[0];
        return this._matchesCondition(doc, field, value);
      });
    }
    
    if (operator === 'OR') {
      return conditions.some(cond => {
        const [field, value] = Object.entries(cond)[0];
        return this._matchesCondition(doc, field, value);
      });
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
  
}

export default MultiFieldQueryEngine;


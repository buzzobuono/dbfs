import SchemaParser from '../core/SchemaParser.js'
import ValueNormalizer from '../utils/ValueNormalizer.js'

class MultiFieldQueryEngine {

  constructor(collection) {
    this.collection = collection;
  }
  
  async findByAnd(conditions) {
    const strategy = await this._planAndQuery(conditions);

    console.log('### Best Plan And Query ###\n Conditions:');
    console.log(conditions);
    console.log('Strategy:');
    console.log(strategy)
    
    return await this._executeAndQuery(conditions, strategy);
  
  }

  async findByOr(conditions) {
    const strategy = await this._planOrQuery(conditions);

    console.log('### Plan Or Query\n Conditions:');
    console.log(conditions);
    console.log('Strategy:');
    console.log(strategy)

    return await this._executeOrQuery(conditions, strategy);

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
      strategy: 'FULL_SCAN',
      estimatedSelectivity: 1
    }
    matches.push(fullScanMatch);
    
    /*console.log(conditions);
    for (const match of matches) {
      console.log('Candidate strategy');
      console.log(match);
    }*/
    
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
          manager: this.collection.indices.get(indexName) || null
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
      strategy,
      indexManager: manager,
      indexedFields,
      nonIndexedFields,
      prefixLength,
      indexCoverage: Math.round(indexCoverage * 100) / 100,
      estimatedSelectivity: Math.pow(0.1, prefixLength),
      indexUtilization: indexFields.length > 0 ? prefixLength / indexFields.length : 0
    };
    return match;
  }
  
  async _evaluateIntersectMatch(conditions) {
    const singleFieldIndexNames = SchemaParser.getSingleFieldIndexNames(this.collection.schema);
    const fields = [];
    for (const condition of conditions) {
      fields.push(Object.keys(condition)[0]);
    }
    
    let canUseIndexIntersect= false;
    
    const indexedConditions = [];
    const nonIndexedConditions = [];

    const indexManagers = [];
    
    const estimatedResults = {};
    
    for (const field of fields) {
      if (singleFieldIndexNames.has(field)) {
        const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
        const value = await this._getEstimatedResults(field, condition[field]);
        estimatedResults[field] = value;
      }
    }
 
    // Ordina by selectivity
    indexedConditions.sort((a, b) => estimatedResults[a] - estimatedResults[b]);
    
    for (const field of fields) {
      if (singleFieldIndexNames.has(field)) {
        indexedConditions.push(field);
        indexManagers.push(this.collection.indices.get(singleFieldIndexNames.get(field)));
        canUseIndexIntersect = true;
      } else {
        nonIndexedConditions.push(field);
      }
    }
    
    let strategy = 'NONE';
    if (canUseIndexIntersect) {
      strategy = 'INDEX_INTERSECT';
    }
    
    let match = {
      strategy,
      indexedFields: indexedConditions,
      nonIndexedFields: nonIndexedConditions,
      numIndices: indexedConditions.length,
      indexManagers,
      estimatedSelectivity: ( 0.1 / indexedConditions.length)
    };
    
    return match;
  }
  
  async _planOrQuery(conditions) {
    const fields = [];
    for (const condition of conditions) {
      fields.push(Object.keys(condition)[0]);
    }
    const singleFieldIndexNames = SchemaParser.getSingleFieldIndexNames(this.collection.schema);

    const indexedFields = [];
    const nonIndexedFields = [];

    const indexManagers = [];

    for (const field of fields) {
      if (singleFieldIndexNames.has(field)) {
        indexedFields.push(field);
        indexManagers.push(this.collection.indices.get(singleFieldIndexNames.get(field)));
      } else {
        nonIndexedFields.push(field);
      }
    }

    if (indexedFields.length > 0 && nonIndexedFields.length === 0) {
      return {
        strategy: 'INDEX_UNION',
        indexedFields: indexedFields,
        nonIndexedFields: nonIndexedFields,
        indexManagers: indexManagers
      };
    } else {
      return {
        strategy: 'FULL_SCAN'
      }
    }
  }
  
  async _getEstimatedResults(field, value) {
    if (this.collection.indices.has(field)) {
      const indexManager = this.collection.indices.get(field);
      const results = await indexManager.getExact([value]);
      return results.length ;
    }
    return 10000; // High estimate for non-indexed
  }
  
  async _executeAndQuery(conditions, strategy) {
    if (strategy.strategy === 'EXACT_MATCH') {
      return await this._executeExactMatch(conditions, strategy);
    } else if (strategy.strategy === 'PREFIX_MATCH') {
      return await this._executePrefixMatch(conditions, strategy);
    } else if (strategy.strategy === 'INDEX_INTERSECT') {
      return await this._executeIndexIntersect(conditions, strategy);
    } else if (strategy.strategy === 'INDEX_SEEK_FILTER') {
      return await this._executeIndexSeekFilter(conditions, strategy);
    } else if (strategy.strategy === 'FULL_SCAN') {
      return await this._executeTableScanAndQuery(conditions);
    } else {
      throw new Error("No strategy found");
    }
  }
  
  async _executeOrQuery(conditions, strategy) {
    if (strategy.strategy === 'INDEX_UNION') {
      return await this._executeIndexUnion(conditions, strategy);
    } else if (strategy.strategy === 'FULL_SCAN') {
      return await this._executeTableScanOrQuery(conditions);
    } else {
      throw new Error("No strategy found");
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
    for (const condition of conditions) {
      let field = Object.keys(condition)[0];
      values.push(condition[field]);
    }
    const resultIds = await indexManager.getPrefix(values);
    const results = await this.collection._loadDocuments(resultIds);
    return results;
  }
  
  async _executeIndexSeekFilter(conditions, strategy) {
    console.log(`Executing Index Seek Filter on [${strategy.indexedFields.join(', ')}], filtering [${strategy.nonIndexedFields.join(', ')}]`);
 
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
      
      results = this._executeInMemoryFilter(results, conditions, strategy);
      
    }
    
    return results || [];
  }
  
  async _executeInMemoryFilter(results, conditions, strategy) {
    const { indexedFields, nonIndexedFields, indexManagers } = strategy;
    
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
  
  async _executeIndexUnion(conditions, strategy) {
    const { indexedFields, nonIndexedFields, indexManagers } = strategy;

    if (nonIndexedFields.length > 0) {
      throw new Error("Incoerent input for Index Union strategy");
    }

    const resultMap = new Map();

    for (const condition of conditions) {
      const field = Object.keys(condition)[0];
      const value = condition[field]
      const indexManager = this.collection.indices.get(field);
      const ids = await indexManager.getExact([value]);
      for (const id of ids) {
        resultMap.set(id, id);
      }
    }
    const results = await this.collection._loadDocuments(resultMap.keys());
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


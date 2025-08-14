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
      const ids = await shardedIndex.getExact([value]);
      return await this.collection._loadDocuments(ids);
    }
    return [];
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

  async find(query) {
    const results = await this._executeComplexQuery(query);
    return results;
  }

  async _planAndQuery(conditions) {
    const compositeMatch = await this._findBestCompositeIndex(conditions);
    if (compositeMatch) {
      return {
        useIndex: true,
        strategy: 'COMPOSITE_INDEX',
        indexName: compositeMatch.indexName,
        compositeIndex: compositeMatch.compositeIndex,
        matchedFields: compositeMatch.matchedFields,
        unmatchedFields: compositeMatch.unmatchedFields,
        estimatedCost: 10, // Very low cost
        reason: `Using composite index '${compositeMatch.indexName}' for ${compositeMatch.matchedFields.length}/${compositeMatch.compositeIndex.fields.length} fields`
      };
    }

    // Fall back to single field analysis
    const indexedFields = SchemaParser.getIndexedFields(this.collection.schema);
    const fields = [];
    for (const condition of conditions) {
      fields.push(Object.keys(condition)[0]);
    }
    const selectivity = {};
    for (const field of fields) {
      if (indexedFields.includes(field)) {
        const stats = await this._getFieldSelectivity(field, conditions[field]);
        selectivity[field] = stats;
      }
    }

    if (Object.keys(selectivity).length === 0) {
      return {
        useIndex: false,
        strategy: 'TABLE_SCAN',
        estimatedCost: 10000,
        reason: 'No indexed fields in query'
      };
    }

    const mostSelective = Object.entries(selectivity)
      .sort(([, a], [, b]) => a.estimatedResults - b.estimatedResults)[0];

    return {
      useIndex: true,
      strategy: 'INDEX_INTERSECT',
      startField: mostSelective[0],
      selectivity: selectivity,
      estimatedCost: 100,
      reason: `Starting with most selective field '${mostSelective[0]}' (${mostSelective[1].estimatedResults} results)`
    };
  }

  async _findBestCompositeIndex(conditions) {
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

    if (candidateIndices.length === 0) {
      return null;
    }

    let bestMatch = null;
    let bestScore = 0;

    for (const candidate of candidateIndices) {
      const match = this._evaluateCompositeMatch(candidate, conditions);

      if (match && match.score > bestScore) {
        bestScore = match.score;
        bestMatch = match;
      }
    }

    return bestMatch;
  }

  _evaluateCompositeMatch(candidate, conditions) {
    const { indexName, fields: indexFields, manager } = candidate;

    if (!manager) return null; // Index not available

    const matchedFields = [];
    const unmatchedFields = [];

    // Check prefix matching (fields must match in order for optimal performance)
    let consecutiveMatches = 0;
    for (let i = 0; i < indexFields.length; i++) {
      const indexField = indexFields[i];

      if (conditions.some(obj => indexField in obj)) {
        matchedFields.push(indexField);
        consecutiveMatches++;
      } else {
        break; 
      }
    }

    // Find unmatched query fields
    for (const condition of conditions) {
      let queryField = Object.keys(condition)[0];
      if (!matchedFields.includes(queryField)) {
        unmatchedFields.push(queryField);
      }
    }
    if (consecutiveMatches === 0) {
      return null;
    }

    // Calculate score based on:
    // 1. Number of consecutive matches (higher is better)
    // 2. Percentage of query covered (higher is better)
    // 3. Whether it's an exact match (bonus)
    const coverageRatio = matchedFields.length / candidate.fields.length;
    const exactMatch = matchedFields.length === candidate.fields.length;
    const prefixOptimal = consecutiveMatches === matchedFields.length;

    let score = consecutiveMatches * 10; // Base score
    score += coverageRatio * 5; // Coverage bonus
    if (exactMatch) score += 20; // Exact match bonus
    if (prefixOptimal) score += 10; // Prefix optimization bonus

    return {
      indexName,
      compositeIndex: manager,
      matchedFields,
      unmatchedFields,
      consecutiveMatches,
      coverageRatio,
      exactMatch,
      prefixOptimal,
      score,
      estimatedSelectivity: Math.pow(0.1, matchedFields.length) // Rough estimate
    };
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
    }
    return { estimatedResults: 10000 }; // High estimate for non-indexed
  }

  async _executeCompositeIndexQuery(conditions, strategy) {
    const { compositeIndex, matchedFields, unmatchedFields } = strategy;

    // Extract values for matched fields in correct order
    const matchedValues = matchedFields.map(field => {
      const found = conditions.find(obj => Object.prototype.hasOwnProperty.call(obj, field));
      return found ? found[field] : undefined;
    });

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
    const candidates = await this.collection._loadDocuments(candidateIds);

    // Apply remaining conditions not covered by composite index
    let results = candidates;
    if (unmatchedFields.length > 0) {
      const remainingConditions = unmatchedFields.map(field => {
        const found = conditions.find(obj => Object.prototype.hasOwnProperty.call(obj, field));
        return found ;
      });

      results = candidates.filter(doc =>
        remainingConditions.every(cond => {
          const [field, value] = Object.entries(cond)[0];
          return this._matchesCondition(doc, field, value);
        })
      );

    }

    return results;
  }

  async _executeIndexedAndQuery(conditions, strategy) {
    if (strategy.strategy === 'COMPOSITE_INDEX') {
      return await this._executeCompositeIndexQuery(conditions, strategy);
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
      const condition = conditions.find(cond => Object.prototype.hasOwnProperty.call(cond, field));
      const value = condition[field]
      const results = await this.findByField(field, value);
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
  
  async _executeTableScanAndQuery(conditions) {
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
    const matches = [];

    conditions.every(cond => {
      const [field, value] = Object.entries(cond)[0];
      const match = this._matchesCondition(doc, field, value);
      matches.push(match);
    });

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
  
}

export default MultiFieldQueryEngine;


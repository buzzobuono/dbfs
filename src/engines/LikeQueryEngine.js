import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import SchemaParser from '../core/SchemaParser.js'
import PatternMatcher from '../utils/PatternMatcher.js'

class LikeQueryEngine {
  
  constructor(collection) {
    this.collection = collection;
    this.patternCache = new Map();
  }

  async findByLike(field, pattern, options = {}) {
    const { limit = 1000, offset = 0, caseSensitive = false } = options;
    
    const patternInfo = PatternMatcher.analyzePattern(pattern, caseSensitive);
    
    if (patternInfo.canUseIndex) {
      return await this._executeIndexedLike(field, pattern, patternInfo, limit, offset);
    } else {
      return await this._executeTableScanLike(field, pattern, patternInfo, limit, offset);
    }
  }

  async findByLikes(conditions, operator = 'AND', options = {}) {
    const { limit = 1000, offset = 0 } = options;
    
    if (operator === 'AND') {
      return await this._executeLikeAnd(conditions, limit, offset);
    } else {
      return await this._executeLikeOr(conditions, limit, offset);
    }
  }

  async findByFullText(field, searchTerms, options = {}) {
    const { operator = 'AND', ...otherOptions } = options;
    const terms = searchTerms.toLowerCase().split(/\s+/).filter(term => term.length > 0);
    
    const termConditions = {};
    for (const term of terms) {
      termConditions[`${field}_${term}`] = `%${term}%`;
    }
    
    // Use original field for all terms
    const conditions = {};
    for (const term of terms) {
      conditions[field] = `%${term}%`;
    }
    
    if (operator === 'AND') {
      return await this._executeLikeAnd(termConditions, otherOptions.limit || 1000, otherOptions.offset || 0);
    } else {
      return await this._executeLikeOr(termConditions, otherOptions.limit || 1000, otherOptions.offset || 0);
    }
  }

  async _executeIndexedLike(field, pattern, patternInfo, limit, offset) {
    const candidates = await this._getPrefixMatches(field, patternInfo.prefixPattern);
    
    if (candidates.length === 0) return [];
    
    const docs = await this.collection._loadDocuments(candidates.slice(0, limit * 2));
    
    const filtered = docs.filter(doc => {
      if (!doc || !doc[field]) return false;
      return patternInfo.regex.test(String(doc[field]));
    });
    
    return filtered.slice(offset, offset + limit);
  }

  async _executeTableScanLike(field, pattern, patternInfo, limit, offset) {
    const results = [];
    let skipped = 0;
    
    const allDocs = await this.collection.storage.getAllDocuments();
    
    for (const doc of allDocs) {
      if (PatternMatcher.documentMatches(doc, field, patternInfo)) {
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

  async _getPrefixMatches(field, prefix) {
    const candidates = [];
    
    if (this.collection.shardedIndices.has(field)) {
      const shardedIndex = this.collection.shardedIndices.get(field);
      candidates.push(...await this._getPrefixFromShardedIndex(shardedIndex, prefix));
    } else if (this.collection.loadedIndices.has(field)) {
      const index = this.collection.loadedIndices.get(field);
      candidates.push(...this._getPrefixFromIndex(index, prefix));
    }
    
    return candidates;
  }

  async _getPrefixFromShardedIndex(shardedIndex, prefix) {
    const candidates = [];
    const prefixShards = this._getPrefixShards(prefix, 4);
    
    for (const shardKey of prefixShards) {
      const shard = await shardedIndex._loadShard(shardKey);
      for (const [key, docIds] of shard.entries()) {
        if (key.startsWith(prefix)) {
          candidates.push(...docIds);
        }
      }
    }
    
    return candidates;
  }

  _getPrefixFromIndex(index, prefix) {
    const candidates = [];
    
    for (const [key, docIds] of index.entries()) {
      if (key.startsWith(prefix)) {
        candidates.push(...docIds);
      }
    }
    
    return candidates;
  }

  _getPrefixShards(prefix, maxShards) {
    const shards = new Set();
    
    // Add a few likely shards for prefix matching
    for (let i = 0; i < maxShards; i++) {
      const testValue = prefix + String.fromCharCode(97 + i); // a, b, c, d
      const hash = createHash('md5').update(testValue).digest('hex');
      const shardKey = parseInt(hash.substring(0, 2), 16) % 16;
      shards.add(shardKey);
    }
    
    return Array.from(shards);
  }

  async _executeLikeAnd(conditions, limit, offset) {
    console.log(conditions);
    const patterns = Object.entries(conditions).map(([field, pattern]) => ({
      field,
      pattern,
      info: PatternMatcher.analyzePattern(pattern)
    }));
    console.log(patterns);
    patterns.sort((a, b) => {
      if (a.info.canUseIndex && !b.info.canUseIndex) return -1;
      if (!a.info.canUseIndex && b.info.canUseIndex) return 1;
      if (a.info.prefixPattern && b.info.prefixPattern) {
        return b.info.prefixPattern.length - a.info.prefixPattern.length;
      }
      return 0;
    });
    
    const startPattern = patterns[0];
    let candidates = await this.findByLike(startPattern.field, startPattern.pattern, { 
      limit: 10000 
    });
    
    for (let i = 1; i < patterns.length; i++) {
      const { field, pattern, info } = patterns[i];
      
      candidates = candidates.filter(doc => 
        PatternMatcher.documentMatches(doc, field, info)
      );
    }
    
    return candidates.slice(offset, offset + limit);
  }

  async _executeLikeOr(conditions, limit, offset) {
    const resultMap = new Map();
    
    for (const [field, pattern] of Object.entries(conditions)) {
      const results = await this.findByLike(field, pattern, { limit: 5000 });
      
      for (const doc of results) {
        resultMap.set(doc.id, doc);
      }
    }
    
    const finalResults = Array.from(resultMap.values());
    return finalResults.slice(offset, offset + limit);
  }
}

export default LikeQueryEngine;


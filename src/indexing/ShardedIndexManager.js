import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import ValueNormalizer from '../utils/ValueNormalizer.js'

class ShardedIndexManager {
  
  constructor(collectionPath, field, options = {}) {
    this.collectionPath = collectionPath;
    this.field = field;
    this.isComposite = Array.isArray(field); // ← Nuovo: detect composite
    this.fields = this.isComposite ? field : [field];
    this.shardCount = options.shardCount || 16;
    this.maxShardSize = options.maxShardSize || 50 * 1024 * 1024;
    this.indicesPath = path.join(collectionPath, '_indices');
    
    this.loadedShards = new Map();
    this.shardStats = new Map();
    
  }
  
  // ---------- COMPOSITE KEY GENERATION ----------
  
  _generateCompositeKey(values) {
    if (!this.isComposite) {
      return ValueNormalizer.normalize(values);
    }
    
    // values può essere un documento o array di valori
    let keyValues;
    if (Array.isArray(values)) {
      keyValues = values;
    } else {
      // Extract values from document for each field
      keyValues = this.fields.map(field => values[field]);
    }
    
    return keyValues
      .map(v => ValueNormalizer.normalize(v))
      .join('|');
  }

  _parseCompositeKey(compositeKey) {
    if (!this.isComposite) {
      return [compositeKey];
    }
    return compositeKey.split('|');
  }
  
  _getShardKey(values) {
    const compositeKey = this._generateCompositeKey(values);
    const hash = createHash('md5').update(compositeKey).digest('hex');
    return parseInt(hash.substring(0, 2), 16) % this.shardCount;
  }

  _getShardPath(shardKey) {
    const indexName = this.isComposite 
      ? `${this.fields.join('_')}_shard${shardKey}.json`
      : `${this.field}_shard${shardKey}.json`;
    return path.join(this.indicesPath, indexName);
  }
  
  async add(values, docId) {
    if (!this.isComposite) {
      const shardKey = this._getShardKey(values);
      const shard = await this._loadShard(shardKey);
      const normalizedValue = ValueNormalizer.normalize(value);
      
      if (!shard.has(normalizedValue)) {
        shard.set(normalizedValue, []);
      }
      
      const docIds = shard.get(normalizedValue);
      if (!docIds.includes(docId)) {
        docIds.push(docId);
      }
      
      return await this._saveShard(shardKey);
    }
    
    const compositeKey = this._generateCompositeKey(values);
    const shardKey = this._getShardKey(values);
    const shard = await this._loadShard(shardKey);
    
    if (!shard.has(compositeKey)) {
      shard.set(compositeKey, []);
    }
    
    const docIds = shard.get(compositeKey);
    if (!docIds.includes(docId)) {
      docIds.push(docId);
    }
    
    this._saveShard(shardKey);
  }

  async removeComposite(values, docId) {
    if (!this.isComposite) {
      const shardKey = this._getShardKey(values);
      const shard = await this._loadShard(shardKey);
      const normalizedValue = ValueNormalizer.normalize(value);
      
      if (shard.has(normalizedValue)) {
        const docIds = shard.get(normalizedValue);
        const index = docIds.indexOf(docId);
        if (index !== -1) {
          docIds.splice(index, 1);
          if (docIds.length === 0) {
            shard.delete(normalizedValue);
          }
        }
      }

      return await this._saveShard(shardKey);
    }
    
    const compositeKey = this._generateCompositeKey(values);
    const shardKey = this._getShardKey(values);
    const shard = await this._loadShard(shardKey);
    
    if (shard.has(compositeKey)) {
      const docIds = shard.get(compositeKey);
      const index = docIds.indexOf(docId);
      if (index !== -1) {
        docIds.splice(index, 1);
        if (docIds.length === 0) {
          shard.delete(compositeKey);
        }
        this._saveShard(shardKey);
      }
    }
  }
  
  async _loadShard(shardKey) {
    if (this.loadedShards.has(shardKey)) {
      return this.loadedShards.get(shardKey);
    }

    const shardPath = this._getShardPath(shardKey);
    let shard = new Map();
    
    if (fs.existsSync(shardPath)) {
      try {
        const shardObj = JSON.parse(fs.readFileSync(shardPath, 'utf8'));
        shard = new Map(Object.entries(shardObj));
      } catch (e) {
        console.warn(`⚠️ Corrupted shard ${shardKey} for field ${this.field}`);
        shard = new Map();
      }
    }

    this._cacheShard(shardKey, shard);
    return shard;
  }

  _cacheShard(shardKey, shard) {
    const maxCachedShards = 4;
    
    if (this.loadedShards.size >= maxCachedShards) {
      const oldestKey = this.loadedShards.keys().next().value;
      this.loadedShards.delete(oldestKey);
    }
    
    this.loadedShards.set(shardKey, shard);
  }

  async get(value) {
    const shardKey = this._getShardKey(value);
    const shard = await this._loadShard(shardKey);
    const normalizedValue = ValueNormalizer.normalize(value);
    return shard.get(normalizedValue) || [];
  }

  async _saveShard(shardKey) {
    const shard = this.loadedShards.get(shardKey);
    if (!shard) return;
    
    const shardPath = this._getShardPath(shardKey);
    const shardObj = Object.fromEntries(shard.entries());
    const tempPath = `${shardPath}.tmp`;
    
    try {
      fs.writeFileSync(tempPath, JSON.stringify(shardObj, null, 2));
      fs.renameSync(tempPath, shardPath);
    } catch (error) {
      console.error(`❌ Error saving shard ${shardKey}:`, error);
      if (fs.existsSync(tempPath)) {
        fs.unlinkSync(tempPath);
      }
    }
  }

  // ---------- ADVANCED QUERY METHODS ----------
  
  async getExact(values) {
    const compositeKey = this._generateCompositeKey(values);
    const shardKey = this._getShardKey(values);
    const shard = await this._loadShard(shardKey);
    
    return shard.get(compositeKey) || [];
  }

  async getPrefix(prefixValues) {
    if (!this.isComposite) {
      throw new Error('Prefix queries only available for composite indices');
    }
    
    const prefixKey = prefixValues
      .map(v => ValueNormalizer.normalize(v))
      .join('|');
    
    const results = [];
    
    // Need to check all shards for prefix matches (expensive but necessary)
    for (let shardKey = 0; shardKey < this.shardCount; shardKey++) {
      try {
        const shard = await this._loadShard(shardKey);
        
        for (const [key, docIds] of shard.entries()) {
          if (key.startsWith(prefixKey + '|') || key === prefixKey) {
            results.push(...docIds);
          }
        }
      } catch (e) {
        // Skip missing shards
      }
    }
    
    return [...new Set(results)]; // Remove duplicates
  }

  async getRange(prefixValues, lastFieldMin, lastFieldMax) {
    if (!this.isComposite) {
      throw new Error('Range queries only available for composite indices');
    }
    
    const prefix = prefixValues
      .map(v => ValueNormalizer.normalize(v))
      .join('|');
    
    const results = [];
    
    // Check all shards for range matches
    for (let shardKey = 0; shardKey < this.shardCount; shardKey++) {
      try {
        const shard = await this._loadShard(shardKey);
        
        for (const [key, docIds] of shard.entries()) {
          if (key.startsWith(prefix + '|')) {
            const parts = this._parseCompositeKey(key);
            const lastValue = parseFloat(parts[parts.length - 1]);
            
            if (!isNaN(lastValue) && lastValue >= lastFieldMin && lastValue <= lastFieldMax) {
              results.push(...docIds);
            }
          }
        }
      } catch (e) {
        // Skip missing shards
      }
    }
    
    return [...new Set(results)];
  }

  async getPartialMatch(conditions) {
    if (!this.isComposite) {
      throw new Error('Partial match only available for composite indices');
    }
    
    // Build partial key from available conditions
    const partialValues = this.fields.map(field => 
      conditions.hasOwnProperty(field) ? conditions[field] : null
    );
    
    // Find first null value (incomplete key)
    const completeLength = partialValues.findIndex(v => v === null);
    const actualLength = completeLength === -1 ? partialValues.length : completeLength;
    
    if (actualLength === 0) {
      throw new Error('At least one field value required for partial match');
    }
    
    if (actualLength === this.fields.length) {
      // Complete key - use exact match
      return await this.getExact(partialValues);
    } else {
      // Partial key - use prefix match
      return await this.getPrefix(partialValues.slice(0, actualLength));
    }
  }
  
  async buildFromDocuments(getAllDocuments) {
    console.log(`🏗️ Building sharded ${this.isComposite ? 'composite' : 'single'} index for: ${this.fields.join(', ')}`);
    console.time(`build-sharded-${this.fields.join('_')}`);
    
    // Clear existing shards
    this.loadedShards.clear();
    for (let i = 0; i < this.shardCount; i++) {
      const shardPath = this._getShardPath(i);
      if (fs.existsSync(shardPath)) {
        fs.unlinkSync(shardPath);
      }
    }
    
    // Initialize empty shards
    const shards = [];
    for (let i = 0; i < this.shardCount; i++) {
      shards[i] = new Map();
    }
    
    const docs = await getAllDocuments();
    let processedDocs = 0;
    
    for (const doc of docs) {
      // Check if all required fields are present
      const hasAllFields = this.fields.every(field => 
        doc[field] !== undefined && doc[field] !== null
      );
      
      if (hasAllFields) {
        if (this.isComposite) {
          // Composite index
          const compositeKey = this._generateCompositeKey(doc);
          const shardKey = this._getShardKey(doc);
          
          if (!shards[shardKey].has(compositeKey)) {
            shards[shardKey].set(compositeKey, []);
          }
          
          const docIds = shards[shardKey].get(compositeKey);
          if (!docIds.includes(doc.id)) {
            docIds.push(doc.id);
          }
        } else {
          // Single field index (handle arrays)
          const value = doc[this.field];
          const values = Array.isArray(value) ? value : [value];
          
          for (const val of values) {
            const shardKey = this._getShardKey(val);
            const normalizedValue = ValueNormalizer.normalize(val);
            
            if (!shards[shardKey].has(normalizedValue)) {
              shards[shardKey].set(normalizedValue, []);
            }
            
            const docIds = shards[shardKey].get(normalizedValue);
            if (!docIds.includes(doc.id)) {
              docIds.push(doc.id);
            }
          }
        }
      }
      processedDocs++;
    }
    
    // Save all shards
    const savePromises = [];
    for (let i = 0; i < this.shardCount; i++) {
      if (shards[i].size > 0) {
        this.loadedShards.set(i, shards[i]);
        savePromises.push(this._saveShard(i));
      }
    }
    
    await Promise.all(savePromises);
    
    console.timeEnd(`build-sharded-${this.fields.join('_')}`);
    console.log(`✅ Built sharded index: ${processedDocs} docs across ${this.shardCount} shards`);
    
    return this.getStats();
  }
  
  async _getAllKeys() {
    const allKeys = new Map();
    
    for (let shardKey = 0; shardKey < this.shardCount; shardKey++) {
      try {
        const shard = await this._loadShard(shardKey);
        for (const [key, docIds] of shard.entries()) {
          if (allKeys.has(key)) {
            allKeys.get(key).push(...docIds);
          } else {
            allKeys.set(key, [...docIds]);
          }
        }
      } catch (e) {
        // Skip missing shards
      }
    }
    
    return allKeys;
  }

  /*
  async getStats() {
    const stats = {
      field: this.field,
      shardCount: this.shardCount,
      loadedShards: this.loadedShards.size,
      totalMemory: 0
    };
    let totalMemory = 0;
    for (const [shardKey, shard] of this.loadedShards.entries()) {
      const shardObj = Object.fromEntries(shard.entries());
      totalMemory += Buffer.byteLength(JSON.stringify(shardObj));
    }
    stats.totalMemory = totalMemory;
    return stats;
  }
*/
  
  async getStats() {
    const stats = {
      fields: this.fields,
      isComposite: this.isComposite,
      shardCount: this.shardCount,
      loadedShards: this.loadedShards.size,
      totalKeys: 0,
      totalSizeBytes: 0,
      shardDetails: []
    };
    
    for (const [shardKey, shardStats] of this.shardStats.entries()) {
      stats.totalKeys += shardStats.keys;
      stats.totalSizeBytes += shardStats.sizeBytes;
      stats.shardDetails.push({
        shard: shardKey,
        ...shardStats
      });
    }
    
    return stats;
  }
  
  async close() {
    const savePromises = Array.from(this.loadedShards.keys()).map(shardKey => 
      this._saveShard(shardKey)
    );
    await Promise.all(savePromises);
  }
}

export default ShardedIndexManager;


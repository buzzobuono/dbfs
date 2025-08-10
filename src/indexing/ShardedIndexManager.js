import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import SchemaParser from '../core/SchemaParser.js'
import ValueNormalizer from '../utils/ValueNormalizer.js'

class ShardedIndexManager {
  
  constructor(collectionPath, field, options = {}) {
    this.collectionPath = collectionPath;
    this.field = field;
    this.shardCount = options.shardCount || 16;
    this.maxShardSize = options.maxShardSize || 50 * 1024 * 1024;
    this.indicesPath = path.join(collectionPath, '_indices');
    
    this.loadedShards = new Map();
    
  }

  _getShardKey(value) {
    const hash = createHash('md5').update(String(value)).digest('hex');
    return parseInt(hash.substring(0, 2), 16) % this.shardCount;
  }

  _getShardPath(shardKey) {
    return path.join(this.indicesPath, `${this.field}_shard${shardKey}.json`);
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

  async add(value, docId) {
    const shardKey = this._getShardKey(value);
    const shard = await this._loadShard(shardKey);
    const normalizedValue = ValueNormalizer.normalize(value);
    
    if (!shard.has(normalizedValue)) {
      shard.set(normalizedValue, []);
    }
    
    const docIds = shard.get(normalizedValue);
    if (!docIds.includes(docId)) {
      docIds.push(docId);
    }
    
    await this._saveShard(shardKey);
  }

  async remove(value, docId) {
    const shardKey = this._getShardKey(value);
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
        await this._saveShard(shardKey);
      }
    }
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

  async buildFromDocuments(getAllDocuments) {
    console.log(`🏗️ Building sharded index for field: ${this.field}`);
    console.time(`build-sharded-${this.field}`);
    
    this.loadedShards.clear();
    for (let i = 0; i < this.shardCount; i++) {
      const shardPath = this._getShardPath(i);
      if (fs.existsSync(shardPath)) {
        fs.unlinkSync(shardPath);
      }
    }
    
    const shards = [];
    for (let i = 0; i < this.shardCount; i++) {
      shards[i] = new Map();
    }
    
    const docs = await getAllDocuments();
    let processedDocs = 0;
    
    for (const doc of docs) {
      const value = doc[this.field];
      if (value !== undefined && value !== null) {
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
      processedDocs++;
    }
    
    const savePromises = [];
    for (let i = 0; i < this.shardCount; i++) {
      if (shards[i].size > 0) {
        this.loadedShards.set(i, shards[i]);
        savePromises.push(this._saveShard(i));
      }
    }
    
    await Promise.all(savePromises);
    
    console.timeEnd(`build-sharded-${this.field}`);
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

  async close() {
    const savePromises = Array.from(this.loadedShards.keys()).map(shardKey => 
      this._saveShard(shardKey)
    );
    await Promise.all(savePromises);
  }
}

export default ShardedIndexManager;


import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import SchemaParser from './SchemaParser.js'
import DocumentStorage from '../storage/DocumentStorage.js'
import MultiFieldQueryEngine from '../engines/MultiFieldQueryEngine.js'
import LikeQueryEngine from '../engines/LikeQueryEngine.js'
import OrderByEngine from '../engines/OrderByEngine.js'
import RelationEngine from '../engines/RelationEngine.js'
import ShardedIndexManager from '../indexing/ShardedIndexManager.js'
import RegularIndexManager from '../indexing/RegularIndexManager.js'
import PatternMatcher from '../utils/PatternMatcher.js'
import ValueNormalizer from '../utils/ValueNormalizer.js'

class UnifiedCollection {
  
  constructor(basePath, name, schema, db, options = {}) {
    this.collectionPath = path.join(basePath, name);
    this.indicesPath = path.join(this.collectionPath, '_indices');
    this.name = name;
    this.schema = SchemaParser.parseSchema(schema);
    this.db = db;
    this.maxPerDir = schema.maxPerDir || 10000;
    this.options = options;
    
    // Storage
    this.storage = new DocumentStorage(this.collectionPath, this.maxPerDir);
    
    // Index management
    this.loadedIndices = new Map();
    this.shardedIndices = new Map();
    this.useSharding = schema.useSharding !== false;
    
    // Query engines
    this.multiFieldEngine = new MultiFieldQueryEngine(this);
    this.likeEngine = new LikeQueryEngine(this);
    this.orderByEngine = new OrderByEngine(this);
    this.relationEngine = new RelationEngine(this);
  }
  
  async initialize() {
    this._initializeCollection();
    
    // Conditional index building
    if (!this.options.skipInitialIndexBuild) {
      await this._buildSchemaIndices();
    } else {
      await this._loadExistingIndices();
    }
  }

  async _loadExistingIndices() {
    console.log(`📂 Loading existing indices for collection: ${this.name}`);
    
    if (!fs.existsSync(this.indicesPath)) {
      console.log(`⚠️ No indices directory found for ${this.name}`);
      return;
    }
    
    const indexedFields = SchemaParser.getIndexedFields(this.schema);
    const indexFiles = fs.readdirSync(this.indicesPath).filter(f => f.endsWith('.json'));
    
    for (const indexFile of indexFiles) {
      const fieldName = indexFile.replace('.json', '').replace(/_shard\d+/, '');
      
      if (indexedFields.includes(fieldName)) {
        await this._loadIndex(fieldName);
        console.log(`✅ Loaded index for field: ${fieldName}`);
      }
    }
    
    console.log(`📋 Loaded ${this.loadedIndices.size + this.shardedIndices.size} indices for ${this.name}`);
  }
  
  async rebuildAllIndices() {
    console.log(`🔄 Rebuilding all indices for collection: ${this.name}`);
    
    // Clear existing indices
    this.loadedIndices.clear();
    this.shardedIndices.clear();
    
    // Remove index files
    if (fs.existsSync(this.indicesPath)) {
      const indexFiles = fs.readdirSync(this.indicesPath);
      for (const file of indexFiles) {
        fs.unlinkSync(path.join(this.indicesPath, file));
      }
    }
    
    // Rebuild from scratch
    await this._buildSchemaIndices();
    
    console.log('✅ All indices rebuilt');
  }
  
  async checkIndicesHealth() {
    const health = {
      collection: this.name,
      expectedIndices: SchemaParser.getIndexedFields(this.schema),
      existingIndices: [],
      missingIndices: [],
      corruptedIndices: [],
      upToDate: true
    };
    
    for (const field of health.expectedIndices) {
      const indexPath = path.join(this.indicesPath, `${field}.json`);
      
      if (fs.existsSync(indexPath)) {
        try {
          // Try to load index to check if corrupted
          const indexData = JSON.parse(fs.readFileSync(indexPath, 'utf8'));
          health.existingIndices.push(field);
        } catch (e) {
          health.corruptedIndices.push(field);
          health.upToDate = false;
        }
      } else {
        health.missingIndices.push(field);
        health.upToDate = false;
      }
    }
    
    return health;
  }
  
  _initializeCollection() {
    if (!fs.existsSync(this.collectionPath)) {
      fs.mkdirSync(this.collectionPath, { recursive: true });
    }
    if (!fs.existsSync(this.indicesPath)) {
      fs.mkdirSync(this.indicesPath, { recursive: true });
    }
  }

  async _buildSchemaIndices() {
    const indexedFields = SchemaParser.getIndexedFields(this.schema);
    
    if (indexedFields.length === 0) return;
    
    const indexPromises = indexedFields.map(field => {
      const indexPath = path.join(this.indicesPath, `${field}.json`);
      if (!fs.existsSync(indexPath)) {
        return this._buildIndex(field);
      } else {
        return this._loadIndex(field);
      }
    });
    
    await Promise.all(indexPromises);
  }
  
  async _buildIndex(field) {
    if (this.useSharding) {
      const shardedIndex = new ShardedIndexManager(this.collectionPath, field);
      const stats = await shardedIndex.buildFromDocuments(() => this.storage.getAllDocuments());
      this.shardedIndices.set(field, shardedIndex);
      return stats;
    } else {
      const indexManager = new RegularIndexManager(this.collectionPath, field);
      const stats = await indexManager.buildIndex(() => this.storage.getAllDocuments());
      this.loadedIndices.set(field, indexManager.index);
      return stats;
    }
  }

  async _saveRegularIndex(field, index) {
    const indexPath = path.join(this.indicesPath, `${field}.json`);
    const indexObj = Object.fromEntries(index.entries());
    
    const tempPath = `${indexPath}.tmp`;
    fs.writeFileSync(tempPath, JSON.stringify(indexObj, null, 2));
    fs.renameSync(tempPath, indexPath);
  }
  
  async _loadIndex(field) {
    if (this.useSharding) {
      const shardedIndex = new ShardedIndexManager(this.collectionPath, field);
      this.shardedIndices.set(field, shardedIndex);
    } else {
      const indexManager = new RegularIndexManager(this.collectionPath, field);
      await indexManager.load();
      this.loadedIndices.set(field, indexManager.index);
    }
  }

  async loadDocumentsBatch(docIds, batchSize = 20) {
    if (docIds.length === 0) return [];
    
    // Raggruppa per shard per ottimizzare I/O
  
    const shardGroups = this._groupDocumentsByShards(docIds);
    
    const results = new Map();
    const loadPromises = [];
    
    // Processa ogni shard in parallelo
    
    for (const [shardPath, idsInShard] of shardGroups.entries()) {
      // Suddividi in batch
      for (let i = 0; i < idsInShard.length; i += batchSize) {
        const batch = idsInShard.slice(i, i + batchSize);
        loadPromises.push(this._loadShardBatch(shardPath, batch, results));
      }
    }
    
    await Promise.all(loadPromises);
    
    // Restituisci nell'ordine originale
    return docIds.map(id => results.get(id)).filter(doc => doc !== null);
  }
  
  _groupDocumentsByShards(docIds) {
    const groups = new Map();
    
    for (const id of docIds) {
      const shardPath = this.storage.getDocumentPath(id);
      const shardDir = path.dirname(shardPath);
      
      if (!groups.has(shardDir)) {
        groups.set(shardDir, []);
      }
      groups.get(shardDir).push({ id, path: shardPath });
      
    }
    
    return groups;
  }
  
  async _loadShardBatch(shardPath, batch, resultsMap) {
    const loadPromises = batch.map(async ({ id, path }) => {
      try {
        if (fs.existsSync(path)) {
          const doc = JSON.parse(fs.readFileSync(path, 'utf8'));
          resultsMap.set(id, doc);
        } else {
          resultsMap.set(id, null);
        }
      } catch (e) {
        console.warn(`⚠️ Error loading document ${id}:`, e.message);
        resultsMap.set(id, null);
      }
    });
    
    await Promise.all(loadPromises);
  }
  
  // ---------- CORE CRUD ----------

  async insert(doc) {
    const id = Date.now() + '-' + Math.random().toString(36).slice(2);
    const docWithId = { id, ...doc };
    
    // Schema validation
    const validationErrors = SchemaParser.validateDocument(docWithId, this.schema);
    if (validationErrors.length > 0) {
      throw new Error(`Schema validation failed: ${validationErrors.join(', ')}`);
    }
    
    // Relation validation
    if (this.schema.validateRelations) {
      const relationErrors = await SchemaParser.validateRelations(docWithId, this.schema, this.db);
      if (relationErrors.length > 0) {
        throw new Error(`Relation validation failed: ${relationErrors.join(', ')}`);
      }
    }
    
    // Save document
    await this.storage.saveDocument(id, docWithId);
    
    // Update indices
    await this._updateAllIndices(docWithId, 'insert');
    
    return docWithId;
  }

  async getById(id) {
    return await this.storage.loadDocument(id);
  }

  async update(id, changes) {
    const existingDoc = await this.getById(id);
    if (!existingDoc) {
      throw new Error(`Document with id ${id} not found`);
    }
    
    const updatedDoc = { ...existingDoc, ...changes };
    
    // Schema validation
    const validationErrors = SchemaParser.validateDocument(updatedDoc, this.schema);
    if (validationErrors.length > 0) {
      throw new Error(`Schema validation failed: ${validationErrors.join(', ')}`);
    }
    
    // Save document
    await this.storage.saveDocument(id, updatedDoc);
    
    // Update indices
    await this._updateAllIndices(updatedDoc, 'update', existingDoc);
    
    return updatedDoc;
  }

  async delete(id) {
    const existingDoc = await this.getById(id);
    if (!existingDoc) return false;
    
    // Delete document
    await this.storage.deleteDocument(id);
    
    // Update indices
    await this._updateAllIndices(existingDoc, 'delete');
    
    return true;
  }

  async _updateAllIndices(doc, operation, oldDoc = null) {
    const indexedFields = SchemaParser.getIndexedFields(this.schema);
    
    for (const field of indexedFields) {
      if (this.shardedIndices.has(field)) {
        const shardedIndex = this.shardedIndices.get(field);
        
        if (operation === 'insert') {
          const value = doc[field];
          if (value !== undefined && value !== null) {
            await shardedIndex.add(value, doc.id);
          }
        } else if (operation === 'update') {
          const oldValue = oldDoc[field];
          const newValue = doc[field];
          
          if (oldValue !== undefined && oldValue !== null) {
            await shardedIndex.remove(oldValue, doc.id);
          }
          if (newValue !== undefined && newValue !== null) {
            await shardedIndex.add(newValue, doc.id);
          }
        } else if (operation === 'delete') {
          const value = doc[field];
          if (value !== undefined && value !== null) {
            await shardedIndex.remove(value, doc.id);
          }
        }
      } else if (this.loadedIndices.has(field)) {
        const index = this.loadedIndices.get(field);
        const normalizedValue = ValueNormalizer.normalize(doc[field]);
        
        if (operation === 'insert') {
          if (!index.has(normalizedValue)) index.set(normalizedValue, []);
          index.get(normalizedValue).push(doc.id);
        } else if (operation === 'delete') {
          if (index.has(normalizedValue)) {
            const ids = index.get(normalizedValue);
            const idx = ids.indexOf(doc.id);
            if (idx !== -1) ids.splice(idx, 1);
            if (ids.length === 0) index.delete(normalizedValue);
          }
        }
        await this._saveRegularIndex(field, index);
      }
    }
  }

  // ---------- BASIC QUERIES ----------

  async findByField(field, value, options = {}) {
    const { limit = 1000, offset = 0 } = options;
    const indexedFields = SchemaParser.getIndexedFields(this.schema);
    
    if (indexedFields.includes(field)) {
      return await this._indexScan(field, value, limit, offset);
    } else {
      return await this._tableScan(field, value, limit, offset);
    }
  }

  async _indexScan(field, value, limit, offset) {
    if (this.shardedIndices.has(field)) {
      const shardedIndex = this.shardedIndices.get(field);
      const ids = await shardedIndex.get(value);
      const paginatedIds = ids.slice(offset, offset + limit);
      return await this._loadDocuments(paginatedIds);
    } else if (this.loadedIndices.has(field)) {
      const index = this.loadedIndices.get(field);
      const key = ValueNormalizer.normalize(value);
      const ids = index.get(key) || [];
      const paginatedIds = ids.slice(offset, offset + limit);
      return await this._loadDocuments(paginatedIds);
    }
    return [];
  }

  async _tableScan(field, value, limit, offset) {
    const results = [];
    let found = 0;
    let skipped = 0;
    
    const allDocs = await this.storage.getAllDocuments();
    
    for (const doc of allDocs) {
      if (this._documentMatches(doc, field, value)) {
        if (skipped < offset) {
          skipped++;
          continue;
        }
        results.push(doc);
        found++;
        if (results.length >= limit) break;
      }
    }
    
    return results;
  }

  async findByRange(field, min, max, options = {}) {
    const { limit = 1000 } = options;
    const indexedFields = SchemaParser.getIndexedFields(this.schema);
    
    if (!indexedFields.includes(field)) {
      throw new Error(`Range query requires indexed field. Add ${field} to schema.fields with indexed: true`);
    }
    
    const matchingIds = [];
    
    if (this.shardedIndices.has(field)) {
      const shardedIndex = this.shardedIndices.get(field);
      // Range queries on sharded indices are expensive - need to check all shards
      const allKeys = await shardedIndex._getAllKeys();
      for (const [key, ids] of allKeys.entries()) {
        const numValue = parseFloat(key);
        if (!isNaN(numValue) && numValue >= min && numValue <= max) {
          matchingIds.push(...ids);
        }
      }
    } else if (this.loadedIndices.has(field)) {
      const index = this.loadedIndices.get(field);
      for (const [key, ids] of index.entries()) {
        const numValue = parseFloat(key);
        if (!isNaN(numValue) && numValue >= min && numValue <= max) {
          matchingIds.push(...ids);
        }
      }
    }
    
    const limitedIds = matchingIds.slice(0, limit);
    return await this._loadDocuments(limitedIds);
  }

  // ---------- MULTI-FIELD QUERIES (Delegated) ----------

  async findByAnd(conditions, options = {}) {
    return await this.multiFieldEngine.findByAnd(conditions, options);
  }

  async findByOr(conditions, options = {}) {
    return await this.multiFieldEngine.findByOr(conditions, options);
  }

  async findByComplex(query, options = {}) {
    return await this.multiFieldEngine.findByComplex(query, options);
  }

  // ---------- LIKE QUERIES (Delegated) ----------

  async findByLike(field, pattern, options = {}) {
    return await this.likeEngine.findByLike(field, pattern, options);
  }

  async findByLikes(conditions, operator = 'AND', options = {}) {
    return await this.likeEngine.findByLikes(conditions, operator, options);
  }

  async findByFullText(field, searchTerms, options = {}) {
    return await this.likeEngine.findByFullText(field, searchTerms, options);
  }

  // ---------- ORDER BY QUERIES (Delegated) ----------

  async findWithOrderBy(query = {}, orderBy, options = {}) {
    return await this.orderByEngine.findWithOrderBy(query, orderBy, options);
  }

  async findAllOrderedBy(orderBy, options = {}) {
    return await this.orderByEngine.findWithOrderBy({}, orderBy, options);
  }

  // ---------- ADVANCED COMBINED QUERIES ----------

  async findAdvanced(options = {}) {
    const {
      where = {},
      whereLike = {},
      whereComplex = null,
      orderBy = null,
      limit = 1000,
      offset = 0,
      populate = []
    } = options;
    
    let results = [];
    
    // Execute primary query
    if (whereComplex) {
      results = await this.findByComplex(whereComplex, { limit: limit * 2 });
    } else if (Object.keys(where).length > 1) {
      results = await this.findByAnd(where, { limit: limit * 2 });
    } else if (Object.keys(where).length === 1) {
      const field = Object.keys(where)[0];
      const value = where[field];
      results = await this.findByField(field, value, { limit: limit * 2 });
    } else {
      results = await this.storage.getAllDocuments();
    }
    
    // Apply LIKE filters
    if (Object.keys(whereLike).length > 0) {
      for (const [field, pattern] of Object.entries(whereLike)) {
        const patternInfo = PatternMatcher.analyzePattern(pattern);
        results = results.filter(doc => 
          PatternMatcher.documentMatches(doc, field, patternInfo)
        );
      }
    }
    
    // Apply ORDER BY
    if (orderBy) {
      const orderFields = this.orderByEngine._parseOrderBy(orderBy);
      results = this.orderByEngine._applySortInMemory(results, orderFields);
    }
    
    // Apply pagination
    const paginated = results.slice(offset, offset + limit);
    
    // Apply population
    if (populate.length > 0) {
      return await this.relationEngine.populate(paginated, ...populate);
    }
    
    return paginated;
  }

  // ---------- RELATIONS (Delegated) ----------

  async populate(docs, ...relationNames) {
    return await this.relationEngine.populate(docs, ...relationNames);
  }

  async join(targetCollectionName, localField, options = {}) {
    return await this.relationEngine.join(targetCollectionName, localField, options);
  }

  // ---------- UTILITY METHODS ----------

  async _loadDocuments(docIds) {
    const loadPromises = docIds.map(id => this.storage.loadDocument(id));
    const docs = await Promise.all(loadPromises);
    return docs.filter(doc => doc !== null);
  }

  async _loadDocuments_(docIds) {
    if (docIds.length === 0) return [];
    
    // Usa batch loading per performance migliori
    return await this.loadDocumentsBatch(docIds, 20);
  }
  
  _documentMatches(doc, field, value) {
    const docValue = doc[field];
    if (docValue === undefined || docValue === null) return false;
    
    if (Array.isArray(docValue)) {
      return docValue.some(item => 
        ValueNormalizer.normalize(item) === ValueNormalizer.normalize(value)
      );
    }
    
    return ValueNormalizer.normalize(docValue) === ValueNormalizer.normalize(value);
  }

  getSchema() {
    return {
      fields: this.schema.fields,
      relations: this.schema.relations,
      indexedFields: SchemaParser.getIndexedFields(this.schema),
      validateRelations: this.schema.validateRelations,
      useSharding: this.useSharding
    };
  }

  async getStats() {
    const stats = {
      collection: this.name,
      schema: this.getSchema(),
      indices: { regular: {}, sharded: [] }
    };
    
    for (const [field, index] of this.loadedIndices.entries()) {
      stats.indices.regular[field] = {
        uniqueValues: index.size,
        memoryMB: Math.round(JSON.stringify(Object.fromEntries(index)).length / 1024 / 1024 * 100) / 100
      };
    }
    
    for (const [field, shardedIndex] of this.shardedIndices.entries()) {
      const shardIndexStats = await shardedIndex.getStats();
      stats.indices.sharded.push(shardIndexStats);
    }
    
    return stats;
  }

  async optimize() {
    const indexedFields = SchemaParser.getIndexedFields(this.schema);
    const rebuildPromises = indexedFields.map(field => this._buildIndex(field));
    await Promise.all(rebuildPromises);
  }

  async close() {
    // Flush pending operations
    for (const shardedIndex of this.shardedIndices.values()) {
      await shardedIndex.close();
    }
  }
}

export default UnifiedCollection;


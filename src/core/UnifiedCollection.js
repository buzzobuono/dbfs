import fs from 'fs';
import path from 'path';
import SchemaParser from './SchemaParser.js'
import DocumentStorage from '../storage/DocumentStorage.js'
import MultiFieldQueryEngine from '../engines/MultiFieldQueryEngine.js'
import LikeQueryEngine from '../engines/LikeQueryEngine.js'
import OrderByEngine from '../engines/OrderByEngine.js'
import RelationEngine from '../engines/RelationEngine.js'
import ShardedIndexManager from '../indexing/ShardedIndexManager.js'
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
    this.shardedIndices = new Map();
    
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
      await this.buildAllIndices();
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
    
    //const indexedFields = SchemaParser.getIndexedFields(this.schema);
    //const indexFiles = fs.readdirSync(this.indicesPath).filter(f => f.endsWith('.json'));
    //
    //for (const indexFile of indexFiles) {
    //  const fieldName = indexFile.replace('.json', '').replace(/_shard\d+/, '');
    //  
    //  if (indexedFields.includes(fieldName)) {
    //    await this._loadIndex(fieldName);
    //    console.log(`✅ Loaded index for field: ${fieldName}`);
    //  }
    //}
    
    const indices = this.schema.indices;
    const indexFiles = fs.readdirSync(this.indicesPath).filter(f => f.endsWith('.json'));
    
    for (const indexFile of indexFiles) {
      const indexName = indexFile.replace('.json', '').replace(/_shard\d+/, '');
      
      if (indices.hasOwnProperty(indexName)) {
        await this._loadIndex(indexName, this.schema.indices[indexName]);
        console.log(`✅ Loaded index: ${indexName}`);
      }
    }
    console.log(`📋 Loaded ${this.shardedIndices.size} indices for ${this.name}`);
  }
  
  async rebuildAllIndices() {
    console.log(`🔄 Rebuilding all indices for collection: ${this.name}`);
    
    // Clear existing indices
    this.shardedIndices.clear();
    
    // Remove index files
    if (fs.existsSync(this.indicesPath)) {
      const indexFiles = fs.readdirSync(this.indicesPath);
      for (const file of indexFiles) {
        fs.unlinkSync(path.join(this.indicesPath, file));
      }
    }
    
    // Rebuild from scratch
    await this.buildAllIndices();
    
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

  async buildAllIndices() {
/*    const indexedFields = SchemaParser.getIndexedFields(this.schema);
    
    const indexPromises = indexedFields.map(field => {
      const indexPath = path.join(this.indicesPath, `${field}.json`);
      if (!fs.existsSync(indexPath)) {
        return this._buildIndex(field);
      } else {
        return this._loadIndex(field);
      }
    });
    
    await Promise.all(indexPromises);
    
*/  
    // Build composite indices
    if (this.schema.indices) {
      for (const [indexName, fields] of Object.entries(this.schema.indices)) {
        console.log(`🔨 Building composite sharded index: ${indexName}`);
        
        const compositeIndex = new ShardedIndexManager(
          this.collectionPath, 
          fields, // ← Array of fields for composite
          { shardCount: 16 }
        );
        
        await compositeIndex.buildFromDocuments(() => this.storage.getAllDocuments());
        this.shardedIndices.set(indexName, compositeIndex);
      }
    }
  }
  
  async _buildIndex(field) {
    const shardedIndex = new ShardedIndexManager(this.collectionPath, field);
    const stats = await shardedIndex.buildFromDocuments(() => this.storage.getAllDocuments());
    this.shardedIndices.set(field, shardedIndex);
    return stats;
  }
  
  async _loadIndex(name, fields) {
    const shardedIndex = new ShardedIndexManager(this.collectionPath, fields);
    this.shardedIndices.set(name, shardedIndex);
  }
  
  // ---------- CORE CRUD ----------

  async insert(doc, options = {}) {
    const { updateIndices = true } = options;
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
    if (options.updateIndices) {
      await this._updateAllIndices(docWithId, 'insert');
    }
    
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
      }
    }
  }

  // ---------- BASIC QUERIES ----------
  
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

  async find(query) {
    const {
      where = null,
      like = {},
      filter = {},
      orderBy = null,
      limit = 1000,
      offset = 0,
      populate = []
    } = query;
    
    let response = {};
    
    let results = [];
    
    // Execute primary query
    if (where) {
      results = await this.findByComplex(where);
    } else {
      results = await this.storage.getAllDocuments();
    }
    
    // Apply filters
    if (Object.keys(filter).length > 0) {
      for (const [field, value] of Object.entries(filter)) {
        results = results.filter(doc => 
          this._documentMatches(doc, field, value)
        );
      }
    }
    
    // Apply LIKE filters
    if (Object.keys(like).length > 0) {
      for (const [field, pattern] of Object.entries(like)) {
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
    
    response['size'] = results.length;
    response['limit'] = limit;
    response['offset'] = offset;
    
    // Apply pagination
    results = results.slice(offset, offset + limit);
    
    // Apply population
    if (populate.length > 0) {
      return await this.relationEngine.populate(results, ...populate);
    }
    
    response['results'] = results;
    
    return response;
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
    };
  }

  async getStats() {
    const stats = {
      collection: this.name,
      schema: this.getSchema(),
      indices: []
    };
    
    console.log(this.shardedIndices);
    for (const [field, shardedIndex] of this.shardedIndices.entries()) {
      
      const shardIndexStats = await shardedIndex.getStats();
      stats.indices.push(shardIndexStats);
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


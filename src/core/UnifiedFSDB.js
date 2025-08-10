import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import UnifiedCollection from './UnifiedCollection.js'

class UnifiedFSDB {
  constructor(basePath, options = {}) {
    this.basePath = basePath;
    this.collections = new Map();
    this.options = options;
    this.metadataFile = path.join(basePath, '_db_metadata.json');
    
    // Don't auto-create directory in constructor
    this.isInitialized = false;
  }

  // ---------- CREATION API ----------

  static async create(basePath, options = {}) {
    console.log(`🏗️ Creating new database at: ${basePath}`);
    
    if (fs.existsSync(basePath)) {
      const files = fs.readdirSync(basePath);
      if (files.length > 0) {
        throw new Error(`Directory ${basePath} already exists and is not empty. Use open() instead.`);
      }
    }
    
    const db = new UnifiedFSDB(basePath, options);
    await db._initialize(true); // true = creating new
    return db;
  }

  // ---------- OPENING API ----------

  static async open(basePath, options = {}) {
    console.log(`📂 Opening existing database at: ${basePath}`);
    
    if (!fs.existsSync(basePath)) {
      throw new Error(`Database directory ${basePath} does not exist. Use create() instead.`);
    }
    
    const db = new UnifiedFSDB(basePath, options);
    await db._initialize(false); // false = opening existing
    return db;
  }

  // ---------- INTERNAL INITIALIZATION ----------

  async _initialize(isCreating) {
    if (this.isInitialized) return;
    
    if (isCreating) {
      // Create directory structure
      if (!fs.existsSync(this.basePath)) {
        fs.mkdirSync(this.basePath, { recursive: true });
      }
      
      // Create database metadata
      const metadata = {
        version: '1.0.0',
        created: new Date().toISOString(),
        collections: {}
      };
      fs.writeFileSync(this.metadataFile, JSON.stringify(metadata, null, 2));
      console.log('✅ Database structure created');
      
    } else {
      // Validate existing database
      if (!fs.existsSync(this.metadataFile)) {
        throw new Error(`Invalid database: ${this.metadataFile} not found`);
      }
      
      // Load database metadata
      const metadata = JSON.parse(fs.readFileSync(this.metadataFile, 'utf8'));
      console.log(`✅ Database opened (version: ${metadata.version})`);
      
      // Auto-discover existing collections
      await this._discoverCollections(metadata);
    }
    
    this.isInitialized = true;
  }

  async _discoverCollections(metadata) {
    console.log('🔍 Auto-discovering collections...');
    
    for (const [collectionName, collectionMetadata] of Object.entries(metadata.collections)) {
      console.log(`📁 Found collection: ${collectionName}`);
      
      // Create collection instance without rebuilding indices
      const collection = new UnifiedCollection(
        this.basePath,
        collectionName,
        collectionMetadata.schema,
        this,
        { skipInitialIndexBuild: true } // ← Key parameter!
      );
      
      await collection.initialize();
      
      this.collections.set(collectionName, collection);
    }
    
    console.log(`✅ Discovered ${this.collections.size} collections`);
  }

  // ---------- COLLECTION MANAGEMENT ----------

  async collection(name, schema = null) {
    if (!this.isInitialized) {
      throw new Error('Database not initialized. Use UnifiedFSDB.create() or UnifiedFSDB.open()');
    }
    
    if (this.collections.has(name)) {
      // Return existing collection
      return this.collections.get(name);
    }
    
    if (schema === null) {
      throw new Error(`Collection '${name}' does not exist and no schema provided. Provide schema to create new collection.`);
    }
    
    // Create new collection
    console.log(`➕ Creating new collection: ${name}`);
    
    const collection = new UnifiedCollection(
      this.basePath,
      name,
      schema,
      this,
      { skipInitialIndexBuild: false } // Build indices for new collection
    );
    
    await collection.initialize();
    
    this.collections.set(name, collection);
    
    // Update database metadata
    this._updateMetadata(name, schema);
    
    return collection;
  }

  _updateMetadata(collectionName, schema) {
    const metadata = JSON.parse(fs.readFileSync(this.metadataFile, 'utf8'));
    metadata.collections[collectionName] = {
      schema: schema,
      created: new Date().toISOString()
    };
    fs.writeFileSync(this.metadataFile, JSON.stringify(metadata, null, 2));
  }

  // ---------- DATABASE OPERATIONS ----------

  listCollections() {
    return Array.from(this.collections.keys());
  }

  async dropCollection(name) {
    if (!this.collections.has(name)) {
      throw new Error(`Collection '${name}' does not exist`);
    }
    
    console.log(`🗑️ Dropping collection: ${name}`);
    
    // Close collection
    const collection = this.collections.get(name);
    await collection.close();
    
    // Remove collection directory
    const collectionPath = path.join(this.basePath, name);
    if (fs.existsSync(collectionPath)) {
      fs.rmSync(collectionPath, { recursive: true, force: true });
    }
    
    // Remove from memory
    this.collections.delete(name);
    
    // Update metadata
    const metadata = JSON.parse(fs.readFileSync(this.metadataFile, 'utf8'));
    delete metadata.collections[name];
    fs.writeFileSync(this.metadataFile, JSON.stringify(metadata, null, 2));
    
    console.log(`✅ Collection '${name}' dropped`);
  }

  async backup(backupPath) {
    console.log(`💾 Creating backup to: ${backupPath}`);
    
    // Close all collections
    await this.close();
    
    // Copy entire database directory
    fs.cpSync(this.basePath, backupPath, { recursive: true });
    
    // Reopen database
    await this._initialize(false);
    
    console.log('✅ Backup completed');
  }

  async close() {
    const closePromises = Array.from(this.collections.values()).map(coll => coll.close());
    await Promise.all(closePromises);
  }

  async optimize() {
    const optimizePromises = Array.from(this.collections.values()).map(coll => coll.optimize());
    await Promise.all(optimizePromises);
  }
}

export default UnifiedFSDB;
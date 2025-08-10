import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import SchemaParser from '../core/SchemaParser.js'
import ValueNormalizer from '../utils/ValueNormalizer.js'

class RegularIndexManager {
  constructor(collectionPath, field) {
    this.collectionPath = collectionPath;
    this.field = field;
    this.indexPath = path.join(collectionPath, '_indices', `${field}.json`);
    this.index = new Map();
  }

  async buildIndex(getAllDocuments) {
    console.log(`🔨 Building regular index for field: ${this.field}`);
    console.time(`build-index-${this.field}`);
    
    this.index.clear();
    const docs = await getAllDocuments();
    let processedDocs = 0;
    
    for (const doc of docs) {
      const value = doc[this.field];
      if (value !== undefined && value !== null) {
        if (Array.isArray(value)) {
          for (const item of value) {
            const key = ValueNormalizer.normalize(item);
            if (!this.index.has(key)) this.index.set(key, []);
            if (!this.index.get(key).includes(doc.id)) {
              this.index.get(key).push(doc.id);
            }
          }
        } else {
          const key = ValueNormalizer.normalize(value);
          if (!this.index.has(key)) this.index.set(key, []);
          this.index.get(key).push(doc.id);
        }
      }
      processedDocs++;
    }
    
    await this.save();
    
    console.log(`✅ Index built for ${this.field}: ${this.index.size} unique values, ${processedDocs} documents`);
    console.timeEnd(`build-index-${this.field}`);
    
    return { field: this.field, uniqueValues: this.index.size, totalDocuments: processedDocs };
  }

  async save() {
    const indexObj = Object.fromEntries(this.index.entries());
    const tempPath = `${this.indexPath}.tmp`;
    
    try {
      fs.writeFileSync(tempPath, JSON.stringify(indexObj, null, 2));
      fs.renameSync(tempPath, this.indexPath);
    } catch (error) {
      if (fs.existsSync(tempPath)) {
        fs.unlinkSync(tempPath);
      }
      throw error;
    }
  }

  async load() {
    if (fs.existsSync(this.indexPath)) {
      try {
        const indexObj = JSON.parse(fs.readFileSync(this.indexPath, 'utf8'));
        this.index = new Map(Object.entries(indexObj));
      } catch (e) {
        console.warn(`⚠️ Corrupted index for ${this.field}, will rebuild`);
        this.index = new Map();
      }
    }
  }
}

export default RegularIndexManager;

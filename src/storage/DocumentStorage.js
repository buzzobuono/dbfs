import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';

class DocumentStorage {
  constructor(collectionPath, maxPerDir = 10000) {
    this.collectionPath = collectionPath;
    this.maxPerDir = maxPerDir;
  }

  getDocumentPath(id) {
    const shardNum = this.hashShard(id);
    const shardName = String(shardNum).padStart(3, '0');
    const shardPath = path.join(this.collectionPath, shardName);
    
    if (!fs.existsSync(shardPath)) {
      fs.mkdirSync(shardPath, { recursive: true });
    }
    
    const files = fs.readdirSync(shardPath).filter(f => f.endsWith('.json')).length;
    const subShard = Math.floor(files / this.maxPerDir);
    const subShardPath = path.join(shardPath, String(subShard).padStart(3, '0'));
    
    if (!fs.existsSync(subShardPath)) {
      fs.mkdirSync(subShardPath, { recursive: true });
    }
    
    return path.join(subShardPath, `${id}.json`);
  }

  hashShard(id, shardCount = 256) {
    const hash = createHash('md5').update(id).digest('hex');
    return parseInt(hash.substring(0, 2), 16) % shardCount;
  }

  async saveDocument(id, doc) {
    const filePath = this.getDocumentPath(id);
    fs.writeFileSync(filePath, JSON.stringify(doc, null, 2));
  }

  async loadDocument(id) {
    const filePath = this.getDocumentPath(id);
    if (fs.existsSync(filePath)) {
      try {
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
      } catch (e) {
        console.warn(`⚠️ Corrupted document: ${id}`);
        return null;
      }
    }
    return null;
  }

  async deleteDocument(id) {
    const filePath = this.getDocumentPath(id);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      return true;
    }
    return false;
  }

  async getAllDocuments() {
    const docs = [];
    
    for (let shard = 0; shard < 256; shard++) {
      const shardName = String(shard).padStart(3, '0');
      const shardPath = path.join(this.collectionPath, shardName);
      
      if (fs.existsSync(shardPath)) {
        const shardDocs = await this.getShardDocs(shardPath);
        docs.push(...shardDocs);
      }
    }
    
    return docs;
  }

  async getShardDocs(shardPath) {
    const docs = [];
    const subShards = fs.readdirSync(shardPath).filter(f => 
      fs.statSync(path.join(shardPath, f)).isDirectory()
    );
    
    for (const subShard of subShards) {
      const subShardPath = path.join(shardPath, subShard);
      const files = fs.readdirSync(subShardPath).filter(f => f.endsWith('.json'));
      
      for (const file of files) {
        try {
          const doc = JSON.parse(fs.readFileSync(path.join(subShardPath, file), 'utf8'));
          docs.push(doc);
        } catch (e) {
          console.warn(`⚠️ Corrupted document: ${file}`);
        }
      }
    }
    
    return docs;
  }
  
  async countDocuments() {
    let count = 0;
    
    for (let shard = 0; shard < 256; shard++) {
      const shardName = String(shard).padStart(3, '0');
      const shardPath = path.join(this.collectionPath, shardName);
      
      if (fs.existsSync(shardPath)) {
        const subShards = fs.readdirSync(shardPath).filter(f =>
          fs.statSync(path.join(shardPath, f)).isDirectory()
        );
        
        for (const subShard of subShards) {
          const subShardPath = path.join(shardPath, subShard);
          const files = fs.readdirSync(subShardPath).filter(f => f.endsWith('.json'));
          count += files.length;
        }
      }
    }
    
    return count;
  }
  
}

export default DocumentStorage;



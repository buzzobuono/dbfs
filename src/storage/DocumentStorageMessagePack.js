import fs from 'fs';
import path from 'path';
import { createHash } from 'crypto';
import { encode, decode } from '@msgpack/msgpack';

class DocumentStorage {
  constructor(collectionPath, subShardCount = 16) {
    this.collectionPath = collectionPath;
    this.subShardCount = subShardCount;
  }

  getDocumentPath(id) {
    const shardNum = this.hashShard(id);
    const shardName = String(shardNum).padStart(3, '0');
    const shardPath = path.join(this.collectionPath, shardName);
    if (!fs.existsSync(shardPath)) fs.mkdirSync(shardPath, { recursive: true });

    const subShardNum = this.hashSubShard(id);
    const subShardName = String(subShardNum).padStart(3, '0');
    const subShardPath = path.join(shardPath, subShardName);
    if (!fs.existsSync(subShardPath)) fs.mkdirSync(subShardPath, { recursive: true });

    return path.join(subShardPath, `${id}.json`);
  }

  hashShard(id, shardCount = 256) {
    const hash = createHash('md5').update(id).digest('hex');
    return parseInt(hash.substring(0, 2), 16) % shardCount;
  }

  hashSubShard(id) {
    const hash = createHash('md5').update(id).digest('hex');
    return parseInt(hash.substring(2, 4), 16) % this.subShardCount;
  }

  async saveDocument(id, doc) {
    const filePath = this.getDocumentPath(id);
    const bin = encode(doc);
    this.safeWriteFileSync(filePath, Buffer.from(bin));
  }

  safeWriteFileSync(filePath, buffer) {
    const dir = path.dirname(filePath);
    const tmp = path.join(dir, `.${path.basename(filePath)}.tmp-${process.pid}-${Date.now()}`);
    fs.writeFileSync(tmp, buffer);
    fs.renameSync(tmp, filePath);
  }

  async loadDocument(id) {
    const filePath = this.getDocumentPath(id);
    if (fs.existsSync(filePath)) {
      try {
        const buf = fs.readFileSync(filePath);
        return decode(buf);
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
      if (!fs.existsSync(shardPath)) continue;

      const subShards = fs.readdirSync(shardPath).filter((f) =>
        fs.statSync(path.join(shardPath, f)).isDirectory()
      );

      for (const sub of subShards) {
        const subPath = path.join(shardPath, sub);
        const files = fs
          .readdirSync(subPath)
          .filter((f) => f.endsWith(this.ext));

        for (const file of files) {
          const full = path.join(subPath, file);
          try {
            const buf = fs.readFileSync(full);
            const doc = decode(buf);
            docs.push(doc);
          } catch (e) {
            console.warn(`\u26a0\ufe0f Corrupted document: ${file}`);
          }
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
      if (!fs.existsSync(shardPath)) continue;

      const subShards = fs.readdirSync(shardPath).filter((f) =>
        fs.statSync(path.join(shardPath, f)).isDirectory()
      );

      for (const sub of subShards) {
        const subPath = path.join(shardPath, sub);
        const files = fs
          .readdirSync(subPath)
          .filter((f) => f.endsWith(this.ext));
        count += files.length;
      }
    }
    return count;
  }

}

export default DocumentStorage;



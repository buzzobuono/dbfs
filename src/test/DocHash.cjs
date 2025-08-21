const fs = require('fs');
const crypto = require('crypto');

class HashDocumentStorage {
    constructor(filePath, buckets = 1000000) {
        this.filePath = filePath;
        this.buckets = buckets;
        this.bucketSize = 12; // offset(8) + size(4) bytes per bucket
        this.headerSize = 16; // magic(4) + buckets(4) + dataStart(8)
        this.hashTableSize = this.buckets * this.bucketSize;
        
        this.initFile();
        this.fd = fs.openSync(filePath, 'r+');
    }

    initFile() {
        if (!fs.existsSync(this.filePath)) {
            // Crea file vuoto con header e hash table
            const fd = fs.openSync(this.filePath, 'w');
            
            // Header: [MAGIC][BUCKETS][DATA_START_OFFSET]
            const header = Buffer.alloc(this.headerSize);
            header.writeUInt32LE(0x48444353, 0); // Magic "HDCS"
            header.writeUInt32LE(this.buckets, 4);
            header.writeBigUInt64LE(BigInt(this.headerSize + this.hashTableSize), 8);
            fs.writeSync(fd, header);
            
            // Hash table vuota (tutti zeri)
            const emptyTable = Buffer.alloc(this.hashTableSize);
            fs.writeSync(fd, emptyTable);
            
            fs.closeSync(fd);
        }
    }

    // Hash function semplice ma efficace
    hash(key) {
        const hash = crypto.createHash('md5').update(key).digest();
        return hash.readUInt32LE(0) % this.buckets;
    }

    // Legge entry dalla hash table
    async readBucket(bucket) {
        const bucketOffset = this.headerSize + (bucket * this.bucketSize);
        const buffer = Buffer.alloc(this.bucketSize);
        
        await new Promise((resolve, reject) => {
            fs.read(this.fd, buffer, 0, this.bucketSize, bucketOffset, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
        
        const offset = buffer.readBigUInt64LE(0);
        const size = buffer.readUInt32LE(8);
        
        return { offset: Number(offset), size };
    }

    // Scrive entry nella hash table
    async writeBucket(bucket, offset, size) {
        const bucketOffset = this.headerSize + (bucket * this.bucketSize);
        const buffer = Buffer.alloc(this.bucketSize);
        
        buffer.writeBigUInt64LE(BigInt(offset), 0);
        buffer.writeUInt32LE(size, 8);
        
        await new Promise((resolve, reject) => {
            fs.write(this.fd, buffer, 0, this.bucketSize, bucketOffset, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    // Linear probing per gestire collisioni
    async findSlot(key, forWrite = false) {
        let bucket = this.hash(key);
        const originalBucket = bucket;
        
        while (true) {
            const entry = await this.readBucket(bucket);
            
            // Slot vuoto
            if (entry.offset === 0) {
                return forWrite ? bucket : null;
            }
            
            // Verifica se è il documento giusto
            if (!forWrite) {
                const storedKey = await this.readKeyAt(entry.offset);
                if (storedKey === key) {
                    return bucket;
                }
            }
            
            // Linear probing
            bucket = (bucket + 1) % this.buckets;
            
            // Tabella piena (non dovrebbe mai succedere con load factor basso)
            if (bucket === originalBucket) {
                throw new Error('Hash table full');
            }
        }
    }

    // Legge la chiave dal documento per verificare match
    async readKeyAt(offset) {
        // Formato documento: [keyLen(4)][key][docLen(4)][document]
        const keyLenBuffer = Buffer.alloc(4);
        
        await new Promise((resolve, reject) => {
            fs.read(this.fd, keyLenBuffer, 0, 4, offset, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
        
        const keyLen = keyLenBuffer.readUInt32LE(0);
        const keyBuffer = Buffer.alloc(keyLen);
        
        await new Promise((resolve, reject) => {
            fs.read(this.fd, keyBuffer, 0, keyLen, offset + 4, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
        
        return keyBuffer.toString('utf8');
    }

    // GET - O(1) accesso
    async get(id) {
        try {
            const bucket = await this.findSlot(id, false);
            if (bucket === null) return null;
            
            const entry = await this.readBucket(bucket);
            const docOffset = entry.offset + 4 + id.length + 4; // Skip key data
            
            // Legge lunghezza documento
            const docLenBuffer = Buffer.alloc(4);
            await new Promise((resolve, reject) => {
                fs.read(this.fd, docLenBuffer, 0, 4, entry.offset + 4 + id.length, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
            
            const docLen = docLenBuffer.readUInt32LE(0);
            
            // Legge documento
            const docBuffer = Buffer.alloc(docLen);
            await new Promise((resolve, reject) => {
                fs.read(this.fd, docBuffer, 0, docLen, docOffset, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
            
            return JSON.parse(docBuffer.toString('utf8'));
            
        } catch (error) {
            console.error('Error getting document:', error);
            return null;
        }
    }

    // PUT - O(1) scrittura
    async put(id, document) {
        try {
            const jsonStr = JSON.stringify(document);
            const jsonBuffer = Buffer.from(jsonStr, 'utf8');
            const keyBuffer = Buffer.from(id, 'utf8');
            
            // Formato: [keyLen][key][docLen][document]
            const totalSize = 4 + keyBuffer.length + 4 + jsonBuffer.length;
            const recordBuffer = Buffer.alloc(totalSize);
            
            let offset = 0;
            recordBuffer.writeUInt32LE(keyBuffer.length, offset);
            offset += 4;
            keyBuffer.copy(recordBuffer, offset);
            offset += keyBuffer.length;
            recordBuffer.writeUInt32LE(jsonBuffer.length, offset);
            offset += 4;
            jsonBuffer.copy(recordBuffer, offset);
            
            // Append al file
            const stats = fs.fstatSync(this.fd);
            const writeOffset = stats.size;
            
            await new Promise((resolve, reject) => {
                fs.write(this.fd, recordBuffer, 0, totalSize, writeOffset, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
            
            // Aggiorna hash table
            const bucket = await this.findSlot(id, true);
            await this.writeBucket(bucket, writeOffset, totalSize);
            
        } catch (error) {
            console.error('Error putting document:', error);
            throw error;
        }
    }

    // DELETE - O(1) rimozione (marca come deleted)
    async delete(id) {
        try {
            const bucket = await this.findSlot(id, false);
            if (bucket === null) return false;
            
            // Marca slot come vuoto
            await this.writeBucket(bucket, 0, 0);
            return true;
            
        } catch (error) {
            console.error('Error deleting document:', error);
            return false;
        }
    }

    close() {
        if (this.fd) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
    }

    // Utility per statistiche
    async stats() {
        let usedSlots = 0;
        
        for (let i = 0; i < this.buckets; i++) {
            const entry = await this.readBucket(i);
            if (entry.offset > 0) usedSlots++;
        }
        
        const stats = fs.fstatSync(this.fd);
        
        return {
            buckets: this.buckets,
            usedSlots,
            loadFactor: (usedSlots / this.buckets * 100).toFixed(2) + '%',
            fileSize: stats.size,
            fileSizeMB: (stats.size / 1024 / 1024).toFixed(2) + ' MB'
        };
    }
}

// Esempio d'uso
async function example() {
    const storage = new HashDocumentStorage('./documents.hds', 100000);
    
    try {
        // PUT
        await storage.put('user123', { 
            name: 'Mario Rossi', 
            email: 'mario@example.com',
            age: 30,
            created: new Date().toISOString()
        });
        
        await storage.put('user456', {
            name: 'Laura Verdi',
            email: 'laura@example.com', 
            age: 28
        });
        
        // GET - O(1)
        const user = await storage.get('user123');
        console.log('Retrieved user:', user);
        
        console.time('1');
        const nonExistent = await storage.get('user999');
        console.timeEnd('1');
        console.log('Non-existent user:', nonExistent);
        
        // Stats
        const stats = await storage.stats();
        console.log('Storage stats:', stats);
        
        // DELETE
        const deleted = await storage.delete('user456');
        console.log('Deleted user456:', deleted);
        
    } finally {
        storage.close();
    }
}

// Decommentare per testare
example().catch(console.error);

module.exports = HashDocumentStorage;
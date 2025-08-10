

class TopNHeap {
  constructor(maxSize, orderFields) {
    this.maxSize = maxSize;
    this.orderFields = orderFields;
    this.heap = [];
  }

  add(doc) {
    if (this.heap.length < this.maxSize) {
      this.heap.push(doc);
      if (this.heap.length === this.maxSize) {
        this._buildHeap();
      }
    } else {
      if (this._shouldReplace(doc, this.heap[0])) {
        this.heap[0] = doc;
        this._heapifyDown(0);
      }
    }
  }

  getSorted() {
    const result = [...this.heap];
    return result.sort((a, b) => this._compare(a, b));
  }

  _shouldReplace(newDoc, currentWorst) {
    const comparison = this._compare(newDoc, currentWorst);
    const isAscending = this.orderFields[0].direction === 'ASC';
    return isAscending ? comparison < 0 : comparison > 0;
  }

  _compare(a, b) {
    for (const { field, direction } of this.orderFields) {
      const aVal = this._getValue(a, field);
      const bVal = this._getValue(b, field);
      
      const comparison = this._compareValues(aVal, bVal);
      
      if (comparison !== 0) {
        return direction === 'DESC' ? -comparison : comparison;
      }
    }
    return 0;
  }

  _getValue(doc, field) {
    if (field.includes('.')) {
      return field.split('.').reduce((obj, key) => obj && obj[key], doc);
    }
    return doc[field];
  }

  _compareValues(a, b) {
    if (a === null || a === undefined) return b === null || b === undefined ? 0 : -1;
    if (b === null || b === undefined) return 1;
    
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime();
    
    return String(a).localeCompare(String(b));
  }

  _buildHeap() {
    for (let i = Math.floor(this.heap.length / 2) - 1; i >= 0; i--) {
      this._heapifyDown(i);
    }
  }

  _heapifyDown(index) {
    const leftChild = 2 * index + 1;
    const rightChild = 2 * index + 2;
    let worst = index;

    if (leftChild < this.heap.length && this._isWorse(leftChild, worst)) {
      worst = leftChild;
    }

    if (rightChild < this.heap.length && this._isWorse(rightChild, worst)) {
      worst = rightChild;
    }

    if (worst !== index) {
      [this.heap[index], this.heap[worst]] = [this.heap[worst], this.heap[index]];
      this._heapifyDown(worst);
    }
  }

  _isWorse(i, j) {
    const comparison = this._compare(this.heap[i], this.heap[j]);
    const isAscending = this.orderFields[0].direction === 'ASC';
    return isAscending ? comparison > 0 : comparison < 0;
  }
}

export default TopNHeap;

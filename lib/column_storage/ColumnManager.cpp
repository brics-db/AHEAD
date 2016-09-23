#include <llvm/ADT/FoldingSet.h>
#include <llvm/ADT/DenseMap.h>

#include "column_storage/ColumnManager.h"

ColumnManager* ColumnManager::instance = 0;

ColumnManager* ColumnManager::getInstance() {
    if (ColumnManager::instance == 0) {
        ColumnManager::instance = new ColumnManager();
    }

    return ColumnManager::instance;
}

void ColumnManager::destroyInstance() {
    if (ColumnManager::instance) {
        delete ColumnManager::instance;
        ColumnManager::instance = nullptr;
    }
}

ColumnManager::ColumnManager() {
}

ColumnManager::~ColumnManager() {
    columns.clear();
}

ColumnManager::ColumnIterator* ColumnManager::openColumn(unsigned int id, unsigned int *version) {
    if (columns.find(id) != columns.end()) {
        return new ColumnManager::ColumnIterator(&columns.find(id)->second, BucketManager::getInstance()->openStream(id, version));
    } else {
        // Problem : Spalte existiert nicht
        return 0;
    }
}

std::set<id_t> ColumnManager::listColumns() {
    std::set<id_t> list;
    std::map<id_t, Column>::iterator it;

    for (it = this->columns.begin(); it != this->columns.end(); it++) {
        list.insert(it->first);
    }

    return list;
}

void ColumnManager::createColumn(id_t id, size_t width) {
    if (columns.find(id) == columns.end()) {
        columns[id].width = width; // map auto-generates new entries
    } else {
        // Problem : Spalte existiert bereits
    }
}

size_t ColumnManager::ColumnIterator::size() {
    const unsigned int recordsPerBucket = (unsigned int) ((CHUNK_CONTENT_SIZE - sizeof (unsigned int)) / this->column->width);
    unsigned int position;
    unsigned int *elementCounter;

    if (this->iterator->countBuckets() == 0) {
        return 0;
    } else {
        position = this->iterator->position();

        this->currentChunk = this->iterator->seek(this->iterator->countBuckets() - 1);

        elementCounter = (unsigned int*) this->currentChunk->content;

        this->currentChunk = this->iterator->seek(position);

        return (this->iterator->countBuckets() - 1) * recordsPerBucket + *elementCounter;
    }
}

size_t ColumnManager::ColumnIterator::consumption() {
    return this->iterator->countBuckets() * CHUNK_CONTENT_SIZE;
}

ColumnManager::Record ColumnManager::ColumnIterator::next() {
    if (this->currentChunk != 0) {
        unsigned int *elementCounter = (unsigned int*) this->currentChunk->content;

        if (this->currentPosition >= *elementCounter) {
            BucketManager::Chunk *chunk = this->iterator->next();

            if (chunk == 0) {
                // Problem : Ende der Spalte
                this->currentChunk = 0;
                return Record(nullptr);
            } else {
                this->currentChunk = chunk;
                this->currentPosition = 0;
                return next();
            }
        } else {
            Record rec(reinterpret_cast<char*> (this->currentChunk->content) + sizeof (unsigned int) + this->currentPosition * this->column->width);
            this->currentPosition++;
            return rec;
        }
    } else {
        if (this->currentPosition == 0) {
            this->currentChunk = this->iterator->next();
            if (this->currentChunk != 0) {
                return next();
            } else {
                // Problem : Spalte enthält keine Elemente
                return Record(nullptr);
            }
        } else {
            // Problem : Ende der Spalte
            return Record(nullptr);
        }
    }
}

ColumnManager::Record ColumnManager::ColumnIterator::seek(oid_t index) {
    const size_t recordsPerBucket = (size_t) ((CHUNK_CONTENT_SIZE - sizeof (size_t)) / this->column->width);

    this->currentChunk = this->iterator->seek(index / recordsPerBucket);

    if (this->currentChunk != 0) {
        unsigned int *elementCounter = (unsigned int*) this->currentChunk->content;

        this->currentPosition = index - recordsPerBucket * (index / recordsPerBucket);

        if (this->currentPosition >= *elementCounter) {
            // Problem : Falscher Index
            rewind();
            return Record(nullptr);
        } else {
            Record rec(reinterpret_cast<char*> (this->currentChunk->content) + sizeof (unsigned int) + this->currentPosition * this->column->width);
            this->currentPosition++;
            return rec;
        }
    } else {
        // Problem : Falscher Index
        rewind();
        return Record(nullptr);
    }
}

void ColumnManager::ColumnIterator::rewind() {
    this->iterator->rewind();
    this->currentChunk = 0;
    this->currentPosition = 0;
}

ColumnManager::Record ColumnManager::ColumnIterator::edit() {
    if (this->currentChunk != 0) {
        this->currentChunk = this->iterator->edit();
        return Record(reinterpret_cast<char*> (this->currentChunk->content) + sizeof (unsigned int) + (this->currentPosition - 1) * this->column->width);
    } else {
        // Problem : Anfang/Ende der Spalte
        return Record(nullptr);
    }
}

ColumnManager::Record ColumnManager::ColumnIterator::append() {
    unsigned int *elementCounter = nullptr;

    unsigned numBuckets = this->iterator->countBuckets();
    if (numBuckets == 0) {
        this->currentChunk = this->iterator->append();
        elementCounter = (unsigned*) this->currentChunk->content;
        *elementCounter = 0;
    } else {
        this->currentChunk = this->iterator->seek(numBuckets - 1);
        elementCounter = (unsigned*) this->currentChunk->content;
        if (*elementCounter == recordsPerBucket) {
            this->currentChunk = this->iterator->append();
            elementCounter = (unsigned*) this->currentChunk->content;
            *elementCounter = 0;
        } else {
            this->currentChunk = this->iterator->edit();
            elementCounter = (unsigned*) this->currentChunk->content;
        }
    }

    this->currentPosition = *elementCounter;

    Record rec(reinterpret_cast<char*> (this->currentChunk->content) + sizeof (unsigned int) + this->currentPosition * this->column->width);
    this->currentPosition++;
    (*elementCounter)++;
    return rec;
}

void ColumnManager::ColumnIterator::undo() {
    this->iterator->undo();
    this->currentChunk = 0;
    this->currentPosition = 0;
}

ColumnManager::ColumnIterator::ColumnIterator(Column *column, BucketManager::BucketIterator *iterator) : recordsPerBucket(static_cast<unsigned> ((CHUNK_CONTENT_SIZE - sizeof (unsigned)) / column->width)) {
    this->iterator = iterator;
    this->iterator->rewind();
    this->column = column;
    this->currentChunk = 0;
    this->currentPosition = 0;
}

ColumnManager::ColumnIterator::ColumnIterator(const ColumnIterator& copy) : recordsPerBucket(copy.recordsPerBucket) {
    this->iterator = new BucketManager::BucketIterator(*copy.iterator);
    this->column = new Column;
    this->column->width = copy.column->width;
    this->currentChunk = copy.currentChunk;
    this->currentPosition = copy.currentPosition;
}

ColumnManager::ColumnIterator::~ColumnIterator() {
    delete iterator;
}

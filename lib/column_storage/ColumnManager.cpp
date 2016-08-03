#include "column_storage/ColumnManager.h"

ColumnManager* ColumnManager::instance = 0;

ColumnManager* ColumnManager::getInstance() {
    if (ColumnManager::instance == 0) {
        ColumnManager::instance = new ColumnManager();
    }

    return ColumnManager::instance;
}

ColumnManager::ColumnIterator* ColumnManager::openColumn(unsigned int id, unsigned int *version) {
    if (columns.find(id) != columns.end()) {
        return new ColumnManager::ColumnIterator(&columns.find(id)->second, BucketManager::getInstance()->openStream(id, version));
    } else {
        // Problem : Spalte existiert nicht
        return 0;
    }
}

std::set<unsigned int> ColumnManager::listColumns() {
    std::set<unsigned int> list;
    std::map<unsigned int, Column>::iterator it;

    for (it = this->columns.begin(); it != this->columns.end(); it++) {
        list.insert(it->first);
    }

    return list;
}

void ColumnManager::createColumn(unsigned int id, unsigned int width) {
    if (columns.find(id) == columns.end()) {
        columns[id].width = width; // map auto-generates new entries
    } else {
        // Problem : Spalte existiert bereits
    }
}

ColumnManager::ColumnManager() {
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

ColumnManager::Record* ColumnManager::ColumnIterator::next() {
    if (this->currentChunk != 0) {
        unsigned int *elementCounter = (unsigned int*) this->currentChunk->content;

        if (this->currentPosition >= *elementCounter) {
            BucketManager::Chunk *chunk = this->iterator->next();

            if (chunk == 0) {
                // Problem : Ende der Spalte

                this->currentChunk = 0;

                return 0;
            } else {
                this->currentChunk = chunk;
                this->currentPosition = 0;

                return next();
            }
        } else {
            Record *record = new Record;
            record->content = (char*) this->currentChunk->content + sizeof (unsigned int) + this->currentPosition * this->column->width;

            this->currentPosition++;

            return record;
        }
    } else {
        if (this->currentPosition == 0) {
            this->currentChunk = this->iterator->next();

            if (this->currentChunk != 0) {
                return next();
            } else {
                // Problem : Spalte enth�lt keine Elemente

                return 0;
            }
        } else {
            // Problem : Ende der Spalte

            return 0;
        }
    }
}

ColumnManager::Record* ColumnManager::ColumnIterator::seek(unsigned int index) {
    const unsigned int recordsPerBucket = (unsigned int) ((CHUNK_CONTENT_SIZE - sizeof (unsigned int)) / this->column->width);

    this->currentChunk = this->iterator->seek(index / recordsPerBucket);

    if (this->currentChunk != 0) {
        unsigned int *elementCounter = (unsigned int*) this->currentChunk->content;

        this->currentPosition = index - recordsPerBucket * (index / recordsPerBucket);

        if (this->currentPosition >= *elementCounter) {
            // Problem : Falscher Index
            rewind();
            return 0;
        } else {
            Record *record = new Record;
            record->content = (char*) this->currentChunk->content + sizeof (unsigned int) + this->currentPosition * this->column->width;

            this->currentPosition++;

            return record;
        }
    } else {
        // Problem : Falscher Index
        rewind();
        return 0;
    }
}

void ColumnManager::ColumnIterator::rewind() {
    this->iterator->rewind();
    this->currentChunk = 0;
    this->currentPosition = 0;
}

ColumnManager::Record* ColumnManager::ColumnIterator::edit() {
    if (this->currentChunk != 0) {
        this->currentChunk = this->iterator->edit();

        // TODO unsigned int *elementCounter = (unsigned int*) this->currentChunk->content;

        Record *record = new Record;
        record->content = (char*) this->currentChunk->content + sizeof (unsigned int) + (this->currentPosition - 1) * this->column->width;

        return record;
    } else {
        // Problem : Anfang/Ende der Spalte
        return 0;
    }
}

ColumnManager::Record* ColumnManager::ColumnIterator::append() {
    const unsigned int recordsPerBucket = (unsigned int) ((CHUNK_CONTENT_SIZE - sizeof (unsigned int)) / this->column->width);
    unsigned int *elementCounter;

    if (this->iterator->countBuckets() == 0) {
        this->currentChunk = this->iterator->append();
        elementCounter = (unsigned int*) this->currentChunk->content;

        *elementCounter = 0;
    } else {
        this->currentChunk = this->iterator->seek(this->iterator->countBuckets() - 1);
        elementCounter = (unsigned int*) this->currentChunk->content;

        if (*elementCounter == recordsPerBucket) {
            this->currentChunk = this->iterator->append();
            elementCounter = (unsigned int*) this->currentChunk->content;

            *elementCounter = 0;
        } else {
            this->currentChunk = this->iterator->edit();
            elementCounter = (unsigned int*) this->currentChunk->content;
        }
    }

    this->currentPosition = *elementCounter;

    Record *record = new Record;
    record->content = (char*) this->currentChunk->content + sizeof (unsigned int) + this->currentPosition * this->column->width;

    this->currentPosition++;
    (*elementCounter)++;

    return record;
}

void ColumnManager::ColumnIterator::undo() {
    this->iterator->undo();
    this->currentChunk = 0;
    this->currentPosition = 0;
}

ColumnManager::ColumnIterator::ColumnIterator(Column *column, BucketManager::BucketIterator *iterator) {
    this->iterator = iterator;
    this->iterator->rewind();
    this->column = column;
    this->currentChunk = 0;
    this->currentPosition = 0;
}

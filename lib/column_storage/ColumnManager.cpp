
#include <sstream>

#include "column_storage/ColumnManager.h"

ColumnManager* ColumnManager::instance = 0;

ColumnManager*
ColumnManager::getInstance () {
    if (ColumnManager::instance == 0) {
        ColumnManager::instance = new ColumnManager();
    }

    return ColumnManager::instance;
}

void
ColumnManager::destroyInstance () {
    if (ColumnManager::instance) {
        delete ColumnManager::instance;
        ColumnManager::instance = nullptr;
    }
}

ColumnManager::ColumnManager () : columnMetaData () {
}

ColumnManager::~ColumnManager () {
    columnMetaData.clear();
}

ColumnManager::ColumnIterator*
ColumnManager::openColumn (unsigned int id, unsigned int *version) {
    if (columnMetaData.find(id) != columnMetaData.end()) {
        return new ColumnManager::ColumnIterator(columnMetaData.find(id)->second, BucketManager::getInstance()->openStream(id, version));
    } else {
        // Problem : Spalte existiert nicht
        return 0;
    }
}

std::unordered_set<id_t>
ColumnManager::getColumnIDs () {
    std::unordered_set<id_t> list;
    list.reserve(columnMetaData.size());

    for (auto it = this->columnMetaData.begin(); it != this->columnMetaData.end(); it++) {
        list.insert(it->first);
    }

    return list;
}

std::unordered_map<id_t, ColumnMetaData> *
ColumnManager::getColumnMetaData () {
    auto result = new std::unordered_map<id_t, ColumnMetaData>;
    result->reserve(columnMetaData.size());
    for (auto p : columnMetaData) {
        (*result)[p.first] = p.second;
    }
    return result;
}

ColumnMetaData
ColumnManager::getColumnMetaData (id_t id) {
    auto iter = columnMetaData.find(id);
    if (iter == columnMetaData.end()) {
        std::stringstream ss;
        ss << "ColumnManager::Column::getColumn(id_t) : id " << id << " is invalid!";
        throw std::runtime_error(ss.str());
    }
    return iter->second;
}

ColumnMetaData &
ColumnManager::createColumn (id_t id, uint32_t width) {
    auto iter = columnMetaData.find(id);
    if (iter != columnMetaData.end()) {
        std::stringstream ss;
        ss << "There is already a column with id " << id;
        throw std::runtime_error(ss.str());
    }
    columnMetaData.emplace(id, width);
    return iter->second;
}

ColumnMetaData &
ColumnManager::createColumn (id_t id, ColumnMetaData && column) {
    auto iter = columnMetaData.find(id);
    if (iter != columnMetaData.end()) {
        std::stringstream ss;
        ss << "There is already a column with id " << id;
        throw std::runtime_error(ss.str());
    }
    columnMetaData.emplace(id, std::forward<ColumnMetaData>(column));
    return iter->second;
}

size_t
ColumnManager::ColumnIterator::size () {
    const unsigned int recordsPerBucket = (unsigned int)((CHUNK_CONTENT_SIZE - sizeof (unsigned int)) / this->columnMetaData.width);
    unsigned int position;
    unsigned int *elementCounter;

    if (this->iterator->countBuckets() == 0) {
        return 0;
    } else {
        position = this->iterator->position();

        this->currentChunk = this->iterator->seek(this->iterator->countBuckets() - 1);

        elementCounter = (unsigned int*)this->currentChunk->content;

        this->currentChunk = this->iterator->seek(position);

        return (this->iterator->countBuckets() - 1) * recordsPerBucket + *elementCounter;
    }
}

size_t
ColumnManager::ColumnIterator::consumption () {
    return this->iterator->countBuckets() * CHUNK_CONTENT_SIZE;
}

ColumnManager::Record
ColumnManager::ColumnIterator::next () {
    if (this->currentChunk != 0) {
        unsigned int *elementCounter = (unsigned int*)this->currentChunk->content;

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
            Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof (unsigned int) + this->currentPosition * this->columnMetaData.width);
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

ColumnManager::Record
ColumnManager::ColumnIterator::seek (oid_t index) {
    const size_t recordsPerBucket = (size_t)((CHUNK_CONTENT_SIZE - sizeof (size_t)) / this->columnMetaData.width);

    this->currentChunk = this->iterator->seek(index / recordsPerBucket);

    if (this->currentChunk != 0) {
        unsigned int *elementCounter = (unsigned int*)this->currentChunk->content;

        this->currentPosition = index - recordsPerBucket * (index / recordsPerBucket);

        if (this->currentPosition >= *elementCounter) {
            // Problem : Falscher Index
            rewind();
            return Record(nullptr);
        } else {
            Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof (unsigned int) + this->currentPosition * this->columnMetaData.width);
            this->currentPosition++;
            return rec;
        }
    } else {
        // Problem : Falscher Index
        rewind();
        return Record(nullptr);
    }
}

void
ColumnManager::ColumnIterator::rewind () {
    this->iterator->rewind();
    this->currentChunk = 0;
    this->currentPosition = 0;
}

ColumnManager::Record
ColumnManager::ColumnIterator::edit () {
    if (this->currentChunk != 0) {
        this->currentChunk = this->iterator->edit();
        return Record(reinterpret_cast<char*>(this->currentChunk->content) + sizeof (unsigned int) + (this->currentPosition - 1) * this->columnMetaData.width);
    } else {
        // Problem : Anfang/Ende der Spalte
        return Record(nullptr);
    }
}

ColumnManager::Record
ColumnManager::ColumnIterator::append () {
    unsigned int *elementCounter = nullptr;

    unsigned numBuckets = this->iterator->countBuckets();
    if (numBuckets == 0) {
        this->currentChunk = this->iterator->append();
        elementCounter = (unsigned*)this->currentChunk->content;
        *elementCounter = 0;
    } else {
        this->currentChunk = this->iterator->seek(numBuckets - 1);
        elementCounter = (unsigned*)this->currentChunk->content;
        if (*elementCounter == recordsPerBucket) {
            this->currentChunk = this->iterator->append();
            elementCounter = (unsigned*)this->currentChunk->content;
            *elementCounter = 0;
        } else {
            this->currentChunk = this->iterator->edit();
            elementCounter = (unsigned*)this->currentChunk->content;
        }
    }

    this->currentPosition = *elementCounter;

    Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof (unsigned int) + this->currentPosition * this->columnMetaData.width);
    this->currentPosition++;
    (*elementCounter)++;
    return rec;
}

void
ColumnManager::ColumnIterator::undo () {
    this->iterator->undo();
    this->currentChunk = 0;
    this->currentPosition = 0;
}

ColumnManager::ColumnIterator::ColumnIterator (ColumnMetaData & columnMetaData, BucketManager::BucketIterator *iterator) : iterator (iterator), columnMetaData (columnMetaData), currentChunk (nullptr), currentPosition (0), recordsPerBucket (static_cast<unsigned>((CHUNK_CONTENT_SIZE - sizeof (unsigned)) / columnMetaData.width)) {
    this->iterator->rewind();
}

ColumnManager::ColumnIterator::ColumnIterator (const ColumnIterator & copy) : iterator (new BucketManager::BucketIterator (*copy.iterator)), columnMetaData (copy.columnMetaData), currentChunk (copy.currentChunk), currentPosition (copy.currentPosition), recordsPerBucket (copy.recordsPerBucket) {
}

ColumnManager::ColumnIterator::~ColumnIterator () {
    delete iterator;
}

ColumnManager::ColumnIterator& ColumnManager::ColumnIterator::operator= (const ColumnManager::ColumnIterator &copy) {
    new (this) ColumnIterator(copy);
    return *this;
}

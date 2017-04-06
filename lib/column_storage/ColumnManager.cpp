// Copyright (c) 2016-2017 Till Kolditz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <sstream>
#include <fstream>

#include <column_storage/ColumnManager.h>
#include <column_storage/TransactionManager.h>

namespace ahead {

    ColumnManager* ColumnManager::instance = 0;

    ColumnManager*
    ColumnManager::getInstance() {
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

    ColumnManager::ColumnManager()
            : columnMetaData() {
    }

    ColumnManager::~ColumnManager() {
        columnMetaData.clear();
    }

    ColumnManager::ColumnIterator*
    ColumnManager::openColumn(id_t id, version_t *version) {
        if (columnMetaData.find(id) != columnMetaData.end()) {
            return new ColumnManager::ColumnIterator(columnMetaData.find(id)->second, BucketManager::getInstance()->openStream(id, version));
        } else {
            // Problem : Spalte existiert nicht
            return nullptr;
        }
    }

    std::unordered_set<id_t> ColumnManager::getColumnIDs() {
        std::unordered_set<id_t> list;
        list.reserve(columnMetaData.size());

        for (auto it = this->columnMetaData.begin(); it != this->columnMetaData.end(); it++) {
            list.insert(it->first);
        }

        return list;
    }

    std::unordered_map<id_t, ColumnMetaData> *
    ColumnManager::getColumnMetaData() {
        auto result = new std::unordered_map<id_t, ColumnMetaData>;
        result->reserve(columnMetaData.size());
        for (auto p : columnMetaData) {
            (*result)[p.first] = p.second;
        }
        return result;
    }

    ColumnMetaData ColumnManager::getColumnMetaData(id_t id) {
        auto iter = columnMetaData.find(id);
        if (iter == columnMetaData.end()) {
            std::stringstream ss;
            ss << "ColumnManager::Column::getColumn(id_t) : id " << id << " is invalid!";
            throw std::runtime_error(ss.str());
        }
        return iter->second;
    }

    ColumnMetaData &
    ColumnManager::createColumn(id_t id, uint32_t width) {
        auto iter = columnMetaData.find(id);
        if (iter != columnMetaData.end()) {
            std::stringstream ss;
            ss << "There is already a column with id " << id;
            throw std::runtime_error(ss.str());
        }
        auto pairIns = columnMetaData.emplace(id, width);
        if (!pairIns.second) {
            std::stringstream ss;
            ss << "Could not create column with id " << id;
            throw std::runtime_error(ss.str());
        }
        return pairIns.first->second;
    }

    ColumnMetaData &
    ColumnManager::createColumn(id_t id, ColumnMetaData && column) {
        auto iter = columnMetaData.find(id);
        if (iter != columnMetaData.end()) {
            std::stringstream ss;
            ss << "There is already a column with id " << id;
            throw std::runtime_error(ss.str());
        }
        auto pairIns = columnMetaData.emplace(id, std::forward<ColumnMetaData>(column));
        return pairIns.first->second;
    }

    size_t ColumnManager::ColumnIterator::size() {
        const oid_t recordsPerBucket = (oid_t) ((CHUNK_CONTENT_SIZE - sizeof(oid_t)) / this->columnMetaData.width);
        oid_t position;
        oid_t * elementCounter;
        if (this->iterator->countBuckets() == 0) {
            return 0;
        } else {
            position = this->iterator->position();
            this->currentChunk = this->iterator->seek(this->iterator->countBuckets() - 1);
            elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            this->currentChunk = this->iterator->seek(position);
            return (this->iterator->countBuckets() - 1) * recordsPerBucket + *elementCounter;
        }
    }

    size_t ColumnManager::ColumnIterator::consumption() {
        return this->iterator->countBuckets() * CHUNK_CONTENT_SIZE;
    }

    ColumnManager::Record ColumnManager::ColumnIterator::next() {
        if (this->currentChunk != 0) {
            oid_t * elementCounter = static_cast<oid_t *>(this->currentChunk->content);

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
                Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + this->currentPosition * this->columnMetaData.width);
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
        this->currentChunk = this->iterator->seek(index / this->recordsPerBucket);

        if (this->currentChunk != 0) {
            oid_t * elementCounter = (oid_t *) this->currentChunk->content;

            this->currentPosition = index - this->recordsPerBucket * (index / this->recordsPerBucket);

            if (this->currentPosition >= *elementCounter) {
                // Problem : Falscher Index
                rewind();
                return Record(nullptr);
            } else {
                Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + this->currentPosition * this->columnMetaData.width);
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
            return Record(reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + (this->currentPosition - 1) * this->columnMetaData.width);
        } else {
            // Problem : Anfang/Ende der Spalte
            return Record(nullptr);
        }
    }

    ColumnManager::Record ColumnManager::ColumnIterator::append() {
        oid_t * elementCounter = nullptr;

        oid_t numBuckets = this->iterator->countBuckets();
        if (numBuckets == 0) {
            this->currentChunk = this->iterator->append();
            elementCounter = (oid_t *) this->currentChunk->content;
        } else {
            this->currentChunk = this->iterator->seek(numBuckets - 1);
            elementCounter = (oid_t *) this->currentChunk->content;
            if (*elementCounter == this->recordsPerBucket) {
                this->currentChunk = this->iterator->append();
                elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            } else {
                this->currentChunk = this->iterator->edit();
                elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            }
        }

        this->currentPosition = *elementCounter;

        Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + this->currentPosition * this->columnMetaData.width);
        this->currentPosition++;
        (*elementCounter)++;
        return rec;
    }

    void ColumnManager::ColumnIterator::read(std::istream & inStream) {
        const size_t pos = inStream.tellg();
        inStream.seekg(0, std::ios_base::end);
        const size_t numBytesTotal = static_cast<size_t>(inStream.tellg()) - pos;
        inStream.seekg(pos, std::ios_base::beg);
        // char * buffer = new char[numBytesTotal];
        // istream.read(buffer, numBytesTotal);
        ssize_t totalValues = static_cast<ssize_t>(numBytesTotal / this->columnMetaData.width);
        // char * pSrc = buffer;
        oid_t * elementCounter = nullptr;

        // find first bucket to insert. Mostly this will be the very first (initialized) bucket
        oid_t numBuckets = this->iterator->countBuckets();
        if (numBuckets == 0) {
            this->currentChunk = this->iterator->append();
            elementCounter = static_cast<oid_t *>(this->currentChunk->content);
        } else {
            this->currentChunk = this->iterator->seek(numBuckets - 1);
            elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            if (*elementCounter == this->recordsPerBucket) {
                this->currentChunk = this->iterator->append();
                elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            } else {
                this->currentChunk = this->iterator->edit();
                elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            }
        }

        // now read in the contents and split it into bucket sizes
        while (totalValues > 0) {
            this->currentPosition = *elementCounter;
            size_t numValuesToInsert = this->recordsPerBucket - this->currentPosition;
            if (numValuesToInsert > static_cast<size_t>(totalValues)) {
                numValuesToInsert = totalValues;
            }
            size_t numBytesToInsert = numValuesToInsert * this->columnMetaData.width;
            *elementCounter = this->currentPosition + numValuesToInsert;
            char * pDest = reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + this->currentPosition * this->columnMetaData.width;
            // std::memcpy(pDest, pSrc, numValuesToInsert * this->columnMetaData.width);
            inStream.read(pDest, numBytesToInsert);
            totalValues -= numValuesToInsert;
            if (totalValues > 0) {
                this->currentChunk = this->iterator->append();
                elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            }
        }
    }

    void ColumnManager::ColumnIterator::write(std::ostream & outStream) {
        this->rewind();
        auto pChunk = this->iterator->next();
        while (pChunk != nullptr) {
            auto numValues = *static_cast<oid_t*>(pChunk->content);
            char * pStart = reinterpret_cast<char*>(reinterpret_cast<oid_t*>(pChunk->content) + 1);
            outStream.write(pStart, numValues * this->columnMetaData.width);
            pChunk = this->iterator->next();
        }
    }

    void ColumnManager::ColumnIterator::undo() {
        this->iterator->undo();
        this->currentChunk = 0;
        this->currentPosition = 0;
    }

    ColumnManager::ColumnIterator::ColumnIterator(ColumnMetaData & columnMetaData, BucketManager::BucketIterator *iterator)
            : iterator(iterator), columnMetaData(columnMetaData), currentChunk(nullptr), currentPosition(0),
                    recordsPerBucket(static_cast<oid_t>((CHUNK_CONTENT_SIZE - sizeof(oid_t)) / columnMetaData.width)) {
        this->iterator->rewind();
    }

    ColumnManager::ColumnIterator::ColumnIterator(const ColumnIterator & copy)
            : iterator(new BucketManager::BucketIterator(*copy.iterator)), columnMetaData(copy.columnMetaData), currentChunk(copy.currentChunk), currentPosition(copy.currentPosition),
                    recordsPerBucket(copy.recordsPerBucket) {
    }

    ColumnManager::ColumnIterator::~ColumnIterator() {
        delete iterator;
    }

    ColumnManager::ColumnIterator& ColumnManager::ColumnIterator::operator=(const ColumnManager::ColumnIterator &copy) {
        new (this) ColumnIterator(copy);
        return *this;
    }

}

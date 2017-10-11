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
#include <memory>

#include <AHEAD.hpp>
#include <column_storage/ColumnManager.h>
#include <column_storage/TransactionManager.h>
#include <util/resilience.hpp>
#include <util/v2typeconversion.hpp>

namespace ahead {

    const size_t ColumnManager::BAT_COLNAMES_MAXLEN = 256;
    const id_t ColumnManager::ID_BAT_COLNAMES = 0;
    const id_t ColumnManager::ID_BAT_COLTYPES = 1;
    const id_t ColumnManager::ID_BAT_COLIDENT = 2;
    const id_t ColumnManager::ID_BAT_FIRST_USER = 3;

    std::shared_ptr<ColumnManager> ColumnManager::instance(new ColumnManager);

    std::shared_ptr<ColumnManager> ColumnManager::getInstance() {
        return ColumnManager::instance;
    }

    void ColumnManager::destroyInstance() {
        if (ColumnManager::instance) {
            ColumnManager::instance.reset();
        }
    }

    ColumnManager::ColumnManager()
            : columnMetaData(),
              nextID(ID_BAT_FIRST_USER) {
        createColumn(ID_BAT_COLNAMES, size_bytes<char[BAT_COLNAMES_MAXLEN]>);
        createColumn(ID_BAT_COLTYPES, size_bytes<column_type_t>);
        createColumn(ID_BAT_COLIDENT, size_bytes<id_t>);
    }

    ColumnManager::~ColumnManager() {
        columnMetaData.clear();
    }

    ColumnManager::ColumnIterator*
    ColumnManager::openColumn(
            id_t id,
            std::shared_ptr<version_t> & version) {
        if (columnMetaData.find(id) != columnMetaData.end()) {
            return new ColumnManager::ColumnIterator(id, columnMetaData.find(id)->second, BucketManager::getInstance()->openStream(id, version));
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

    ColumnMetaData ColumnManager::getColumnMetaData(
            id_t id) {
        return getColumnMetaDataRef(id);
    }

    ColumnMetaData & ColumnManager::getColumnMetaDataRef(
            id_t id) {
        auto iter = columnMetaData.find(id);
        if (iter == columnMetaData.end()) {
            std::stringstream ss;
            ss << "ColumnManager::Column::getColumnMetaData(@" << __LINE__ << ") : id " << id << " is invalid!";
            throw std::runtime_error(ss.str());
        }
        return iter->second;
    }

    id_t ColumnManager::getNextColumnID() {
        return nextID++;
    }

    ColumnMetaData &
    ColumnManager::createColumn(
            id_t id,
            data_width_t width) {
        auto iter = columnMetaData.find(id);
        if (iter != columnMetaData.end()) {
            std::stringstream ss;
            ss << "ColumnManager::createColumn(@" << __LINE__ << ") There is already a column with id " << id;
            throw std::runtime_error(ss.str());
        }
        auto pairIns = columnMetaData.emplace(id, width);
        if (!pairIns.second) {
            std::stringstream ss;
            ss << "ColumnManager::createColumn(@" << __LINE__ << ") Could not create column with id " << id;
            throw std::runtime_error(ss.str());
        }
        return pairIns.first->second;
    }

    ColumnMetaData &
    ColumnManager::createColumn(
            id_t id,
            ColumnMetaData && column) {
        auto iter = columnMetaData.find(id);
        if (iter != columnMetaData.end()) {
            std::stringstream ss;
            ss << "ColumnManager::createColumn(@" << __LINE__ << ") There is already a column with id " << id;
            throw std::runtime_error(ss.str());
        }
        auto pairIns = columnMetaData.emplace(id, std::forward<ColumnMetaData>(column));
        return pairIns.first->second;
    }

    size_t ColumnManager::ColumnIterator::size() {
        const oid_t recordsPerBucket = (oid_t) ((CHUNK_CONTENT_SIZE - sizeof(oid_t)) / ahead::get<bytes_t>(this->columnMetaData.width));
        oid_t position;
        oid_t * elementCounter;
        auto numBuckets = this->iterator->countBuckets();
        if (numBuckets == 0) {
            return 0;
        } else {
            position = this->iterator->position();
            this->currentChunk = this->iterator->seek(numBuckets - 1);
            elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            this->currentChunk = this->iterator->seek(position);
            return (numBuckets - 1) * recordsPerBucket + *elementCounter;
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
                Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + this->currentPosition * ahead::get<bytes_t>(this->columnMetaData.width));
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

    ColumnManager::Record ColumnManager::ColumnIterator::seek(
            oid_t index) {
        this->currentChunk = this->iterator->seek(index / this->recordsPerBucket);

        if (this->currentChunk != 0) {
            oid_t * elementCounter = (oid_t *) this->currentChunk->content;

            this->currentPosition = index - this->recordsPerBucket * (index / this->recordsPerBucket);

            if (this->currentPosition >= *elementCounter) {
                // Problem : Falscher Index
                rewind();
                return Record(nullptr);
            } else {
                Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + this->currentPosition * ahead::get<bytes_t>(this->columnMetaData.width));
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
            return Record(reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + (this->currentPosition - 1) * ahead::get<bytes_t>(this->columnMetaData.width));
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

        Record rec(reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + this->currentPosition * ahead::get<bytes_t>(this->columnMetaData.width));
        this->currentPosition++;
        (*elementCounter)++;
        return rec;
    }

    size_t ColumnManager::ColumnIterator::read(
            std::istream & inStream) {
        return read0<false>(inStream, 1);
    }

    size_t ColumnManager::ColumnIterator::read(
            std::istream & inStream,
            A_t newA) {
        return read0<true>(inStream, newA);
    }

#pragma GCC push_options
#pragma GCC optimize ("2")
    // DO NOT OPTIMIZE THIS FUNCTION WITH -O3 !!! At least GCC (c++) 7.1.0 produces invalid code!
    template<bool reencode>
    size_t ColumnManager::ColumnIterator::read0(
            std::istream & inStream,
            A_t newA) {
        const size_t pos = inStream.tellg();
        inStream.seekg(0, std::ios_base::end);
        const size_t numBytesTotal = static_cast<size_t>(inStream.tellg()) - pos;
        inStream.seekg(pos, std::ios_base::beg);
        size_t numTotalValues = numBytesTotal / ahead::get<bytes_t>(this->columnMetaData.width);
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
        ssize_t tmpNumTotalValues = static_cast<ssize_t>(numTotalValues);
        while (tmpNumTotalValues > 0) {
            this->currentPosition = *elementCounter;
            size_t numValuesToInsert = this->recordsPerBucket - this->currentPosition;
            if (numValuesToInsert > static_cast<size_t>(tmpNumTotalValues)) {
                numValuesToInsert = tmpNumTotalValues;
            }
            const auto widthBytes = ahead::get<bytes_t>(this->columnMetaData.width);
            size_t numBytesToInsert = numValuesToInsert * widthBytes;
            *elementCounter = this->currentPosition + numValuesToInsert;
            char * pDest = reinterpret_cast<char*>(this->currentChunk->content) + sizeof(oid_t) + this->currentPosition * widthBytes;
            inStream.read(pDest, numBytesToInsert);
            if (reencode) {
                const auto widthBits = ahead::get<bits_t>(this->columnMetaData.width);
                if (widthBits == 16) {
                    uint16_t reencFactor = static_cast<uint16_t>(ext_euclidean(uint32_t(this->columnMetaData.AN_A), 16)) * newA;
                    auto pNum = reinterpret_cast<uint16_t*>(pDest);
                    for (size_t i = 0; i < numValuesToInsert; ++i) {
                        *pNum++ *= reencFactor;
                    }
                } else if (widthBits == 32) {
                    uint32_t reencFactor = static_cast<uint32_t>(ext_euclidean(size_t(this->columnMetaData.AN_A), 32)) * newA;
                    auto pNum = reinterpret_cast<uint32_t*>(pDest);
                    for (size_t i = 0; i < numValuesToInsert; ++i) {
                        *pNum++ *= reencFactor;
                    }
                } else if (widthBits == 64) {
                    uint64_t reencFactor = v2convert<uint64_t>(ext_euclidean(uint128_t(this->columnMetaData.AN_A), 64)) * newA;
                    auto pNum = reinterpret_cast<uint64_t*>(pDest);
                    for (size_t i = 0; i < numValuesToInsert; ++i) {
                        *pNum++ *= reencFactor;
                    }
                } else {
                    std::stringstream ss;
                    ss << "ColumnManager::ColumnIterator::read0(" << __FILE__ << "@" << __LINE__ << ") : column width of " << widthBits << " bits not supported!";
                    throw std::runtime_error(ss.str());
                }
            }
            tmpNumTotalValues -= numValuesToInsert;
            if (tmpNumTotalValues > 0) {
                this->currentChunk = this->iterator->append();
                elementCounter = static_cast<oid_t *>(this->currentChunk->content);
            }
        }

        // update this Iterator's and the ColumnManager's columnMetaData!
        if (reencode) {
            this->columnMetaData.AN_A = newA;
            auto & cmdRef = ColumnManager::getInstance()->getColumnMetaDataRef(columnId);
            cmdRef.AN_A = newA;
            const auto widthBits = ahead::get<bits_t>(this->columnMetaData.width);
            if (widthBits == 16) {
                auto newAinv = ext_euclidean(uint32_t(cmdRef.AN_A), 16);
                this->columnMetaData.AN_Ainv = newAinv;
                cmdRef.AN_Ainv = newAinv;
            } else if (widthBits == 32) {
                auto newAinv = ext_euclidean(size_t(cmdRef.AN_A), 32);
                this->columnMetaData.AN_Ainv = newAinv;
                cmdRef.AN_Ainv = newAinv;
            } else if (widthBits == 64) {
                auto newAinv = v2convert<uint64_t>(ext_euclidean(uint128_t(cmdRef.AN_A), 64));
                this->columnMetaData.AN_Ainv = newAinv;
                cmdRef.AN_Ainv = newAinv;
            }
        }

        return numTotalValues;
    }
#pragma GCC pop_options

    void ColumnManager::ColumnIterator::write(
            std::ostream & outStream) {
        this->rewind();
        auto pChunk = this->iterator->next();
        while (pChunk != nullptr) {
            auto numValues = *static_cast<oid_t*>(pChunk->content);
            char * pStart = reinterpret_cast<char*>(reinterpret_cast<oid_t*>(pChunk->content) + 1);
            outStream.write(pStart, numValues * ahead::get<bytes_t>(this->columnMetaData.width));
            pChunk = this->iterator->next();
        }
    }

    void ColumnManager::ColumnIterator::undo() {
        this->iterator->undo();
        this->currentChunk = 0;
        this->currentPosition = 0;
    }

    ColumnManager::ColumnIterator::ColumnIterator(
            id_t columnId,
            ColumnMetaData & columnMetaData,
            BucketManager::BucketIterator *iterator)
            : columnId(columnId),
              iterator(iterator),
              columnMetaData(columnMetaData),
              currentChunk(nullptr),
              currentPosition(0),
              recordsPerBucket(static_cast<oid_t>((CHUNK_CONTENT_SIZE - sizeof(oid_t)) / ahead::get<bytes_t>(this->columnMetaData.width))) {
        this->iterator->rewind();
    }

    ColumnManager::ColumnIterator::ColumnIterator(
            const ColumnIterator & copy)
            : columnId(copy.columnId),
              iterator(new BucketManager::BucketIterator(*copy.iterator)),
              columnMetaData(copy.columnMetaData),
              currentChunk(copy.currentChunk),
              currentPosition(copy.currentPosition),
              recordsPerBucket(copy.recordsPerBucket) {
    }

    ColumnManager::ColumnIterator::~ColumnIterator() {
        delete iterator;
    }

    ColumnManager::ColumnIterator& ColumnManager::ColumnIterator::operator=(
            const ColumnManager::ColumnIterator &copy) {
        new (this) ColumnIterator(copy);
        return *this;
    }

}

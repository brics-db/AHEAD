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

#include <column_storage/BucketManager.h>

namespace ahead {

    BucketManager* BucketManager::instance = nullptr;

    BucketManager*
    BucketManager::getInstance() {
        if (BucketManager::instance == nullptr) {
            BucketManager::instance = new BucketManager();
        }

        return BucketManager::instance;
    }

    void BucketManager::destroyInstance() {
        auto current = BucketManager::instance;
        if (current) {
            BucketManager::instance = nullptr;
            delete current;
        }
    }

    BucketManager::Chunk::Chunk()
            : content(nullptr) {
    }

    BucketManager::Chunk::Chunk(
            void* content)
            : content(content) {
    }

    BucketManager::Chunk::Chunk(
            const Chunk &copy)
            : content(copy.content) {
    }

    BucketManager::Chunk::~Chunk() {
        if (this->content) {
            free(this->content);
        }
        this->content = nullptr;
    }

    BucketManager::Chunk& BucketManager::Chunk::operator=(
            const Chunk &copy) {
        new (this) Chunk(copy);
        return *this;
    }

    BucketManager::Bucket::Bucket()
            : number(0),
              version(nullptr),
              next(nullptr),
              older(nullptr),
              newer(nullptr),
              chunk(nullptr) {
    }

    BucketManager::Bucket::Bucket(
            id_t number,
            std::shared_ptr<version_t> & version,
            Bucket * next,
            Bucket * older,
            Bucket * newer,
            Chunk * chunk)
            : number(number),
              version(version),
              next(next),
              older(older),
              newer(newer),
              chunk(chunk) {
    }

    BucketManager::Bucket::Bucket(
            const Bucket &copy)
            : number(copy.number),
              version(copy.version),
              next(copy.next),
              older(copy.older),
              newer(copy.newer),
              chunk(copy.chunk) {
    }

    BucketManager::Bucket::~Bucket() {
        if (this->chunk) {
            delete this->chunk;
        }
        this->chunk = nullptr;
        this->newer = nullptr;
        this->older = nullptr;
        this->next = nullptr;
        this->version = nullptr;
    }

    BucketManager::Bucket& BucketManager::Bucket::operator=(
            const Bucket &copy) {
        new (this) Bucket(copy);
        return *this;
    }

    BucketManager::BucketStream::BucketStream()
            : head(nullptr),
              tail(nullptr),
              index(),
              size(0) {
    }

    BucketManager::BucketStream::BucketStream(
            Bucket *head,
            Bucket *tail,
            std::vector<Bucket*> & index,
            size_t size)
            : head(head),
              tail(tail),
              index(index),
              size(size) {
    }

    BucketManager::BucketStream::BucketStream(
            const BucketStream &copy)
            : head(copy.head),
              tail(copy.tail),
              index(copy.index),
              size(copy.size) {
    }

    BucketManager::BucketStream::~BucketStream() {
        auto bucket = head;
        size_t numBuckets = 0;
        while (bucket) {
            ++numBuckets;
            auto bucket2 = bucket->older;
            while (bucket2) {
                ++numBuckets;
                bucket2 = bucket2->older;
            }
            bucket2 = bucket->newer;
            while (bucket2) {
                ++numBuckets;
                bucket2 = bucket2->newer;
            }
            bucket = bucket->next;
        }
        std::unordered_set<Bucket*> bucketAdresses;
        while (bucket) {
            auto bucket2 = bucket->older;
            while (bucket2) {
                auto bucketTmp = bucket2->older;
                bucketAdresses.insert(bucket2);
                delete bucket2;
                bucket2 = bucketTmp;
            }
            bucket2 = bucket->newer;
            while (bucket2) {
                auto bucketTmp = bucket2->newer;
                bucketAdresses.insert(bucket2);
                delete bucket2;
                bucket2 = bucketTmp;
            }
            bucket2 = bucket->next;
            bucketAdresses.insert(bucket);
            delete bucket;
            bucket = bucket2;
        }
        for (auto pBucket : index) {
            auto iter = bucketAdresses.find(pBucket);
            if (iter == bucketAdresses.end()) {
                delete pBucket;
            }
        }
        this->index.clear();
        this->head = nullptr;
        this->tail = nullptr;
    }

    BucketManager::BucketStream& BucketManager::BucketStream::operator=(
            const BucketStream &copy) {
        new (this) BucketStream(copy);
        return *this;
    }

    BucketManager::BucketManager()
            : streams() {
    }

    BucketManager::~BucketManager() {
        this->streams.clear();
    }

    BucketManager::BucketIterator*
    BucketManager::openStream(
            id_t id,
            std::shared_ptr<version_t> & version) {
        if (this->streams.find(id) == this->streams.end()) {
            this->streams[id].head = nullptr;
            this->streams[id].tail = nullptr;
            this->streams[id].size = 0;
            this->streams[id].index.clear();
        }

        return new BucketManager::BucketIterator(&this->streams[id], version);
    }

#ifdef DEBUG

    void
    BucketManager::printDebugInformation () {
        std::map<unsigned int, BucketManager::BucketStream>::iterator it;

        for (it = this->streams.begin(); it != this->streams.end(); it++) {
            std::cout << "BucketStream-Number: " << it->first << "\n";
            BucketManager::BucketIterator::printBucketStream(&it->second);
        }
    }
#endif

    BucketManager::BucketIterator::BucketIterator(
            BucketManager::BucketStream * stream,
            std::shared_ptr<version_t> & version)
            : stream(stream),
              version(version),
              currentBucket(nullptr),
              previousBucket(nullptr),
              log() {
        if (stream == nullptr || version == nullptr) {
            // Problem : Falsche Parameter
            std::stringstream ss;
            ss << "BucketIterator::BucketIterator(stream, version):";
            if (stream == nullptr)
                ss << " stream is null.";
            if (version == nullptr)
                ss << " version is null.";
            throw std::runtime_error(ss.str());
        }
    }

    BucketManager::BucketIterator::BucketIterator(
            const BucketIterator & copy)
            : stream(copy.stream),
              version(copy.version),
              currentBucket(copy.currentBucket),
              previousBucket(copy.previousBucket),
              log() {
    }

    BucketManager::BucketIterator::~BucketIterator() {
        this->stream = nullptr;
        this->version = nullptr;
        this->currentBucket = nullptr;
        this->previousBucket = nullptr;
    }

    BucketManager::BucketIterator& BucketManager::BucketIterator::operator=(
            const BucketIterator& copy) {
        new (this) BucketIterator(copy);
        return *this;
    }

    size_t BucketManager::BucketIterator::countBuckets() {
        size_t position;
        BucketManager::Bucket *bucket;

        if (this->stream->size > 0) {
            position = this->stream->size - 1;

            do {
                bucket = this->stream->index[position];

                while (bucket && bucket->version && (*bucket->version > *this->version)) {
                    bucket = bucket->older;
                }

                if (bucket) {
                    return position + 1;
                }
            } while (position-- > 0);
        }

        return 0;
    }

    size_t BucketManager::BucketIterator::position() {
        if (this->previousBucket == nullptr && this->currentBucket == nullptr) {
            return 0;
        } else if (this->previousBucket == nullptr) {
            return 1;
        } else if (this->currentBucket == nullptr) {
            return countBuckets();
        } else {
            return this->currentBucket->number + 1;
        }
    }

    void BucketManager::BucketIterator::rewind() {
        this->previousBucket = nullptr;
        this->currentBucket = nullptr;
    }

    BucketManager::Chunk*
    BucketManager::BucketIterator::next() {
        if (this->previousBucket == nullptr && this->currentBucket == nullptr) {
            this->currentBucket = this->stream->head;
        } else if (this->currentBucket != nullptr) {
            this->previousBucket = this->currentBucket;
            this->currentBucket = this->currentBucket->next;
        }

        while (this->currentBucket != nullptr && *this->currentBucket->version > *this->version) {
            this->currentBucket = this->currentBucket->older;
        }

        if (this->currentBucket == 0) {
            // Problem : Ende des Streams
            return nullptr;
        }

        return this->currentBucket->chunk;
    }

    BucketManager::Chunk*
    BucketManager::BucketIterator::seek(
            size_t number) {
        if (number < this->stream->size) {
            if (number == 0) {
                this->previousBucket = nullptr;
                this->currentBucket = nullptr;

                return this->next();
            } else {
                this->previousBucket = this->stream->index[number - 1];

                while (this->previousBucket != nullptr && *this->previousBucket->version > *this->version) {
                    this->previousBucket = this->previousBucket->older;
                }

                if (this->previousBucket == nullptr) {
                    // Problem : Ende des Streams
                    this->currentBucket = nullptr;
                    return 0;
                } else {
                    this->currentBucket = this->previousBucket->next;

                    while (this->currentBucket != nullptr && *this->currentBucket->version > *this->version) {
                        this->currentBucket = this->currentBucket->older;
                    }

                    if (this->currentBucket == nullptr) {
                        // Problem : Ende des Streams
                        this->previousBucket = nullptr;
                        return nullptr;
                    } else {
                        return this->currentBucket->chunk;
                    }
                }
            }

        } else {
            // Problem : Ende des Streams
            this->previousBucket = nullptr;
            this->currentBucket = nullptr;
            return nullptr;
        }
    }

    BucketManager::Chunk*
    BucketManager::BucketIterator::edit() {
        if (this->currentBucket == nullptr) {
            // Problem : Anfang/Ende des Streams
            return nullptr;
        } else {
            if (this->currentBucket->newer != nullptr) {
                // Problem : Existenz aktuellerer Version
                return nullptr;
            } else {
                if (*this->currentBucket->version != *this->version) {
                    auto newBucket = new BucketManager::Bucket(this->currentBucket->number, this->version, this->currentBucket->next, this->currentBucket, nullptr, new BucketManager::Chunk);

                    newBucket->chunk->content = malloc(CHUNK_CONTENT_SIZE);
                    memcpy(newBucket->chunk->content, this->currentBucket->chunk->content, CHUNK_CONTENT_SIZE);

                    if (this->currentBucket == this->stream->head) {
                        this->stream->head = newBucket;
                    } else {
                        this->previousBucket->next = newBucket;
                    }

                    this->currentBucket->newer = newBucket;

                    this->stream->index[this->currentBucket->number] = newBucket;

                    if (this->currentBucket == this->stream->tail) {
                        this->stream->tail = newBucket;
                    }

                    this->currentBucket = newBucket;

                    this->log.push(this->previousBucket);
                }

                return this->currentBucket->chunk;
            }
        }
    }

    BucketManager::Chunk*
    BucketManager::BucketIterator::append() {
        auto newBucket = new BucketManager::Bucket(this->stream->size, this->version, nullptr, nullptr, nullptr, new BucketManager::Chunk);

        newBucket->chunk->content = malloc(CHUNK_CONTENT_SIZE);
        *static_cast<oid_t*>(newBucket->chunk->content) = 0;

        this->log.push(this->stream->tail);

        this->previousBucket = this->stream->tail;
        this->currentBucket = newBucket;

        if (this->stream->tail == nullptr) {
            this->stream->head = newBucket;
            this->stream->tail = newBucket;
        } else {
            this->stream->tail->next = newBucket;
            this->stream->tail = newBucket;
        }

        this->stream->index.push_back(newBucket);
        this->stream->size++;

        return newBucket->chunk;
    }

    void BucketManager::BucketIterator::undo() {
        while (!this->log.empty()) {
            this->previousBucket = this->log.top();
            this->log.pop();

            // Atomic Block Start
            if (this->previousBucket == nullptr) {
                this->currentBucket = this->stream->head;

                if (this->currentBucket->older != nullptr) {
                    this->currentBucket->older->newer = nullptr;
                }

                this->stream->head = this->currentBucket->older;
                this->stream->index[this->currentBucket->number] = this->currentBucket->older;
            } else {
                this->currentBucket = this->previousBucket->next;

                if (this->currentBucket->older != nullptr) {
                    this->currentBucket->older->newer = nullptr;
                }

                this->previousBucket->next = this->currentBucket->older;
                this->stream->index[this->currentBucket->number] = this->currentBucket->older;
            }

            if (this->stream->tail == this->currentBucket) {
                if (this->currentBucket->older == nullptr) {
                    this->stream->tail = this->previousBucket;
                    this->stream->index.resize(this->stream->size);
                    this->stream->size--;
                } else {
                    this->stream->tail = this->currentBucket->older;
                }
            }
            // Atomic Block Ende

            free(this->currentBucket->chunk->content);
            delete this->currentBucket->chunk;
            delete this->currentBucket;
        }

        this->previousBucket = nullptr;
        this->currentBucket = nullptr;
    }

#ifdef DEBUG

void
BucketManager::BucketIterator::printBucket (BucketManager::Bucket *bucket) {
    if (bucket != 0) {
        std::cout << "Number: " << bucket->number << "\n";
        std::cout << "Memory-Position: " << bucket << "\n";
        std::cout << "Version: " << *bucket->version << "\n";
        std::cout << "Next Bucket: " << bucket->next << "\n";
        std::cout << "Older Bucket: " << bucket->older << "\n";
        std::cout << "Newer Bucket: " << bucket->newer << "\n";
        std::cout << "Content: " << *((int*)bucket->chunk->content) << "\n";
    }
}

void
BucketManager::BucketIterator::printBucketStream (BucketManager::BucketStream *stream) {
    BucketManager::Bucket *bucket;

    if (stream != 0) {
        std::cout << "Size: " << stream->size << "\n";
        std::cout << "Head: " << stream->head << "\n";
        std::cout << "Tail: " << stream->tail << "\n";

        for (unsigned int number = 0; number < stream->size; number++) {
            std::cout << "\n";
            std::cout << "Index: " << number << "<->" << stream->index[number] << "\n";

            bucket = stream->index[number];

            while (bucket != 0) {
                std::cout << "\n";
                printBucket(bucket);
                bucket = bucket->older;
            }
        }
    }
}

void
BucketManager::BucketIterator::printDebugInformation () {
    std::cout << "Version: " << *this->version << " (" << this->version << ")" << "\n";

    std::cout << "Current Bucket: " << this->currentBucket << "\n";
    std::cout << "Previous Bucket: " << this->previousBucket << "\n";

    printBucketStream(this->stream);
}

#endif

}

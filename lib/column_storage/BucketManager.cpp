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
        if (BucketManager::instance) {
            delete BucketManager::instance;
            BucketManager::instance = nullptr;
        }
    }

    BucketManager::Chunk::Chunk()
            : content(nullptr) {
    }

    BucketManager::Chunk::Chunk(
            void* content)
            : content(content) {
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
            version_t *version,
            Bucket *next,
            Bucket *older,
            Bucket* newer,
            Chunk *chunk)
            : number(number),
              version(version),
              next(next),
              older(older),
              newer(newer),
              chunk(chunk) {
    }

    BucketManager::Bucket::~Bucket() {
        delete this->chunk;
        if (this->older) {
            if (this->older->newer == this) {
                this->older->newer = nullptr;
                delete this->older;
            } else {
                // ERROR: something weird is going on
            }
        }
        if (this->newer) {
            if (this->newer->older == this) {
                this->newer->older = nullptr;
                delete this->newer->older;
            } else {
                // ERROR: something weird is going on
            }
        }
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

    BucketManager::BucketManager()
            : streams() {
    }

    BucketManager::~BucketManager() {
        for (auto & mypair : streams) {
            for (auto & pBucket : mypair.second.index) {
                delete pBucket;
            }
        }
    }

    BucketManager::BucketIterator*
    BucketManager::openStream(
            id_t id,
            version_t *version) {
        if (this->streams.find(id) == this->streams.end()) {
            this->streams[id].head = 0;
            this->streams[id].tail = 0;
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
            BucketManager::BucketStream *stream,
            version_t *version)
            : stream(stream),
              version(version),
              currentBucket(0),
              previousBucket(0),
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

                while (bucket != 0 && *bucket->version > *this->version) {
                    bucket = bucket->older;
                }

                if (bucket != 0) {
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
        BucketManager::Bucket *newBucket;

        if (this->currentBucket == nullptr) {
            // Problem : Anfang/Ende des Streams
            return nullptr;
        } else {
            if (this->currentBucket->newer != nullptr) {
                // Problem : Existenz aktuellerer Version
                return nullptr;
            } else {
                if (this->currentBucket->version != this->version) {
                    newBucket = new BucketManager::Bucket;

                    newBucket->next = this->currentBucket->next;
                    newBucket->older = this->currentBucket;
                    newBucket->newer = nullptr;

                    newBucket->version = this->version;
                    newBucket->number = this->currentBucket->number;

                    newBucket->chunk = new BucketManager::Chunk;

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
        BucketManager::Bucket *newBucket;

        newBucket = new BucketManager::Bucket;

        newBucket->next = nullptr;
        newBucket->older = nullptr;
        newBucket->newer = nullptr;

        newBucket->version = this->version;
        newBucket->number = this->stream->size;

        newBucket->chunk = new BucketManager::Chunk;

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

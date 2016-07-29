#include "column_storage/BucketManager.h"

BucketManager* BucketManager::instance = 0;

BucketManager* BucketManager::getInstance() {
    if (BucketManager::instance == 0) {
        BucketManager::instance = new BucketManager();
    }

    return BucketManager::instance;
}

BucketManager::BucketIterator* BucketManager::openStream(unsigned int id, unsigned int *version) {
    if (this->streams.find(id) == this->streams.end()) {
        this->streams[id].head = 0;
        this->streams[id].tail = 0;
        this->streams[id].size = 0;
        this->streams[id].index.clear();
    }

    return new BucketManager::BucketIterator(&this->streams[id], version);
}

#ifdef DEBUG

void BucketManager::printDebugInformation() {
    std::map<unsigned int, BucketManager::BucketStream>::iterator it;

    for (it = this->streams.begin(); it != this->streams.end(); it++) {
        std::cout << "BucketStream-Number: " << it->first << "\n";
        BucketManager::BucketIterator::printBucketStream(&it->second);
    }
}
#endif

BucketManager::BucketManager() {
}

BucketManager::BucketIterator::BucketIterator(BucketManager::BucketStream *stream, unsigned int *version) {
    if (stream != 0 && version != 0) {
        this->stream = stream;
        this->version = version;
        this->currentBucket = 0;
        this->previousBucket = 0;
    } else {
        // Problem : Falsche Parameter
    }
}

unsigned int BucketManager::BucketIterator::size() {
    unsigned int position;
    BucketManager::Bucket *bucket;

    if (this->stream->size > 0) {
        position = this->stream->size - 1;

        do {
            bucket = this->stream->index[position];

            while (bucket != 0 && *bucket->version > * this->version) {
                bucket = bucket->older;
            }

            if (bucket != 0) {
                return position + 1;
            }
        } while (position-- > 0);
    }

    return 0;
}

unsigned int BucketManager::BucketIterator::position() {
    if (this->previousBucket == 0 && this->currentBucket == 0) {
        return 0;
    } else if (this->previousBucket == 0) {
        return 1;
    } else if (this->currentBucket == 0) {
        return size();
    } else {
        return this->currentBucket->number + 1;
    }
}

void BucketManager::BucketIterator::rewind() {
    this->previousBucket = 0;
    this->currentBucket = 0;
}

BucketManager::Chunk* BucketManager::BucketIterator::next() {
    if (this->previousBucket == 0 && this->currentBucket == 0) {
        this->currentBucket = this->stream->head;
    } else if (this->currentBucket != 0) {
        this->previousBucket = this->currentBucket;
        this->currentBucket = this->currentBucket->next;
    }

    while (this->currentBucket != 0 && *this->currentBucket->version > * this->version) {
        this->currentBucket = this->currentBucket->older;
    }

    if (this->currentBucket == 0) {
        // Problem : Ende des Streams
        return 0;
    }

    return this->currentBucket->chunk;
}

BucketManager::Chunk* BucketManager::BucketIterator::seek(unsigned int number) {
    if (number < this->stream->size) {
        if (number == 0) {
            this->previousBucket = 0;
            this->currentBucket = 0;

            return this->next();
        } else {
            this->previousBucket = this->stream->index[number - 1];

            while (this->previousBucket != 0 && *this->previousBucket->version > * this->version) {
                this->previousBucket = this->previousBucket->older;
            }

            if (this->previousBucket == 0) {
                // Problem : Ende des Streams
                this->currentBucket = 0;
                return 0;
            } else {
                this->currentBucket = this->previousBucket->next;

                while (this->currentBucket != 0 && *this->currentBucket->version > * this->version) {
                    this->currentBucket = this->currentBucket->older;
                }

                if (this->currentBucket == 0) {
                    // Problem : Ende des Streams
                    this->previousBucket = 0;
                    return 0;
                } else {
                    return this->currentBucket->chunk;
                }
            }
        }

    } else {
        // Problem : Ende des Streams
        this->previousBucket = 0;
        this->currentBucket = 0;
        return 0;
    }
}

BucketManager::Chunk* BucketManager::BucketIterator::edit() {
    BucketManager::Bucket *newBucket;

    if (this->currentBucket == 0) {
        // Problem : Anfang/Ende des Streams
        return 0;
    } else {
        if (this->currentBucket->newer != 0) {
            // Problem : Existenz aktuellerer Version
            return 0;
        } else {
            if (this->currentBucket->version != this->version) {
                newBucket = new BucketManager::Bucket;

                newBucket->next = this->currentBucket->next;
                newBucket->older = this->currentBucket;
                newBucket->newer = 0;

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

BucketManager::Chunk* BucketManager::BucketIterator::append() {
    BucketManager::Bucket *newBucket;

    newBucket = new BucketManager::Bucket;

    newBucket->next = 0;
    newBucket->older = 0;
    newBucket->newer = 0;

    newBucket->version = this->version;
    newBucket->number = this->stream->size;

    newBucket->chunk = new BucketManager::Chunk;

    newBucket->chunk->content = malloc(CHUNK_CONTENT_SIZE);

    this->log.push(this->stream->tail);

    this->previousBucket = this->stream->tail;
    this->currentBucket = newBucket;

    if (this->stream->tail == 0) {
        this->stream->head = newBucket;
        this->stream->tail = newBucket;
    } else {
        this->stream->tail->next = newBucket;
        this->stream->tail = newBucket;
    }

    this->stream->index.resize(this->stream->size + 1, 0);
    this->stream->index[this->stream->size] = newBucket;
    this->stream->size++;

    return newBucket->chunk;
}

void BucketManager::BucketIterator::undo() {
    while (!this->log.empty()) {
        this->previousBucket = this->log.top();
        this->log.pop();

        // Atomic Block Start
        if (this->previousBucket == 0) {
            this->currentBucket = this->stream->head;

            if (this->currentBucket->older != 0) {
                this->currentBucket->older->newer = 0;
            }

            this->stream->head = this->currentBucket->older;
            this->stream->index[this->currentBucket->number] = this->currentBucket->older;
        } else {
            this->currentBucket = this->previousBucket->next;

            if (this->currentBucket->older != 0) {
                this->currentBucket->older->newer = 0;
            }

            this->previousBucket->next = this->currentBucket->older;
            this->stream->index[this->currentBucket->number] = this->currentBucket->older;
        }

        if (this->stream->tail == this->currentBucket) {
            if (this->currentBucket->older == 0) {
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

    this->previousBucket = 0;
    this->currentBucket = 0;
}

#ifdef DEBUG

void BucketManager::BucketIterator::printBucket(BucketManager::Bucket *bucket) {
    if (bucket != 0) {
        std::cout << "Number: " << bucket->number << "\n";
        std::cout << "Memory-Position: " << bucket << "\n";
        std::cout << "Version: " << *bucket->version << "\n";
        std::cout << "Next Bucket: " << bucket->next << "\n";
        std::cout << "Older Bucket: " << bucket->older << "\n";
        std::cout << "Newer Bucket: " << bucket->newer << "\n";
        std::cout << "Content: " << *((int*) bucket->chunk->content) << "\n";
    }
}

void BucketManager::BucketIterator::printBucketStream(BucketManager::BucketStream *stream) {
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

void BucketManager::BucketIterator::printDebugInformation() {
    std::cout << "Version: " << *this->version << " (" << this->version << ")" << "\n";

    std::cout << "Current Bucket: " << this->currentBucket << "\n";
    std::cout << "Previous Bucket: " << this->previousBucket << "\n";

    printBucketStream(this->stream);
}

#endif

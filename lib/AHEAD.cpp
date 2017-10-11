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

/* 
 * File:   AHEAD.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 04-04-2017 10:50
 */

#include <cassert>
#include <memory>

#include <AHEAD.hpp>
#include <column_storage/Storage.hpp>
#include <column_operators/Operators.hpp>
#include <column_storage/TransactionManager.h>
#include "meta_repository/MetaRepositoryManager.h"

namespace ahead {
    std::shared_ptr<AHEAD> AHEAD::instance;

    AHEAD::AHEAD(
            int argc,
            const char * const * argv)
            : config(argc, argv),
              mgrMeta(MetaRepositoryManager::getInstance()),
              mgrTx(TransactionManager::getInstance()) {
        mgrMeta->init(config);
    }

    AHEAD::AHEAD(
            const AHEAD & other)
            : config(other.config),
              mgrMeta(other.mgrMeta),
              mgrTx(other.mgrTx) {
    }

    AHEAD::~AHEAD() {
        TransactionManager::destroyInstance();
        BucketManager::destroyInstance();
        ColumnManager::destroyInstance();
        MetaRepositoryManager::destroyInstance();
        mgrMeta = nullptr;
        mgrTx = nullptr;
    }

    AHEAD & AHEAD::operator=(
            const AHEAD & other) {
        this->~AHEAD();
        new (this) AHEAD(other);
        return *this;
    }

    std::shared_ptr<AHEAD> AHEAD::getInstance() {
        return instance;
    }

    std::shared_ptr<AHEAD> AHEAD::createInstance(
            int argc,
            const char * const * argv) {
        AHEAD::instance.reset(new AHEAD(argc, argv));
        return AHEAD::instance;
    }

    const AHEAD_Config & AHEAD::getConfig() {
        return config;
    }

    void AHEAD::destroyInstance() {
        if (AHEAD::instance) {
            AHEAD::instance.reset();
        }
    }

    size_t AHEAD::loadTable(
            const std::string & tableName,
            const char * const prefix,
            const size_t size,
            const char * const delim,
            const bool ignoreMoreData) {
        TransactionManager::Transaction* t = mgrTx->beginTransaction(true);
        assert(t != nullptr);
        std::string path(mgrMeta->strBaseDir);
        path += "/";
        path += tableName;
        size_t numBUNs = t->load(path.c_str(), tableName.c_str(), prefix, size, delim, ignoreMoreData);
        mgrTx->endTransaction(t);
        return numBUNs;
    }

    size_t AHEAD::loadTable(
            const char * const tableName,
            const char * const prefix,
            const size_t size,
            const char * const delim,
            const bool ignoreMoreData) {
        TransactionManager::Transaction* t = mgrTx->beginTransaction(true);
        assert(t != nullptr);
        std::string path(mgrMeta->strBaseDir);
        path += "/";
        path += tableName;
        size_t numBUNs = t->load(path.c_str(), tableName, prefix, size, delim, ignoreMoreData);
        mgrTx->endTransaction(t);
        return numBUNs;
    }
}

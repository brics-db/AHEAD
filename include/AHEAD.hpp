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
 * File:   AHEAD.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 04-04-2017 10:15
 */
#ifndef AHEAD_HPP_
#define AHEAD_HPP_

#include <string>
#include <memory>

#include <ColumnStore.h>
#include <column_storage/Storage.hpp>
#include <column_operators/Operators.hpp>

namespace ahead {

    class AHEAD {
        static std::shared_ptr<AHEAD> instance;

        std::shared_ptr<MetaRepositoryManager> mgrMeta;
        std::shared_ptr<TransactionManager> mgrTx;

        bool doConvertTableFilesOnLoad;

        AHEAD(
                const std::string& strBaseDir);
        AHEAD(
                const AHEAD & other);

    public:
        virtual ~AHEAD();

        AHEAD & operator=(
                const AHEAD & other);

        /**
         * Unconditionally returns the instance pointer. Client code must assure that createInstance()
         * is called at least once before getInstance().
         */
        static std::shared_ptr<AHEAD> getInstance();

        /**
         * Creates a new instance of AHEAD column store.
         * Arguments:
         *  * strBaseDir: path to the database files
         */
        static std::shared_ptr<AHEAD> createInstance(
                const std::string & strBaseDir);

        size_t loadTable(
                const std::string & tableName,
                const char * const prefix = nullptr,
                const size_t size = static_cast<size_t>(-1),
                const char * const delim = nullptr,
                const bool ignoreMoreData = true);

        size_t loadTable(
                const char * const tableName,
                const char * const prefix = nullptr,
                const size_t size = static_cast<size_t>(-1),
                const char * const delim = nullptr,
                const bool ignoreMoreData = true);

        bool isConvertTableFilesOnLoad();

        void setConverttableFilesOnLoad(
                bool newValue);
    };

}

#endif /* AHEAD_HPP_ */

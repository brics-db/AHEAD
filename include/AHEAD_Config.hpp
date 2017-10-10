// Copyright (c) 2017 Till Kolditz
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
 * AHEAD_Config.hpp
 *
 *  Created on: 04.10.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#pragma once

#include <cstddef>

#include <ColumnStore.h>
#include <util/argumentparser.hpp>

namespace ahead {

    class AHEAD_Config {
        friend class AHEAD;

        typedef typename ArgumentParser::alias_list_t alias_list_t;

        ArgumentParser parser;
        size_t ANCODING_MINIMUM_BIT_FLIP_WEIGHT;
        std::string DB_PATH;
        bool CONVERT_TABLE_FILES;
        bool VERBOSE;

        AHEAD_Config(
                int argc,
                const char * const * argv);

        AHEAD_Config(
                const AHEAD_Config & other);

        static const constexpr char * const ID_CONVERTTABLEFILES = "ctfol";
        static const constexpr char * const ID_VERBOSE = "verbose";
        static const constexpr char * const ID_DBPATH = "dbpath";
        static const constexpr char * const ID_ANCODING_MINBFW = "AN.minbfw";

    public:
        size_t getMinimumBitFlipWeight() const;

        bool isConvertTableFilesOnLoad() const;

        bool isVerbose() const;

        const std::string & getDBPath() const;
    };

}

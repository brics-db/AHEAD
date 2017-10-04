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
 * AHEAD_Config.cpp
 *
 *  Created on: 04.10.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#include <AHEAD_Config.hpp>

namespace ahead {

    const constexpr char * const AHEAD_Config::ID_ANCODING_MINBFW;
    const constexpr char * const AHEAD_Config::ID_DBPATH;
    const constexpr char * const AHEAD_Config::ID_CONVERTTABLEFILES;

    AHEAD_Config::AHEAD_Config(
            int argc,
            const char * const * argv)
            : parser( {std::forward_as_tuple(ID_ANCODING_MINBFW, alias_list_t {"--AN-minbfw"}, 0)}, {std::forward_as_tuple(ID_DBPATH, alias_list_t {"--dbpath", "-d"}, ".")},
                      {std::forward_as_tuple(ID_CONVERTTABLEFILES, alias_list_t {"--convert-table-files", "-c"}, true)}),
              ANCODING_MINIMUM_BIT_FLIP_WEIGHT(0),
              DB_PATH(),
              CONVERT_TABLE_FILES(true) {
        parser.parse(argc, argv);
        ANCODING_MINIMUM_BIT_FLIP_WEIGHT = parser.get_uint(ID_ANCODING_MINBFW);
        DB_PATH = parser.get_str(ID_DBPATH);
        CONVERT_TABLE_FILES = parser.get_bool(ID_CONVERTTABLEFILES);
    }

    AHEAD_Config::AHEAD_Config(
            const AHEAD_Config & other)
            : parser(other.parser),
              ANCODING_MINIMUM_BIT_FLIP_WEIGHT(other.ANCODING_MINIMUM_BIT_FLIP_WEIGHT),
              DB_PATH(other.DB_PATH),
              CONVERT_TABLE_FILES(other.CONVERT_TABLE_FILES) {
    }

    size_t AHEAD_Config::getMinimzmBitFlipWeight() const {
        return parser.get_uint(ID_ANCODING_MINBFW);
    }

    bool AHEAD_Config::isConvertTableFilesOnLoad() const {
        return parser.get_bool(ID_CONVERTTABLEFILES);
    }

    const std::string & AHEAD_Config::getDBPath() const {
        return parser.get_str(ID_DBPATH);
    }

}

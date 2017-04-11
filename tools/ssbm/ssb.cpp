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
 * File:   ssb.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 11:03
 */

#include "ssb.hpp"

constexpr const char * const SSB_CONF::ID_NUMRUNS;
constexpr const char * const SSB_CONF::ID_LENTIMES;
constexpr const char * const SSB_CONF::ID_LENTYPES;
constexpr const char * const SSB_CONF::ID_LENSIZES;
constexpr const char * const SSB_CONF::ID_LENPCM;
constexpr const char * const SSB_CONF::ID_DBPATH;
constexpr const char * const SSB_CONF::ID_VERBOSE;
constexpr const char * const SSB_CONF::ID_PRINTRESULT;
constexpr const char * const SSB_CONF::ID_CONVERTTABLEFILES;

SSB_CONF::SSB_CONF()
        : NUM_RUNS(0), LEN_TIMES(0), LEN_TYPES(0), LEN_SIZES(0), LEN_PCM(0), DB_PATH(), VERBOSE(false), PRINT_RESULT(0), CONVERT_TABLE_FILES(true),
                parser(
                        {std::forward_as_tuple(ID_NUMRUNS, alias_list_t {"--numruns", "-n"}, 15), std::forward_as_tuple(ID_LENTIMES, alias_list_t {"--lentimes"}, 16), std::forward_as_tuple(
                                ID_LENTYPES, alias_list_t {"--lentypes"}, 20), std::forward_as_tuple(ID_LENSIZES, alias_list_t {"--lensizes"}, 16), std::forward_as_tuple(ID_LENPCM, alias_list_t {
                                "--lenpcm"}, 16)}, {std::forward_as_tuple(ID_DBPATH, alias_list_t {"--dbpath", "-d"}, ".")},
                        {std::forward_as_tuple(ID_VERBOSE, alias_list_t {"--verbose", "-v"}, false), std::forward_as_tuple(ID_PRINTRESULT, alias_list_t {"--print-result", "-p"}, false),
                                std::forward_as_tuple(ID_CONVERTTABLEFILES, alias_list_t {"--convert-table-files", "-c"}, true)}) {
}

SSB_CONF::SSB_CONF(int argc, char** argv)
        : SSB_CONF() {
    init(argc, argv);
}

void SSB_CONF::init(int argc, char** argv) {
    parser.parse(argc, argv, 1);
    NUM_RUNS = parser.get_uint(ID_NUMRUNS);
    LEN_TIMES = parser.get_uint(ID_LENTIMES);
    LEN_TYPES = parser.get_uint(ID_LENTYPES);
    LEN_SIZES = parser.get_uint(ID_LENSIZES);
    LEN_PCM = parser.get_uint(ID_LENPCM);
    DB_PATH = parser.get_str(ID_DBPATH);
    VERBOSE = parser.get_bool(ID_VERBOSE);
    PRINT_RESULT = parser.get_bool(ID_PRINTRESULT);
    CONVERT_TABLE_FILES = parser.get_bool(ID_CONVERTTABLEFILES);
}

StopWatch::rep loadTable(const char* const tableName, const SSB_CONF & CONFIG) {
    StopWatch sw;
    sw.start();
    size_t numBUNs = AHEAD::getInstance()->loadTable(tableName);
    sw.stop();
    if (CONFIG.VERBOSE) {
        std::cout << "Table: " << tableName << "\n\tNumber of BUNs: " << numBUNs << "\n\tTime: " << sw << " ns." << std::endl;
    }
    return sw.duration();
}

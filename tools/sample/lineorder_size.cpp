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
 * File:   ssbm-q01.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include <iostream>
#include <iomanip>
#include <type_traits>
#include <utility>

#include "../ssbm/ssb.hpp"

SSB_CONF CONFIG;
const size_t NUM_RUNS = 10;

template<typename Tail>
void runTable(const char* strTable, const char* strTableAN, const char* strColumn) {
    typedef typename TypeMap<Tail>::v2_base_t v2_base_t;
    typedef typename TypeMap<Tail>::v2_encoded_t v2_encoded_t;

    RUNTABLE_REQUIRED_VARIABLES(32);
    MEASURE_OP(batBc, (new ColumnBAT<v2_oid_t, v2_base_t>(strTable, strColumn)));
    MEASURE_OP(batBcAN, (new ColumnBAT<v2_oid_t, v2_encoded_t>(strTableAN, strColumn)));
    MEASURE_OP(batTcAN, ahead::bat::ops::copy(batBcAN));

    std::cout << "\n#runTable(" << strTable << '.' << strColumn << ")";

    std::cout << "\nname\t" << std::setw(CONFIG.LEN_TIMES) << "time [ns]" << '\t' << std::setw(CONFIG.LEN_SIZES) << "size [#]" << '\t' << std::setw(CONFIG.LEN_SIZES) << "consum [B]" << '\t'
            << std::setw(CONFIG.LEN_TYPES) << "type head" << '\t' << std::setw(CONFIG.LEN_TYPES) << "type tail";
    for (size_t i = 0; i < I; ++i) {
        std::cout << "\nop" << std::setw(2) << i << "\t" << std::setw(CONFIG.LEN_TIMES) << hrc_duration(opTimes[i]) << "\t" << std::setw(CONFIG.LEN_SIZES) << batSizes[i] << "\t"
                << std::setw(CONFIG.LEN_SIZES) << batConsumptions[i] << "\t" << std::setw(CONFIG.LEN_TYPES) << headTypes[i].pretty_name() << "\t" << std::setw(CONFIG.LEN_TYPES)
                << (hasTwoTypes[i] ? tailTypes[i].pretty_name() : emptyString);
    }
    std::cout << "\nnum\tcopy-" << TypeMap<v2_encoded_t>::TYPENAME << "\tcheck\tdecode\tcheck+decode\tcopy-" << TypeMap<v2_base_t>::TYPENAME << std::endl;

    for (size_t i = 0; i < NUM_RUNS; ++i) {
        std::cout << std::setw(3) << i << std::flush;

        sw1.start();
        auto result0 = ahead::bat::ops::copy(batTcAN);
        sw1.stop();
        std::cout << '\t' << std::setw(CONFIG.LEN_TIMES) << sw1.duration() << std::flush;
        delete result0;

        sw1.start();
        auto result1 = ahead::bat::ops::checkAN(batTcAN);
        sw1.stop();
        std::cout << '\t' << std::setw(CONFIG.LEN_TIMES) << sw1.duration() << std::flush;
        delete result1;

        sw1.start();
        auto result2 = ahead::bat::ops::decodeAN(batTcAN);
        sw1.stop();
        std::cout << '\t' << std::setw(CONFIG.LEN_TIMES) << sw1.duration() << std::flush;
        delete result2.first;
        delete result2.second;

        sw1.start();
        auto tuple3 = ahead::bat::ops::checkAndDecodeAN(batTcAN);
        if (std::get<1>(tuple3))
            delete std::get<1>(tuple3);
        if (std::get<2>(tuple3))
            delete get<2>(tuple3);
        sw1.stop();
        std::cout << '\t' << std::setw(CONFIG.LEN_TIMES) << sw1.duration() << std::flush;

        sw1.start();
        auto result4 = ahead::bat::ops::copy(std::get<0>(tuple3));
        sw1.stop();
        std::cout << '\t' << std::setw(CONFIG.LEN_TIMES) << sw1.duration() << std::endl;
        delete result4;
        delete std::get<0>(tuple3);
    }
    delete batTcAN;
    delete batBcAN;
    delete batBc;
}

template<typename Tail>
void runTable2(const char* strTable, const char* strTableAN, const char* strColumn) {
    typedef typename TypeMap<Tail>::v2_base_t v2_base_t;
    typedef typename TypeMap<Tail>::v2_encoded_t v2_encoded_t;

    const size_t MAX_SCALE = 10;
    BAT<v2_void_t, v2_encoded_t>* bats[MAX_SCALE];
    auto batBc = new ColumnBAT<v2_base_t>(strTable, strColumn);
    auto batBcAN = new ColumnBAT<v2_encoded_t>(strTableAN, strColumn);

    std::cout << "\n#runTable2(" << strTable << '.' << strColumn << ')';

    std::cout << "\n#sizes";
    size_t szBats = 0;
    for (size_t i = 0; i < MAX_SCALE; ++i) {
        bats[i] = ahead::bat::ops::copy(batBcAN);
        szBats += bats[i]->size();
        std::cout << '\n' << std::setw(2) << i << '\t' << szBats << std::flush;
    }

    std::cout << "\n#NUM_RUNS=" << NUM_RUNS << " (times are averages over as many runs)\nnum\tcheck\tdecode\tcheck+decode" << std::flush;

    for (size_t scale = 1; scale <= MAX_SCALE; ++scale) {
        std::cout << '\n' << std::setw(3) << scale << std::flush;

        StopWatch::rep totalTime = 0;
        for (size_t i = 0; i < NUM_RUNS; ++i) {
            sw1.start();
            for (size_t scale2 = 0; scale2 < scale; ++scale2) {
                auto result = ahead::bat::ops::checkAN(bats[scale2]);
                delete result;
            }
            sw1.stop();
            totalTime += sw1.duration();
        }
        std::cout << '\t' << std::setw(CONFIG.LEN_TIMES) << (totalTime / NUM_RUNS) << std::flush;

        totalTime = 0;
        for (size_t i = 0; i < NUM_RUNS; ++i) {
            sw1.start();
            for (size_t scale2 = 0; scale2 < scale; ++scale2) {
                auto result = ahead::bat::ops::decodeAN(bats[scale2]);
                delete result.first;
                delete result.second;
            }
            sw1.stop();
            totalTime += sw1.duration();
        }
        std::cout << '\t' << std::setw(CONFIG.LEN_TIMES) << (totalTime / NUM_RUNS) << std::flush;

        totalTime = 0;
        for (size_t i = 0; i < NUM_RUNS; ++i) {
            sw1.start();
            for (size_t scale2 = 0; scale2 < scale; ++scale2) {
                auto tuple = ahead::bat::ops::checkAndDecodeAN(bats[scale2]);
                delete get<0>(tuple);
                if (get<1>(tuple))
                    delete get<1>(tuple);
                if (get<2>(tuple))
                    delete get<2>(tuple);
            }
            sw1.stop();
            totalTime += sw1.duration();
        }
        std::cout << '\t' << std::setw(CONFIG.LEN_TIMES) << (totalTime / NUM_RUNS) << std::flush;
    }
    for (size_t i = 0; i < MAX_SCALE; ++i) {
        delete bats[i];
    }
    delete batBcAN;
    delete batBc;
}

int main(int argc, char** argv) {
    CONFIG.init(argc, argv);

    std::cout << "lineorder_size\n==============" << std::endl;

    boost::filesystem::path p(CONFIG.DB_PATH);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    std::string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    loadTable(baseDir, "lineorder", CONFIG);
    loadTable(baseDir, "lineorderAN", CONFIG);

    // Lineorder:
    // orderkey|linenumber|custkey|partkey|suppkey|orderdate|orderpriority|shippriority|quantity|extendedprice|ordertotalprice|discount|revenue|supplycost|tax|commitdate|shipmode
    // INTEGER|SHORTINT|INTEGER|INTEGER|INTEGER|INTEGER|STRING:15|CHAR|TINYINT|INTEGER|INTEGER|TINYINT|INTEGER|INTEGER|TINYINT|INTEGER|STRING:10
    // RESINT|RESSHORT|RESINT|RESINT|RESINT|RESINT|STRING:15|CHAR|RESTINY|RESINT|RESINT|RESTINY|RESINT|RESINT|RESTINY|RESINT|STRING

    // RESINT
    // ORDERKEY
    runTable2<v2_int_t>("lineorder", "lineorderAN", "orderkey");
    runTable<v2_int_t>("lineorder", "lineorderAN", "orderkey");

    // RESSHORT
    // LINENUMBER
    runTable<v2_shortint_t>("lineorder", "lineorderAN", "linenumber");

    // RESTINY
    // QUANTITY
    runTable<v2_tinyint_t>("lineorder", "lineorderAN", "quantity");

    std::cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB." << std::endl;

    return 0;
}

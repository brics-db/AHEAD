// Copyright (c) 2016 Till Kolditz
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
 * File:   ssbm-q11_early.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include "ssbm.hpp"
#include <column_operators/OperatorsAN.hpp>

int
main (int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    std::vector<StopWatch::rep> totalTimes(CONFIG.NUM_RUNS);
    const size_t NUM_OPS = 30;
    cstr_t OP_NAMES[NUM_OPS] = {"-6", "-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P"};
    SSBM_REQUIRED_VARIABLES
    size_t x = 0;
    StopWatch sw1, sw2;
    size_t rssBeforeLoad, rssAfterLoad, rssAfterCopy, rssAfterQueries;

    std::cout << "SSBM Query 1.1 Early Detection\n==============================" << std::endl;

    MetaRepositoryManager::init(CONFIG.DB_PATH.c_str());

    rssBeforeLoad = getPeakRSS(size_enum_t::KB);

    sw1.start();
    // loadTable(CONFIG.DB_PATH, "customerAN", CONFIG);
    loadTable(CONFIG.DB_PATH, "dateAN", CONFIG);
    loadTable(CONFIG.DB_PATH, "lineorderAN", CONFIG);
    // loadTable(CONFIG.DB_PATH, "partAN", CONFIG);
    // loadTable(CONFIG.DB_PATH, "supplierAN", CONFIG);
    sw1.stop();
    std::cout << "Total loading time: " << sw1 << " ns." << std::endl;

    rssAfterLoad = getPeakRSS(size_enum_t::KB);

    if (CONFIG.VERBOSE) {
        std::cout << "SSBM Q1.1:\n";
        std::cout << "select sum(lo_revenue), d_year, p_brand\n";
        std::cout << "  from lineorder, part, supplier, date\n";
        std::cout << "  where lo_orderdate = d_datekey\n";
        std::cout << "    and d_year = 1993\n";
        std::cout << "    and lo_discount between 1 and 3\n";
        std::cout << "    and lo_quantity < 25;" << std::endl;
    }

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(sw1, x, batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new restiny_colbat_t("lineorderAN", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new restiny_colbat_t("lineorderAN", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new resint_colbat_t("lineorderAN", "extendedprice"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDYenc, v2::bat::ops::copy(batDYcb));
    MEASURE_OP(sw1, x, batDDenc, v2::bat::ops::copy(batDDcb));
    MEASURE_OP(sw1, x, batLQenc, v2::bat::ops::copy(batLQcb));
    MEASURE_OP(sw1, x, batLDenc, v2::bat::ops::copy(batLDcb));
    MEASURE_OP(sw1, x, batLOenc, v2::bat::ops::copy(batLOcb));
    MEASURE_OP(sw1, x, batLEenc, v2::bat::ops::copy(batLEcb));

    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;

    rssAfterCopy = getPeakRSS(size_enum_t::KB);

    if (CONFIG.VERBOSE) {
        COUT_HEADLINE;
        COUT_RESULT(0, x);
        std::cout << std::endl;
    }

    SSBM_BEFORE_QUERY

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 0) Eager Check
        MEASURE_OP_TUPLE(sw2, x, tupleDY, v2::bat::ops::checkAndDecodeAN(batDYenc));
        CLEAR_CHECKANDDECODE_AN(tupleDY);
        MEASURE_OP_TUPLE(sw2, x, tupleDD, v2::bat::ops::checkAndDecodeAN(batDDenc));
        CLEAR_CHECKANDDECODE_AN(tupleDD);
        MEASURE_OP_TUPLE(sw2, x, tupleLQ, v2::bat::ops::checkAndDecodeAN(batLQenc));
        CLEAR_CHECKANDDECODE_AN(tupleLQ);
        MEASURE_OP_TUPLE(sw2, x, tupleLD, v2::bat::ops::checkAndDecodeAN(batLDenc));
        CLEAR_CHECKANDDECODE_AN(tupleLD);
        MEASURE_OP_TUPLE(sw2, x, tupleLO, v2::bat::ops::checkAndDecodeAN(batLOenc));
        CLEAR_CHECKANDDECODE_AN(tupleLO);
        MEASURE_OP_TUPLE(sw2, x, tupleLE, v2::bat::ops::checkAndDecodeAN(batLEenc));
        CLEAR_CHECKANDDECODE_AN(tupleLE);

        // 1) select from lineorder
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::select<std::less>(std::get<0>(tupleLQ), 25)); // lo_quantity < 25
        delete std::get<0>(tupleLQ);
        MEASURE_OP(sw2, x, bat2, v2::bat::ops::select(std::get<0>(tupleLD), 1, 3)); // lo_discount between 1 and 3
        delete std::get<0>(tupleLD);
        auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::matchjoin(bat3, bat2)); // join selection
        delete bat3;
        delete bat2;
        auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP(sw2, x, bat6, v2::bat::ops::matchjoin(bat5, std::get<0>(tupleLO))); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        delete std::get<0>(tupleLO);

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::select<std::equal_to>(std::get<0>(tupleDY), 1993)); // d_year = 1993
        delete std::get<0>(tupleDY);
        auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(sw2, x, bat9, v2::bat::ops::matchjoin(bat8, std::get<0>(tupleDD))); // only those d_datekey where d_year...
        delete bat8;
        delete std::get<0>(tupleDD);

        // 3) join lineorder and date
        auto batA = bat9->reverse();
        delete bat9;
        MEASURE_OP(sw2, x, batB, v2::bat::ops::hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete batA;
        delete bat6;
        // batB has in the Head the positions from lineorder and in the Tail the positions from date
        auto batC = batB->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        MEASURE_OP(sw2, x, batD, v2::bat::ops::matchjoin(batC, std::get<0>(tupleLE)));
        delete std::get<0>(tupleLE);
        MEASURE_OP(sw2, x, batE, v2::bat::ops::matchjoin(batC, bat4));
        delete batC;
        delete bat4;

        // 4) result
        MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(batD, batE, 0));
        delete batD;
        delete batE;

        totalTimes[i] = sw1.stop();

        std::cout << "(" << std::setw(2) << i << ")\n\tresult: " << result << "\n\t  time: " << sw1 << " ns.\n";
        COUT_HEADLINE;
        COUT_RESULT(0, x, OP_NAMES);
    }

    rssAfterQueries = getPeakRSS(size_enum_t::KB);

    if (CONFIG.VERBOSE) {
        std::cout << "Memory statistics (Resident Set size in KB):\n" << std::setw(16) << "before load: " << rssBeforeLoad << "\n" << std::setw(16) << "after load: " << rssAfterLoad << "\n" << std::setw(16) << "after copy: " << rssAfterCopy << "\n" << std::setw(16) << "after queries: " << rssAfterQueries << "\n";
    }

    std::cout << "TotalTimes:";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << '\n' << std::setw(2) << i << '\t' << totalTimes[i];
    }

    std::cout << "\nMemory:\n" << rssBeforeLoad << '\n' << rssAfterLoad << '\n' << rssAfterCopy << '\n' << rssAfterQueries << std::endl;

    delete batDYenc;
    delete batDDenc;
    delete batLQenc;
    delete batLDenc;
    delete batLOenc;
    delete batLEenc;

    TransactionManager::destroyInstance();

    SSBM_FINALIZE

    return 0;
}

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
 * File:   ssbm-q11_dmr_seq.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 24. January 2017, 14:43
 */

#include "ssbm.hpp"

const size_t MODULARITY = 2;

int
main (int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    std::vector<StopWatch::rep> totalTimes(CONFIG.NUM_RUNS);
    const size_t NUM_OPS = 24;
    cstr_t OP_NAMES[NUM_OPS] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P"};
    SSBM_REQUIRED_VARIABLES
    size_t x = 0;
    StopWatch sw1, sw2;
    size_t rssBeforeLoad, rssAfterLoad, rssAfterCopy, rssAfterQueries;

    std::cout << "SSBM Query 1.1 DMR Sequential\n=============================" << std::endl;

    MetaRepositoryManager::init(CONFIG.DB_PATH.c_str());

    rssBeforeLoad = getPeakRSS(size_enum_t::KB);

    sw1.start();
    // loadTable(CONFIG.DB_PATH, "customer", CONFIG);
    loadTable(CONFIG.DB_PATH, "date", CONFIG);
    loadTable(CONFIG.DB_PATH, "lineorder", CONFIG);
    // loadTable(CONFIG.DB_PATH, "part", CONFIG);
    // loadTable(CONFIG.DB_PATH, "supplier", CONFIG);
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
    MEASURE_OP(sw1, x, batDYcb, new shortint_colbat_t("date", "year"));
    MEASURE_OP(sw1, x, batDDcb, new int_colbat_t("date", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new tinyint_colbat_t("lineorder", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new tinyint_colbat_t("lineorder", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new int_colbat_t("lineorder", "extendedprice"));

    /* Measure converting (copying) ColumnBats to TempBats */
    shortint_tmpbat_t * batDYs[2];
    int_tmpbat_t * batDDs[2];
    tinyint_tmpbat_t * batLQs[2];
    tinyint_tmpbat_t * batLDs[2];
    int_tmpbat_t * batLOs[2];
    int_tmpbat_t * batLEs[2];

    MEASURE_OP(sw1, x, batDYs, [0], v2::bat::ops::copy(batDYcb), batDYs[0]->size(), batDYs[0]->consumption(), batDYs[0]->consumptionProjected());
    MEASURE_OP(sw1, x, batDDs, [0], v2::bat::ops::copy(batDDcb), batDDs[0]->size(), batDDs[0]->consumption(), batDDs[0]->consumptionProjected());
    MEASURE_OP(sw1, x, batLQs, [0], v2::bat::ops::copy(batLQcb), batLQs[0]->size(), batLQs[0]->consumption(), batLQs[0]->consumptionProjected());
    MEASURE_OP(sw1, x, batLDs, [0], v2::bat::ops::copy(batLDcb), batLDs[0]->size(), batLDs[0]->consumption(), batLDs[0]->consumptionProjected());
    MEASURE_OP(sw1, x, batLOs, [0], v2::bat::ops::copy(batLOcb), batLOs[0]->size(), batLOs[0]->consumption(), batLOs[0]->consumptionProjected());
    MEASURE_OP(sw1, x, batLEs, [0], v2::bat::ops::copy(batLEcb), batLEs[0]->size(), batLEs[0]->consumption(), batLEs[0]->consumptionProjected());

    MEASURE_OP(sw1, x, batDYs, [1], v2::bat::ops::copy(batDYcb), batDYs[1]->size(), batDYs[1]->consumption(), batDYs[1]->consumptionProjected());
    MEASURE_OP(sw1, x, batDDs, [1], v2::bat::ops::copy(batDDcb), batDDs[1]->size(), batDDs[1]->consumption(), batDDs[1]->consumptionProjected());
    MEASURE_OP(sw1, x, batLQs, [1], v2::bat::ops::copy(batLQcb), batLQs[1]->size(), batLQs[1]->consumption(), batLQs[1]->consumptionProjected());
    MEASURE_OP(sw1, x, batLDs, [1], v2::bat::ops::copy(batLDcb), batLDs[1]->size(), batLDs[1]->consumption(), batLDs[1]->consumptionProjected());
    MEASURE_OP(sw1, x, batLOs, [1], v2::bat::ops::copy(batLOcb), batLOs[1]->size(), batLOs[1]->consumption(), batLOs[1]->consumptionProjected());
    MEASURE_OP(sw1, x, batLEs, [1], v2::bat::ops::copy(batLEcb), batLEs[1]->size(), batLEs[1]->consumption(), batLEs[1]->consumptionProjected());

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

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        uint64_t results[MODULARITY] = {0, 0};

        for (size_t k = 0; k < MODULARITY; ++k) {
            // 1) select from lineorder
            MEASURE_OP(sw2, x, bat1, v2::bat::ops::select<std::less>(batLQs[k], 25)); // lo_quantity < 25
            MEASURE_OP(sw2, x, bat2, v2::bat::ops::select(batLDs[k], 1, 3)); // lo_discount between 1 and 3
            auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
            delete bat1;
            MEASURE_OP(sw2, x, bat4, v2::bat::ops::matchjoin(bat3, bat2)); // join selection
            delete bat3;
            delete bat2;
            auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
            MEASURE_OP(sw2, x, bat6, v2::bat::ops::matchjoin(bat5, batLOs[k])); // only those lo_orderdates where lo_quantity... and lo_discount
            delete bat5;

            // 2) select from date (join inbetween to reduce the number of lines we touch in total)
            MEASURE_OP(sw2, x, bat7, v2::bat::ops::select<std::equal_to>(batDYs[k], 1993)); // d_year = 1993
            auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_datekey
            delete bat7;
            MEASURE_OP(sw2, x, bat9, v2::bat::ops::matchjoin(bat8, batDDs[k])); // only those d_datekey where d_year...
            delete bat8;

            // 3) join lineorder and date
            auto batA = bat9->reverse();
            delete bat9;
            MEASURE_OP(sw2, x, batB, v2::bat::ops::hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
            delete bat6;
            delete batA;
            // batB has in the Head the positions from lineorder and in the Tail the positions from date
            auto batC = batB->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
            delete batB;
            MEASURE_OP(sw2, x, batD, v2::bat::ops::matchjoin(batC, batLEs[k]));
            MEASURE_OP(sw2, x, batE, v2::bat::ops::matchjoin(batC, bat4));
            delete batC;
            delete bat4;

            // 4) result
            MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(batD, batE, 0));
            delete batD;
            delete batE;

            results[k] = result;
        }

        totalTimes[i] = sw1.stop();

        // 5) Voting
        uint64_t result = results[0] == results[1] ? results[0] : static_cast<uint64_t>(-1);

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

    for (size_t k = 0; k < MODULARITY; ++k) {
        delete batDYs[k];
        delete batDDs[k];
        delete batLQs[k];
        delete batLDs[k];
        delete batLOs[k];
        delete batLEs[k];
    }

    TransactionManager::destroyInstance();

    SSBM_FINALIZE

    return 0;
}

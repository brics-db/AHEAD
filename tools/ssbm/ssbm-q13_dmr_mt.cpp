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
 * File:   ssbm-q13_dmr_mt.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 24. January 2017, 17:13
 */

#include "ssbm.hpp"
#include <omp.h>

const size_t MODULARITY = 2;

int
main (int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    std::vector<StopWatch::rep> totalTimes(CONFIG.NUM_RUNS);
    const size_t NUM_OPS = 28;
    cstr_t OP_NAMES[NUM_OPS] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"};
    StopWatch::rep opTimes[NUM_OPS] = {0};
    size_t batSizes[NUM_OPS] = {0};
    size_t batConsumptions[NUM_OPS] = {0};
    bool hasTwoTypes[NUM_OPS] = {false};
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];
    std::string emptyString;
    size_t x = 0;
    StopWatch sw1, sw2;

    std::cout << "SSBM Query 1.3 Parallel\n===========================" << std::endl;

    MetaRepositoryManager::init(CONFIG.DB_PATH.c_str());

    sw1.start();
    // loadTable(CONFIG.DB_PATH, "customer", CONFIG);
    loadTable(CONFIG.DB_PATH, "date", CONFIG);
    loadTable(CONFIG.DB_PATH, "lineorder", CONFIG);
    // loadTable(CONFIG.DB_PATH, "part", CONFIG);
    // loadTable(CONFIG.DB_PATH, "supplier", CONFIG);
    sw1.stop();
    std::cout << "Total loading time: " << sw1 << " ns." << std::endl;

    if (CONFIG.VERBOSE) {
        std::cout << "\nSSBM Q1.3:\n";
        std::cout << "select sum(lo_extendedprice * lo_discount) as revenue\n";
        std::cout << "  from lineorder, date\n";
        std::cout << "  where lo_orderdate = d_datekey\n";
        std::cout << "    and d_weeknuminyear = 6\n";
        std::cout << "    and d_year = 1994\n";
        std::cout << "    and lo_discount between 5 and 7\n";
        std::cout << "    and lo_quantity  between 26 and 35;" << std::endl;
    }

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new shortint_colbat_t("date", "year"));
    MEASURE_OP(sw1, x, batDDcb, new int_colbat_t("date", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new tinyint_colbat_t("lineorder", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new tinyint_colbat_t("lineorder", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new int_colbat_t("lineorder", "extendedprice"));
    MEASURE_OP(sw1, x, batDWcb, new tinyint_colbat_t("date", "weeknuminyear"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDY, v2::bat::ops::copy(batDYcb));
    MEASURE_OP(sw1, x, batDD, v2::bat::ops::copy(batDDcb));
    MEASURE_OP(sw1, x, batLQ, v2::bat::ops::copy(batLQcb));
    MEASURE_OP(sw1, x, batLD, v2::bat::ops::copy(batLDcb));
    MEASURE_OP(sw1, x, batLO, v2::bat::ops::copy(batLOcb));
    MEASURE_OP(sw1, x, batLE, v2::bat::ops::copy(batLEcb));
    MEASURE_OP(sw1, x, batDW, v2::bat::ops::copy(batDWcb));

    shortint_tmpbat_t * batDYs[2];
    int_tmpbat_t * batDDs[2];
    tinyint_tmpbat_t * batLQs[2];
    tinyint_tmpbat_t * batLDs[2];
    int_tmpbat_t * batLOs[2];
    int_tmpbat_t * batLEs[2];
    tinyint_tmpbat_t * batDWs[2];

    MEASURE_OP(sw1, x, batDYs, [0], v2::bat::ops::copy(batDYcb), batDYs[0]->size(), batDYs[0]->consumption());
    MEASURE_OP(sw1, x, batDDs, [0], v2::bat::ops::copy(batDDcb), batDDs[0]->size(), batDDs[0]->consumption());
    MEASURE_OP(sw1, x, batLQs, [0], v2::bat::ops::copy(batLQcb), batLQs[0]->size(), batLQs[0]->consumption());
    MEASURE_OP(sw1, x, batLDs, [0], v2::bat::ops::copy(batLDcb), batLDs[0]->size(), batLDs[0]->consumption());
    MEASURE_OP(sw1, x, batLOs, [0], v2::bat::ops::copy(batLOcb), batLOs[0]->size(), batLOs[0]->consumption());
    MEASURE_OP(sw1, x, batLEs, [0], v2::bat::ops::copy(batLEcb), batLEs[0]->size(), batLEs[0]->consumption());
    MEASURE_OP(sw1, x, batDWs, [0], v2::bat::ops::copy(batDWcb), batDWs[0]->size(), batDWs[0]->consumption());

    MEASURE_OP(sw1, x, batDYs, [1], v2::bat::ops::copy(batDYcb), batDYs[1]->size(), batDYs[1]->consumption());
    MEASURE_OP(sw1, x, batDDs, [1], v2::bat::ops::copy(batDDcb), batDDs[1]->size(), batDDs[1]->consumption());
    MEASURE_OP(sw1, x, batLQs, [1], v2::bat::ops::copy(batLQcb), batLQs[1]->size(), batLQs[1]->consumption());
    MEASURE_OP(sw1, x, batLDs, [1], v2::bat::ops::copy(batLDcb), batLDs[1]->size(), batLDs[1]->consumption());
    MEASURE_OP(sw1, x, batLOs, [1], v2::bat::ops::copy(batLOcb), batLOs[1]->size(), batLOs[1]->consumption());
    MEASURE_OP(sw1, x, batLEs, [1], v2::bat::ops::copy(batLEcb), batLEs[1]->size(), batLEs[1]->consumption());
    MEASURE_OP(sw1, x, batDWs, [1], v2::bat::ops::copy(batDWcb), batDWs[1]->size(), batDWs[1]->consumption());

    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;
    delete batDWcb;

    if (CONFIG.VERBOSE) {
        COUT_HEADLINE;
        COUT_RESULT(0, x);
        std::cout << std::endl;
    }

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        uint64_t results[MODULARITY] = {0, 0};

#pragma omp parallel for
        for (size_t k = 0; k < MODULARITY; ++k) {
            // 1) select from lineorder
            MEASURE_OP(sw2, x, bat1, v2::bat::ops::select(batLQs[k], 26, 35)); // lo_quantity between 26 and 35
            MEASURE_OP(sw2, x, bat2, v2::bat::ops::select(batLDs[k], 5, 7)); // lo_discount between 5 and 7
            auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
            delete bat1;
            MEASURE_OP(sw2, x, bat4, v2::bat::ops::matchjoin(bat3, bat2)); // join selection
            delete bat2;
            delete bat3;
            auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
            MEASURE_OP(sw2, x, bat6, v2::bat::ops::matchjoin(bat5, batLOs[k])); // only those lo_orderdates where lo_quantity... and lo_discount
            delete bat5;

            // 1) select from date (join inbetween to reduce the number of lines we touch in total)
            MEASURE_OP(sw2, x, bat7, v2::bat::ops::select<std::equal_to>(batDYs[k], 1994)); // d_year = 1994
            auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_weeknuminyear
            delete bat7;
            MEASURE_OP(sw2, x, bat9, v2::bat::ops::select<std::equal_to>(batDWs[k], 6)); // d_weeknuminyear = 6
            MEASURE_OP(sw2, x, batA, v2::bat::ops::matchjoin(bat8, bat9));
            delete bat8;
            delete bat9;
            auto batB = batA->mirror_head();
            delete batA;
            MEASURE_OP(sw2, x, batC, v2::bat::ops::matchjoin(batB, batDDs[k])); // only those d_datekey where d_year and d_weeknuminyear...
            delete batB;
            auto batD = batC->reverse();
            delete batC;

            // 3) join lineorder and date
            MEASURE_OP(sw2, x, batE, v2::bat::ops::hashjoin(bat6, batD)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
            delete bat6;
            delete batD;
            // batE now has in the Head the positions from lineorder and in the Tail the positions from date
            auto batF = batE->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
            delete batE;
            // BatF only contains the 
            MEASURE_OP(sw2, x, batG, v2::bat::ops::matchjoin(batF, batLEs[k]));
            MEASURE_OP(sw2, x, batH, v2::bat::ops::matchjoin(batF, bat4));
            delete batF;
            delete bat4;
            MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(batG, batH, 0));
            delete batG;
            delete batH;

#pragma omp critical
            {
                results[k] = result;
            }
        }

        totalTimes[i] = sw1.stop();

        // 5) Voting
        uint64_t result = results[0] == results[1] ? results[0] : static_cast<uint64_t>(-1);

        std::cout << "(" << std::setw(2) << i << ")\n\tresult: " << result << "\n\t  time: " << sw1 << " ns.\n";
        COUT_HEADLINE;
        COUT_RESULT(0, x, OP_NAMES);
    }

    if (CONFIG.VERBOSE) {
        std::cout << "peak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.\n";
    }

    std::cout << "TotalTimes:";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << '\n' << std::setw(2) << i << '\t' << totalTimes[i];
    }
    std::cout << std::endl;

    for (size_t k = 0; k < MODULARITY; ++k) {
        delete batDYs[k];
        delete batDDs[k];
        delete batLQs[k];
        delete batLDs[k];
        delete batLOs[k];
        delete batLEs[k];
        delete batDWs[k];
    }

    TransactionManager::destroyInstance();

    return 0;
}

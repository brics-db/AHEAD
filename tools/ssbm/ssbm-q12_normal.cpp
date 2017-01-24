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
 * File:   ssbm-q12_normal.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 30. October 2016, 00:00
 */

#include "ssbm.hpp"

int
main (int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    std::vector<StopWatch::rep> totalTimes(CONFIG.NUM_RUNS);
    const size_t NUM_OPS = 24;
    cstr_t OP_NAMES[NUM_OPS] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P"};
    StopWatch::rep opTimes[NUM_OPS] = {0};
    size_t batSizes[NUM_OPS] = {0};
    size_t batConsumptions[NUM_OPS] = {0};
    bool hasTwoTypes[NUM_OPS] = {false};
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];
    std::string emptyString;
    size_t x = 0;
    StopWatch sw1, sw2;

    std::cout << "SSBM Query 1.2 Normal\n=====================" << std::endl;

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
        std::cout << "SSBM Q1.2:\n";
        std::cout << "select sum(lo_extendedprice * lo_discount) as revenue\n";
        std::cout << "  from lineorder, date\n";
        std::cout << "  where lo_orderdate = d_datekey\n";
        std::cout << "    and d_yearmonthnum = 199401\n";
        std::cout << "    and lo_discount between 4 and 6\n";
        std::cout << "    and lo_quantity  between 26 and 35;" << std::endl;
    }

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new int_colbat_t("date", "yearmonthnum"));
    MEASURE_OP(sw1, x, batDDcb, new int_colbat_t("date", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new tinyint_colbat_t("lineorder", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new tinyint_colbat_t("lineorder", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new int_colbat_t("lineorder", "extendedprice"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDY, v2::bat::ops::copy(batDYcb));
    MEASURE_OP(sw1, x, batDD, v2::bat::ops::copy(batDDcb));
    MEASURE_OP(sw1, x, batLQ, v2::bat::ops::copy(batLQcb));
    MEASURE_OP(sw1, x, batLD, v2::bat::ops::copy(batLDcb));
    MEASURE_OP(sw1, x, batLO, v2::bat::ops::copy(batLOcb));
    MEASURE_OP(sw1, x, batLE, v2::bat::ops::copy(batLEcb));
    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;

    if (CONFIG.VERBOSE) {
        COUT_HEADLINE;
        COUT_RESULT(0, x);
        std::cout << std::endl;
    }

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 1) select from lineorder
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::select(batLQ, 26, 35)); // lo_quantity between 26 and 35
        MEASURE_OP(sw2, x, bat2, v2::bat::ops::select(batLD, 4, 6)); // lo_discount between 4 and 6
        auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::matchjoin(bat3, bat2)); // join selection
        delete bat2;
        delete bat3;
        auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP(sw2, x, bat6, v2::bat::ops::matchjoin(bat5, batLO)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::select<std::equal_to>(batDY, 199401)); // d_yearmonthnum = 199401
        auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(sw2, x, bat9, v2::bat::ops::matchjoin(bat8, batDD)); // only those d_datekey where d_year...
        delete bat8;

        // 3) join lineorder and date
        auto batA = bat9->reverse();
        delete bat9;
        MEASURE_OP(sw2, x, batB, v2::bat::ops::hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete bat6;
        delete batA;
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        auto batC = batB->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        // BatF only contains the 
        MEASURE_OP(sw2, x, batD, v2::bat::ops::matchjoin(batC, batLE));
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

    if (CONFIG.VERBOSE) {
        std::cout << "peak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.\n";
    }

    std::cout << "TotalTimes:";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << '\n' << std::setw(2) << i << '\t' << totalTimes[i];
    }
    std::cout << std::endl;

    delete batDY;
    delete batDD;
    delete batLQ;
    delete batLD;
    delete batLO;
    delete batLE;

    TransactionManager::destroyInstance();

    return 0;
}

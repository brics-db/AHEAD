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
 * File:   ssbm-q13_dmr_mt.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 24. January 2017, 17:13
 */

#include "ssbm.hpp"
#include <omp.h>

int
main (int argc, char** argv) {
    const size_t MODULARITY = 2;

    SSBM_REQUIRED_VARIABLES("SSBM Query 1.3 DMR Parallel\n===========================", 24, "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P");

    SSBM_LOAD("date", "lineorder",
        "SSBM Q1.3:\n"                                             \
        "select sum(lo_extendedprice * lo_discount) as revenue\n"  \
        "  from lineorder, date\n"                                 \
        "  where lo_orderdate = d_datekey\n"                       \
        "    and d_year = 1997\n"                                  \
        "    and lo_discount between 5 and 7\n"                    \
        "    and lo_quantity between 26 and 35;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batDYcb, new shortint_colbat_t("date", "year"));
    MEASURE_OP(batDDcb, new int_colbat_t("date", "datekey"));
    MEASURE_OP(batLQcb, new tinyint_colbat_t("lineorder", "quantity"));
    MEASURE_OP(batLDcb, new tinyint_colbat_t("lineorder", "discount"));
    MEASURE_OP(batLOcb, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(batLEcb, new int_colbat_t("lineorder", "extendedprice"));
    MEASURE_OP(batDWcb, new tinyint_colbat_t("date", "weeknuminyear"));

    /* Measure converting (copying) ColumnBats to TempBats */
    shortint_tmpbat_t * batDYs[2];
    int_tmpbat_t * batDDs[2];
    tinyint_tmpbat_t * batLQs[2];
    tinyint_tmpbat_t * batLDs[2];
    int_tmpbat_t * batLOs[2];
    int_tmpbat_t * batLEs[2];
    tinyint_tmpbat_t * batDWs[2];

    MEASURE_OP(batDYs, [0], v2::bat::ops::copy(batDYcb), batDYs[0]->size(), batDYs[0]->consumption(), batDYs[0]->consumptionProjected());
    MEASURE_OP(batDDs, [0], v2::bat::ops::copy(batDDcb), batDDs[0]->size(), batDDs[0]->consumption(), batDDs[0]->consumptionProjected());
    MEASURE_OP(batLQs, [0], v2::bat::ops::copy(batLQcb), batLQs[0]->size(), batLQs[0]->consumption(), batLQs[0]->consumptionProjected());
    MEASURE_OP(batLDs, [0], v2::bat::ops::copy(batLDcb), batLDs[0]->size(), batLDs[0]->consumption(), batLDs[0]->consumptionProjected());
    MEASURE_OP(batLOs, [0], v2::bat::ops::copy(batLOcb), batLOs[0]->size(), batLOs[0]->consumption(), batLOs[0]->consumptionProjected());
    MEASURE_OP(batLEs, [0], v2::bat::ops::copy(batLEcb), batLEs[0]->size(), batLEs[0]->consumption(), batLEs[0]->consumptionProjected());
    MEASURE_OP(batDWs, [0], v2::bat::ops::copy(batDWcb), batDWs[0]->size(), batDWs[0]->consumption(), batDWs[0]->consumptionProjected());

    MEASURE_OP(batDYs, [1], v2::bat::ops::copy(batDYcb), batDYs[1]->size(), batDYs[1]->consumption(), batDYs[1]->consumptionProjected());
    MEASURE_OP(batDDs, [1], v2::bat::ops::copy(batDDcb), batDDs[1]->size(), batDDs[1]->consumption(), batDDs[1]->consumptionProjected());
    MEASURE_OP(batLQs, [1], v2::bat::ops::copy(batLQcb), batLQs[1]->size(), batLQs[1]->consumption(), batLQs[1]->consumptionProjected());
    MEASURE_OP(batLDs, [1], v2::bat::ops::copy(batLDcb), batLDs[1]->size(), batLDs[1]->consumption(), batLDs[1]->consumptionProjected());
    MEASURE_OP(batLOs, [1], v2::bat::ops::copy(batLOcb), batLOs[1]->size(), batLOs[1]->consumption(), batLOs[1]->consumptionProjected());
    MEASURE_OP(batLEs, [1], v2::bat::ops::copy(batLEcb), batLEs[1]->size(), batLEs[1]->consumption(), batLEs[1]->consumptionProjected());
    MEASURE_OP(batDWs, [1], v2::bat::ops::copy(batDWcb), batDWs[1]->size(), batDWs[1]->consumption(), batDWs[1]->consumptionProjected());

    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;
    delete batDWcb;

    SSBM_BEFORE_QUERIES;

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        SSBM_BEFORE_QUERY;

        ModularRedundantValue<bigint_t, MODULARITY> results(0);

#pragma omp parallel for
        for (size_t k = 0; k < MODULARITY; ++k) {
            // 1) select from lineorder
            MEASURE_OP(bat1, v2::bat::ops::select(batLQs[k], 26, 35)); // lo_quantity between 26 and 35
            MEASURE_OP(bat2, v2::bat::ops::select(batLDs[k], 5, 7)); // lo_discount between 5 and 7
            auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
            delete bat1;
            MEASURE_OP(bat4, v2::bat::ops::matchjoin(bat3, bat2)); // join selection
            delete bat2;
            delete bat3;
            auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
            MEASURE_OP(bat6, v2::bat::ops::matchjoin(bat5, batLOs[k])); // only those lo_orderdates where lo_quantity... and lo_discount
            delete bat5;

            // 1) select from date (join inbetween to reduce the number of lines we touch in total)
            MEASURE_OP(bat7, v2::bat::ops::select<std::equal_to>(batDYs[k], 1994)); // d_year = 1994
            auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_weeknuminyear
            delete bat7;
            MEASURE_OP(bat9, v2::bat::ops::select<std::equal_to>(batDWs[k], 6)); // d_weeknuminyear = 6
            MEASURE_OP(batA, v2::bat::ops::matchjoin(bat8, bat9));
            delete bat8;
            delete bat9;
            auto batB = batA->mirror_head();
            delete batA;
            MEASURE_OP(batC, v2::bat::ops::matchjoin(batB, batDDs[k])); // only those d_datekey where d_year and d_weeknuminyear...
            delete batB;
            auto batD = batC->reverse();
            delete batC;

            // 3) join lineorder and date
            MEASURE_OP(batE, v2::bat::ops::hashjoin(bat6, batD)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
            delete bat6;
            delete batD;
            // batE now has in the Head the positions from lineorder and in the Tail the positions from date
            auto batF = batE->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
            delete batE;
            // BatF only contains the 
            MEASURE_OP(batG, v2::bat::ops::matchjoin(batF, batLEs[k]));
            MEASURE_OP(batH, v2::bat::ops::matchjoin(batF, bat4));
            delete batF;
            delete bat4;
            MEASURE_OP(batI, v2::bat::ops::aggregate_mul_sum_SSE<v2_bigint_t>(batG, batH, 0));
            delete batG;
            delete batH;
            auto iter = batI->begin();
            auto result = iter->tail();
            delete iter;
            delete batI;

#pragma omp critical
            {
                results[k] = result;
            }
        }

        // 5) Voting
        auto result = results();

        SSBM_AFTER_QUERY(i, result);
    }

    SSBM_AFTER_QUERIES;

    for (size_t k = 0; k < MODULARITY; ++k) {
        delete batDYs[k];
        delete batDDs[k];
        delete batLQs[k];
        delete batLDs[k];
        delete batLOs[k];
        delete batLEs[k];
        delete batDWs[k];
    }

    SSBM_FINALIZE;

    return 0;
}

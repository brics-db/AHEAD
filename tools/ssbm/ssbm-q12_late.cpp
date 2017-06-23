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
 * File:   ssbm-q12_late.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 30. October 2016, 00:03
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Query 1.2 Late Detection");

    SSBM_LOAD("dateAN", "lineorderAN", "SSBM Q1.2:\n"
            "select sum(lo_extendedprice * lo_discount) as revenue\n"
            "  from lineorder, date\n"
            "  where lo_orderdate = d_datekey\n"
            "    and d_yearmonthnum = 199401\n"
            "    and lo_discount between 4 and 6\n"
            "    and lo_quantity between 26 and 35;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batDYcb, new resint_colbat_t("dateAN", "yearmonthnum"));
    MEASURE_OP(batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batLQcb, new restiny_colbat_t("lineorderAN", "quantity"));
    MEASURE_OP(batLDcb, new restiny_colbat_t("lineorderAN", "discount"));
    MEASURE_OP(batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLEcb, new resint_colbat_t("lineorderAN", "extendedprice"));

    ssb::after_create_columnbats();

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(batDYenc, copy(batDYcb));
    MEASURE_OP(batDDenc, copy(batDDcb));
    MEASURE_OP(batLQenc, copy(batLQcb));
    MEASURE_OP(batLDenc, copy(batLDcb));
    MEASURE_OP(batLOenc, copy(batLOcb));
    MEASURE_OP(batLEenc, copy(batLEcb));

    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;

    ssb::before_queries();

    for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
        ssb::before_query();

        // LAZY MODE !!! NO AN-OPERATORS UNTIL DECODING !!!

        // 1) select from lineorder
        MEASURE_OP(bat1,
                (select<std::greater_equal, std::less_equal, AND>(batLQenc, static_cast<restiny_t>(26 * batLQenc->tail.metaData.AN_A), static_cast<restiny_t>(35 * batLQenc->tail.metaData.AN_A)))); // lo_quantity between 26 and 35
        MEASURE_OP(bat2,
                (select<std::greater_equal, std::less_equal, AND>(batLDenc, static_cast<restiny_t>(4 * batLDenc->tail.metaData.AN_A), static_cast<restiny_t>(6 * batLDenc->tail.metaData.AN_A)))); // lo_discount between 4 and 6
        auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(bat4, matchjoin(bat3, bat2)); // join selection
        delete bat3;
        delete bat2;
        auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP(bat6, matchjoin(bat5, batLOenc)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(bat7, select<std::equal_to>(batDYenc, static_cast<resint_t>(199401) * batDYenc->tail.metaData.AN_A)); // d_yearmonthnum = 199401
        auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(bat9, matchjoin(bat8, batDDenc)); // only those d_datekey where d_year...
        delete bat8;

        // 3) join lineorder and date
        auto batA = bat9->reverse();
        delete bat9;
        MEASURE_OP(batB, hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete bat6;
        delete batA;
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        auto batC = batB->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        // BatF only contains the 
        MEASURE_OP(batD, matchjoin(batC, batLEenc));
        MEASURE_OP(batE, matchjoin(batC, bat4));
        delete batC;
        delete bat4;

        // 4) late check and decode
        MEASURE_OP_TUPLE(tupleF, checkAndDecodeAN(batD));
        CLEAR_CHECKANDDECODE_AN(tupleF);
        auto batF = std::get<0>(tupleF);
        delete batD;
        MEASURE_OP_TUPLE(tupleG, checkAndDecodeAN(batE));
        CLEAR_CHECKANDDECODE_AN(tupleG);
        auto batG = std::get<0>(tupleG);
        delete batE;

        // result
        MEASURE_OP(batH, aggregate_mul_sum<v2_bigint_t>(batF, batG));
        delete batF;
        delete batG;
        auto iter = batH->begin();
        auto result = iter->tail();
        delete iter;
        delete batH;

        ssb::after_query(i, result);
    }

    ssb::after_queries();

    delete batDYenc;
    delete batDDenc;
    delete batLQenc;
    delete batLDenc;
    delete batLOenc;
    delete batLEenc;

    ssb::finalize();

    return 0;
}

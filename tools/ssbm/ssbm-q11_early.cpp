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
 * File:   ssbm-q11_early.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(int argc, char** argv) {
    ssb::init(argc, argv, "SSBM Query 1.1 Early Detection\n==============================");

    SSBM_LOAD("dateAN", "lineorderAN", "SSBM Q1.1:\n"
            "select sum(lo_extendedprice * lo_discount) as revenue\n"
            "  from lineorder, date\n"
            "  where lo_orderdate = d_datekey\n"
            "    and d_year = 1993\n"
            "    and lo_discount between 1 and 3\n"
            "    and lo_quantity < 25;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batDYcb, new resshort_colbat_t("dateAN", "year"));
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

        // 0) Eager Check
        MEASURE_OP_TUPLE(tupleDY, checkAndDecodeAN(batDYenc));CLEAR_CHECKANDDECODE_AN(tupleDY);
        MEASURE_OP_TUPLE(tupleDD, checkAndDecodeAN(batDDenc));CLEAR_CHECKANDDECODE_AN(tupleDD);
        MEASURE_OP_TUPLE(tupleLQ, checkAndDecodeAN(batLQenc));CLEAR_CHECKANDDECODE_AN(tupleLQ);
        MEASURE_OP_TUPLE(tupleLD, checkAndDecodeAN(batLDenc));CLEAR_CHECKANDDECODE_AN(tupleLD);
        MEASURE_OP_TUPLE(tupleLO, checkAndDecodeAN(batLOenc));CLEAR_CHECKANDDECODE_AN(tupleLO);
        MEASURE_OP_TUPLE(tupleLE, checkAndDecodeAN(batLEenc));CLEAR_CHECKANDDECODE_AN(tupleLE);

        // 1) select from lineorder
        MEASURE_OP(bat1, select<std::less>(std::get<0>(tupleLQ), 25)); // lo_quantity < 25
        delete std::get<0>(tupleLQ);
        MEASURE_OP(bat2, select(std::get<0>(tupleLD), 1, 3)); // lo_discount between 1 and 3
        delete std::get<0>(tupleLD);
        auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(bat4, matchjoin(bat3, bat2)); // join selection
        delete bat3;
        delete bat2;
        auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP(bat6, matchjoin(bat5, std::get<0>(tupleLO))); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        delete std::get<0>(tupleLO);

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(bat7, select<std::equal_to>(std::get<0>(tupleDY), 1993)); // d_year = 1993
        delete std::get<0>(tupleDY);
        auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(bat9, matchjoin(bat8, std::get<0>(tupleDD))); // only those d_datekey where d_year...
        delete bat8;
        delete std::get<0>(tupleDD);

        // 3) join lineorder and date
        auto batA = bat9->reverse();
        delete bat9;
        MEASURE_OP(batB, hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete batA;
        delete bat6;
        // batB has in the Head the positions from lineorder and in the Tail the positions from date
        auto batC = batB->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        MEASURE_OP(batD, matchjoin(batC, std::get<0>(tupleLE)));
        delete std::get<0>(tupleLE);
        MEASURE_OP(batE, matchjoin(batC, bat4));
        delete batC;
        delete bat4;

        // 4) result
        MEASURE_OP(batF, aggregate_mul_sum<v2_bigint_t>(batD, batE, 0));
        delete batD;
        delete batE;
        auto iter = batF->begin();
        auto result = iter->tail();
        delete iter;
        delete batF;

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

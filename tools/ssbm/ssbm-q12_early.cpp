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
 * File:   ssbm-q12_early.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 30. October 2016, 00:01
 */

#include "ssbm.hpp"
#include <column_operators/OperatorsAN.hpp>

int main(int argc, char** argv) {
    SSBM_REQUIRED_VARIABLES("SSBM Query 1.2 Early Detection\n==============================", 30, "-6", "-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D",
            "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P");

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

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(batDYenc, ahead::bat::ops::copy(batDYcb));
    MEASURE_OP(batDDenc, ahead::bat::ops::copy(batDDcb));
    MEASURE_OP(batLQenc, ahead::bat::ops::copy(batLQcb));
    MEASURE_OP(batLDenc, ahead::bat::ops::copy(batLDcb));
    MEASURE_OP(batLOenc, ahead::bat::ops::copy(batLOcb));
    MEASURE_OP(batLEenc, ahead::bat::ops::copy(batLEcb));

    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;

    SSBM_BEFORE_QUERIES;

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        SSBM_BEFORE_QUERY;

        // 0) Eager Check
        MEASURE_OP_TUPLE(tupleDY, ahead::bat::ops::checkAndDecodeAN(batDYenc));CLEAR_CHECKANDDECODE_AN(tupleDY);
        MEASURE_OP_TUPLE(tupleDD, ahead::bat::ops::checkAndDecodeAN(batDDenc));CLEAR_CHECKANDDECODE_AN(tupleDD);
        MEASURE_OP_TUPLE(tupleLQ, ahead::bat::ops::checkAndDecodeAN(batLQenc));CLEAR_CHECKANDDECODE_AN(tupleLQ);
        MEASURE_OP_TUPLE(tupleLD, ahead::bat::ops::checkAndDecodeAN(batLDenc));CLEAR_CHECKANDDECODE_AN(tupleLD);
        MEASURE_OP_TUPLE(tupleLO, ahead::bat::ops::checkAndDecodeAN(batLOenc));CLEAR_CHECKANDDECODE_AN(tupleLO);
        MEASURE_OP_TUPLE(tupleLE, ahead::bat::ops::checkAndDecodeAN(batLEenc));CLEAR_CHECKANDDECODE_AN(tupleLE);

        // 1) select from lineorder
        MEASURE_OP(bat1, ahead::bat::ops::select(std::get<0>(tupleLQ), 26, 35)); // lo_quantity between 26 and 35
        delete std::get<0>(tupleLQ);
        MEASURE_OP(bat2, ahead::bat::ops::select(std::get<0>(tupleLD), 4, 6)); // lo_discount between 4 and 6
        delete std::get<0>(tupleLD);
        auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(bat4, ahead::bat::ops::matchjoin(bat3, bat2)); // join selection
        delete bat3;
        delete bat2;
        auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP(bat6, ahead::bat::ops::matchjoin(bat5, std::get<0>(tupleLO))); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        delete std::get<0>(tupleLO);

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(bat7, ahead::bat::ops::select<std::equal_to>(std::get<0>(tupleDY), 199401)); // d_yearmonthnum = 199401
        delete std::get<0>(tupleDY);
        auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(bat9, ahead::bat::ops::matchjoin(bat8, std::get<0>(tupleDD))); // only those d_datekey where d_year...
        delete bat8;
        delete std::get<0>(tupleDD);

        // 3) join lineorder and date
        auto batA = bat9->reverse();
        delete bat9;
        MEASURE_OP(batB, ahead::bat::ops::hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete batA;
        delete bat6;
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        auto batC = batB->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        MEASURE_OP(batD, ahead::bat::ops::matchjoin(batC, std::get<0>(tupleLE)));
        delete std::get<0>(tupleLE);
        MEASURE_OP(batE, ahead::bat::ops::matchjoin(batC, bat4));
        delete batC;
        delete bat4;

        // 4) result
        MEASURE_OP(batF, ahead::bat::ops::aggregate_mul_sum<v2_bigint_t>(batD, batE, 0));
        delete batD;
        delete batE;
        auto iter = batF->begin();
        auto result = iter->tail();
        delete iter;
        delete batF;

        SSBM_AFTER_QUERY(i, result);
    }

    SSBM_AFTER_QUERIES;

    delete batDYenc;
    delete batDDenc;
    delete batLQenc;
    delete batLDenc;
    delete batLOenc;
    delete batLEenc;

    SSBM_FINALIZE;

    return 0;
}

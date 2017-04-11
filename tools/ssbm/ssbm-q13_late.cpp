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
 * File:   ssbm-q13_late.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 31. October 2016, 22:54
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"

int main(int argc, char** argv) {
    SSBM_REQUIRED_VARIABLES("SSBM Query 1.3 Late Detection\n=============================", 24, "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M",
            "N", "O", "P");

    SSBM_LOAD("dateAN", "lineorderAN", "SSBM Q1.3:\n"
            "select sum(lo_extendedprice * lo_discount) as revenue\n"
            "  from lineorder, date\n"
            "  where lo_orderdate = d_datekey\n"
            "    and d_year = 1997\n"
            "    and lo_discount between 5 and 7\n"
            "    and lo_quantity between 26 and 35;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batLQcb, new restiny_colbat_t("lineorderAN", "quantity"));
    MEASURE_OP(batLDcb, new restiny_colbat_t("lineorderAN", "discount"));
    MEASURE_OP(batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLEcb, new resint_colbat_t("lineorderAN", "extendedprice"));
    MEASURE_OP(batDWcb, new restiny_colbat_t("dateAN", "weeknuminyear"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(batDYenc, copy(batDYcb));
    MEASURE_OP(batDDenc, copy(batDDcb));
    MEASURE_OP(batLQenc, copy(batLQcb));
    MEASURE_OP(batLDenc, copy(batLDcb));
    MEASURE_OP(batLOenc, copy(batLOcb));
    MEASURE_OP(batLEenc, copy(batLEcb));
    MEASURE_OP(batDWenc, copy(batDWcb));

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

        // LAZY MODE !!! NO AN-OPERATORS UNTIL DECODING !!!

        // 1) select from lineorder
        MEASURE_OP(bat1, select(batLQenc, 26 * batLQenc->tail.metaData.AN_A, 35 * batLQenc->tail.metaData.AN_A)); // lo_quantity between 26 and 35
        MEASURE_OP(bat2, select(batLDenc, 5 * batLDenc->tail.metaData.AN_A, 7 * batLDenc->tail.metaData.AN_A)); // lo_discount between 5 and 7
        auto bat3 = bat1->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(bat4, matchjoin(bat3, bat2)); // join selection
        delete bat2;
        delete bat3;
        auto bat5 = bat4->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP(bat6, matchjoin(bat5, batLOenc)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(bat7, select<std::equal_to>(batDYenc, 1994 * batDYenc->tail.metaData.AN_A)); // d_year = 1994
        auto bat8 = bat7->mirror_head(); // prepare joined selection over d_year and d_weeknuminyear
        delete bat7;
        MEASURE_OP(bat9, select<std::equal_to>(batDWenc, 6 * batDWenc->tail.metaData.AN_A)); // d_weeknuminyear = 6
        MEASURE_OP(batA, matchjoin(bat8, bat9));
        delete bat8;
        delete bat9;
        auto batB = batA->mirror_head();
        delete batA;
        MEASURE_OP(batC, matchjoin(batB, batDDenc)); // only those d_datekey where d_year and d_weeknuminyear...
        delete batB;

        // 3) join lineorder and date
        auto batD = batC->reverse();
        delete batC;
        MEASURE_OP(batE, hashjoin(bat6, batD)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete bat6;
        delete batD;
        // batE has in the Head the positions from lineorder and in the Tail the positions from date
        auto batF = batE->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batE;
        MEASURE_OP(batG, matchjoin(batF, batLEenc));
        MEASURE_OP(batH, matchjoin(batF, bat4));
        delete batF;
        delete bat4;

        // 4) lazy decode and result
        MEASURE_OP_TUPLE(tupleI, checkAndDecodeAN(batG));CLEAR_CHECKANDDECODE_AN(tupleI);
        delete batG;
        MEASURE_OP_TUPLE(tupleJ, checkAndDecodeAN(batH));CLEAR_CHECKANDDECODE_AN(tupleJ);
        delete batH;
        MEASURE_OP(batK, aggregate_mul_sum<v2_bigint_t>(std::get<0>(tupleI), std::get<0>(tupleJ)));
        delete std::get<0>(tupleI);
        delete std::get<0>(tupleJ);
        auto iter = batK->begin();
        auto result = iter->tail();
        delete iter;
        delete batK;

        SSBM_AFTER_QUERY(i, result);
    }

    SSBM_AFTER_QUERIES;

    delete batDYenc;
    delete batDDenc;
    delete batLQenc;
    delete batLDenc;
    delete batLOenc;
    delete batLEenc;
    delete batDWenc;

    SSBM_FINALIZE;

    return 0;
}

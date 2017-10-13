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
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    return ssb::run(argc, argv, [] (int argc, char ** argv) -> int {
        ssb::init(argc, argv, "SSBM Query 1.3 Late Detection");

        ssb::loadTables( {"dateAN", "lineorderAN"}, "SSBM Q1.3:\n"
                "select sum(lo_extendedprice * lo_discount) as revenue\n"
                "  from lineorder, date\n"
                "  where lo_orderdate = d_datekey\n"
                "    and d_weeknuminyear = 6\n"
                "    and d_year = 1994\n"
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

        ssb::after_create_columnbats();

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

        ssb::before_queries();

        for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
            ssb::before_query();

            // LAZY MODE !!! NO AN-OPERATORS UNTIL DECODING !!!

            // 1) select from lineorder
            MEASURE_OP(bat1, (select<std::greater_equal, std::less_equal, ahead::and_is>(batLQenc, 26 * batLQenc->tail.metaData.AN_A, 35 * batLQenc->tail.metaData.AN_A)));// lo_quantity between 26 and 35
            MEASURE_OP(bat2, (select<std::greater_equal, std::less_equal, ahead::and_is>(batLDenc, 5 * batLDenc->tail.metaData.AN_A, 7 * batLDenc->tail.metaData.AN_A)));// lo_discount between 5 and 7
            auto bat3 = bat1->mirror_head();// prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
            delete bat1;
            auto batZ = bat2->mirror_head();
            delete bat2;
            MEASURE_OP(bat4, matchjoin(bat3, batZ));// join selection
            delete bat3;
            delete batZ;
            auto bat5 = bat4->mirror_head();// prepare joined selection with lo_orderdate (contains positions in tail)
            delete bat4;
            MEASURE_OP(bat6, matchjoin(bat5, batLOenc));// only those lo_orderdates where lo_quantity... and lo_discount

            // 2) select from date (join inbetween to reduce the number of lines we touch in total)
            MEASURE_OP(bat7, select<std::equal_to>(batDYenc, 1994 * batDYenc->tail.metaData.AN_A));// d_year = 1994
            auto bat8 = bat7->mirror_head();// prepare joined selection over d_year and d_weeknuminyear
            delete bat7;
            MEASURE_OP(bat9, select<std::equal_to>(batDWenc, 6 * batDWenc->tail.metaData.AN_A));// d_weeknuminyear = 6
            auto batY = bat9->mirror_head();
            delete bat9;
            MEASURE_OP(batA, matchjoin(bat8, batY));
            delete bat8;
            delete batY;
            auto batB = batA->mirror_head();
            delete batA;
            MEASURE_OP(batC, matchjoin(batB, batDDenc));// only those d_datekey where d_year and d_weeknuminyear...
            delete batB;

            // 3) join lineorder and date
            auto batD = batC->reverse();
            delete batC;
            MEASURE_OP(batE, hashjoin(bat6, batD));// only those lineorders where lo_quantity... and lo_discount... and d_year...
            delete bat6;
            delete batD;
            // batE has in the Head the positions from lineorder and in the Tail the positions from date
            auto batF = batE->mirror_head();// only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
            delete batE;
            MEASURE_OP(batG, matchjoin(batF, batLEenc));
            MEASURE_OP(batH, matchjoin(bat5, batLDenc));
            delete bat5;
            MEASURE_OP(batI, matchjoin(batF, batH));
            delete batF;
            delete batH;

            // 4) check and decode
            MEASURE_OP_TUPLE(tupleI, checkAndDecodeAN(batG));
            CLEAR_CHECKANDDECODE_AN(tupleI);
            auto batK = std::get<0>(tupleI);
            delete batG;
            MEASURE_OP_TUPLE(tupleJ, checkAndDecodeAN(batI));
            CLEAR_CHECKANDDECODE_AN(tupleJ);
            auto batL = std::get<0>(tupleJ);
            delete batI;

            // result
            MEASURE_OP(batM, aggregate_mul_sum<v2_bigint_t>(batK, batL));
            delete batK;
            delete batL;
            auto iter = batM->begin();
            auto result = iter->tail();
            delete iter;
            delete batM;

            ssb::after_query(i, result);
        }

        ssb::after_queries();

        delete batDYenc;
        delete batDDenc;
        delete batLQenc;
        delete batLDenc;
        delete batLOenc;
        delete batLEenc;
        delete batDWenc;

        ssb::finalize();

        return 0;
    });
}

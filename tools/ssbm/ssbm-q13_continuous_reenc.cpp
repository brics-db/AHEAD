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
 * File:   ssbm-q13_continuous_reenc.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 8. December 2016, 01:32
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    return ssb::run(argc, argv, [] (int argc, char ** argv) -> int {
        ssb::init(argc, argv, "SSBM Query 1.3 Continuous Detection With Reencoding");

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

            // 1) select from lineorder
            MEASURE_OP_PAIR(pair1,
                    (selectAN<std::greater_equal, std::less_equal, ahead::and_is>(batLQenc, 26ull * batLQenc->tail.metaData.AN_A, 35ull * batLQenc->tail.metaData.AN_A, std::get<6>(*v2_restiny_t::As),
                                    std::get<6>(*v2_restiny_t::Ainvs))));// lo_quantity between 26 and 35
            CLEAR_SELECT_AN(pair1);
            MEASURE_OP_PAIR(pair2,
                    (selectAN<std::greater_equal, std::less_equal, ahead::and_is>(batLDenc, 5ull * batLDenc->tail.metaData.AN_A, 7ull * batLDenc->tail.metaData.AN_A, std::get<5>(*v2_restiny_t::As),
                                    std::get<5>(*v2_restiny_t::Ainvs))));// lo_discount between 5 and 7
            CLEAR_SELECT_AN(pair2);
            auto bat3 = pair1.first->mirror_head();// prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
            delete pair1.first;
            auto batZ = pair2.first->mirror_head();
            delete pair2.first;
            MEASURE_OP_TUPLE(tuple4, matchjoinAN(bat3, batZ, std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs), std::get<4>(*v2_restiny_t::As), std::get<4>(*v2_restiny_t::Ainvs)));// join selection
            delete bat3;
            delete batZ;
            CLEAR_JOIN_AN(tuple4);
            auto bat5 = std::get<0>(tuple4)->mirror_head();// prepare joined selection with lo_orderdate (contains positions in tail)
            delete std::get<0>(tuple4);
            MEASURE_OP_TUPLE(tuple6, matchjoinAN(bat5, batLOenc, std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs), std::get<12>(*v2_resoid_t::As), std::get<12>(*v2_resoid_t::Ainvs)));// only those lo_orderdates where lo_quantity... and lo_discount
            CLEAR_JOIN_AN(tuple6);

            // 2) select from date (join inbetween to reduce the number of lines we touch in total)
            MEASURE_OP_PAIR(pair7, selectAN<std::equal_to>(batDYenc, 1994ull * batDYenc->tail.metaData.AN_A));// d_year = 1994
            CLEAR_SELECT_AN(pair7);
            auto bat8 = pair7.first->mirror_head();// prepare joined selection over d_year and d_weeknuminyear
            delete pair7.first;
            MEASURE_OP_PAIR(pair9, selectAN<std::equal_to>(batDWenc, 6ull * batDWenc->tail.metaData.AN_A));// d_weeknuminyear = 6
            CLEAR_SELECT_AN(pair9);
            auto batY = pair9.first->mirror_head();
            delete pair9.first;
            MEASURE_OP_TUPLE(tupleA, matchjoinAN(bat8, batY, std::get<11>(*v2_resoid_t::As), std::get<11>(*v2_resoid_t::Ainvs), std::get<14>(*v2_restiny_t::As), std::get<14>(*v2_restiny_t::Ainvs)));
            delete bat8;
            delete batY;
            CLEAR_JOIN_AN(tupleA);
            auto batB = std::get<0>(tupleA)->mirror_head();
            delete std::get<0>(tupleA);
            MEASURE_OP_TUPLE(tupleC, matchjoinAN(batB, batDDenc, std::get<10>(*v2_resoid_t::As), std::get<10>(*v2_resoid_t::Ainvs), std::get<14>(*v2_resint_t::As), std::get<14>(*v2_resint_t::Ainvs)));// only those d_datekey where d_year and d_weeknuminyear...
            delete batB;
            CLEAR_JOIN_AN(tupleC);

            // 3) join lineorder and date
            auto batD = std::get<0>(tupleC)->reverse();
            delete std::get<0>(tupleC);
            MEASURE_OP_TUPLE(tupleE,
                    hashjoinAN(std::get<0>(tuple6), batD, std::get<9>(*v2_resoid_t::As), std::get<9>(*v2_resoid_t::Ainvs), std::get<13>(*v2_resint_t::As), std::get<13>(*v2_resint_t::Ainvs)));// only those lineorders where lo_quantity... and lo_discount... and d_year...
            CLEAR_JOIN_AN(tupleE);
            delete std::get<0>(tuple6);
            delete batD;
            // batE has in the Head the positions from lineorder and in the Tail the positions from date
            auto batF = std::get<0>(tupleE)->mirror_head();// only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
            MEASURE_OP_TUPLE(tupleG, matchjoinAN(batF, batLEenc, std::get<8>(*v2_resoid_t::As), std::get<8>(*v2_resoid_t::Ainvs), std::get<12>(*v2_resint_t::As), std::get<12>(*v2_resint_t::Ainvs)));
            CLEAR_JOIN_AN(tupleG);
            MEASURE_OP_TUPLE(tupleH, matchjoinAN(bat5, batLDenc, std::get<7>(*v2_resoid_t::As), std::get<7>(*v2_resoid_t::Ainvs), std::get<3>(*v2_restiny_t::As), std::get<3>(*v2_restiny_t::Ainvs)));
            CLEAR_JOIN_AN(tupleH);
            delete bat5;
            MEASURE_OP_TUPLE(tupleI,
                    matchjoinAN(batF, std::get<0>(tupleH), std::get<6>(*v2_resoid_t::As), std::get<6>(*v2_resoid_t::Ainvs), std::get<2>(*v2_restiny_t::As), std::get<2>(*v2_restiny_t::Ainvs)));
            CLEAR_JOIN_AN(tupleI);
            delete batF;
            delete std::get<0>(tupleH);

            // 4) result
            MEASURE_OP_TUPLE(tupleK, (aggregate_mul_sumAN<v2_resbigint_t>(std::get<0>(tupleG), std::get<0>(tupleI))));
            delete std::get<0>(tupleG);
            delete std::get<0>(tupleI);
            delete std::get<1>(tupleK);
            delete std::get<2>(tupleK);
            auto iter = std::get<0>(tupleK)->begin();
            auto result = iter->tail() * std::get<0>(tupleK)->tail.metaData.AN_Ainv;
            delete iter;
            delete std::get<0>(tupleK);

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

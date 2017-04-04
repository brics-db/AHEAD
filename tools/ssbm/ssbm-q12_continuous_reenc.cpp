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
 * File:   ssbm-q12_continuous_reenc.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 8. December 2016, 01:32
 */

#include "ssbm.hpp"
#include <column_operators/OperatorsAN.hpp>

int main(int argc, char** argv) {
    SSBM_REQUIRED_VARIABLES("SSBM Query 1.2 Continuous Detection With Reencoding\n===================================================", 24, "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C",
            "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P");

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

        // 1) select from lineorder
        MEASURE_OP_PAIR(pair1,
                (ahead::bat::ops::selectAN(batLQenc, static_cast<restiny_t>(26) * batLQenc->tail.metaData.AN_A, static_cast<restiny_t>(35) * batLQenc->tail.metaData.AN_A, std::get<6>(*v2_restiny_t::As),
                        std::get<6>(*v2_restiny_t::Ainvs)))); // lo_quantity between 26 and 35
        delete pair1.second;
        MEASURE_OP_PAIR(pair2,
                (ahead::bat::ops::selectAN(batLDenc, static_cast<restiny_t>(4) * batLDenc->tail.metaData.AN_A, static_cast<restiny_t>(6) * batLDenc->tail.metaData.AN_A, std::get<5>(*v2_restiny_t::As),
                        std::get<5>(*v2_restiny_t::Ainvs)))); // lo_discount between 4 and 6
        delete pair2.second;
        auto bat3 = pair1.first->mirror_head(); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete pair1.first;
        MEASURE_OP_TUPLE(tuple4,
                (ahead::bat::ops::matchjoinAN(bat3, pair2.first, std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs), std::get<4>(*v2_restiny_t::As), std::get<4>(*v2_restiny_t::Ainvs)))); // join selection
        delete bat3;
        delete pair2.first;
        CLEAR_HASHJOIN_AN(tuple4);
        auto bat5 = std::get<0>(tuple4)->mirror_head(); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP_TUPLE(tuple6,
                (ahead::bat::ops::matchjoinAN(bat5, batLOenc, std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs), std::get<12>(*v2_resoid_t::As), std::get<12>(*v2_resoid_t::Ainvs)))); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        CLEAR_HASHJOIN_AN(tuple6);

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP_PAIR(pair7,
                (ahead::bat::ops::selectAN<std::equal_to>(batDYenc, static_cast<resint_t>(199401) * batDYenc->tail.metaData.AN_A, std::get<14>(*v2_resshort_t::As), std::get<14>(*v2_resshort_t::Ainvs)))); // d_yearmonthnum = 199401
        if (pair7.second)
            delete pair7.second;
        auto bat8 = pair7.first->mirror_head(); // prepare joined selection over d_year and d_datekey
        delete pair7.first;
        MEASURE_OP_TUPLE(tuple9,
                (ahead::bat::ops::matchjoinAN(bat8, batDDenc, std::get<11>(*v2_resoid_t::As), std::get<11>(*v2_resoid_t::Ainvs), std::get<14>(*v2_resint_t::As), std::get<14>(*v2_resint_t::Ainvs)))); // only those d_datekey where d_year...
        delete bat8;
        CLEAR_HASHJOIN_AN(tuple9);

        // 3) join lineorder and date
        auto batA = std::get<0>(tuple9)->reverse();
        delete std::get<0>(tuple9);
        MEASURE_OP_TUPLE(tupleB,
                (ahead::bat::ops::hashjoinAN(std::get<0>(tuple6), batA, std::get<10>(*v2_resoid_t::As), std::get<10>(*v2_resoid_t::Ainvs), std::get<9>(*v2_resoid_t::As), std::get<9>(*v2_resoid_t::Ainvs)))); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete std::get<0>(tuple6);
        delete batA;
        CLEAR_HASHJOIN_AN(tupleB);
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        auto batC = std::get<0>(tupleB)->mirror_head(); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete std::get<0>(tupleB);
        // BatF only contains the 
        MEASURE_OP_TUPLE(tupleD,
                (ahead::bat::ops::matchjoinAN(batC, batLEenc, std::get<8>(*v2_resoid_t::As), std::get<8>(*v2_resoid_t::Ainvs), std::get<13>(*v2_resint_t::As), std::get<13>(*v2_resint_t::Ainvs))));CLEAR_HASHJOIN_AN(
                tupleD);
        MEASURE_OP_TUPLE(tupleE,
                (ahead::bat::ops::matchjoinAN(batC, std::get<0>(tuple4), std::get<7>(*v2_resoid_t::As), std::get<7>(*v2_resoid_t::Ainvs), std::get<3>(*v2_restiny_t::As),
                        std::get<3>(*v2_restiny_t::Ainvs))));
        delete batC;
        delete std::get<0>(tuple4);
        CLEAR_HASHJOIN_AN(tupleE);

        // 4) result
        MEASURE_OP_TUPLE(tupleF, (ahead::bat::ops::aggregate_mul_sumAN<v2_resbigint_t>(std::get<0>(tupleD), std::get<0>(tupleE))));
        delete std::get<0>(tupleD);
        delete std::get<0>(tupleE);
        delete std::get<1>(tupleF);
        delete std::get<2>(tupleF);
        auto iter = std::get<0>(tupleF)->begin();
        auto result = iter->tail() * static_cast<resbigint_t>(std::get<0>(tupleF)->tail.metaData.AN_Ainv);
        delete iter;
        delete std::get<0>(tupleF);

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

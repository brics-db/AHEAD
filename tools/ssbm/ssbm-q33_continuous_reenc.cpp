// Copyright (c) 2017 Till Kolditz
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
 * File:   ssbm-q33_continuous_reenc.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. June 2017, 22:43
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Query 3.3 Continuous Detection With Reencoding");

    SSBM_LOAD("customerAN", "lineorderAN", "supplierAN", "dateAN", "SSBM Q3.3:\n"
            "select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
            "  from customer, lineorder, supplier, date\n"
            "  where lo_custkey = c_custkey\n"
            "    and lo_suppkey = s_suppkey\n"
            "    and lo_orderdate = d_datekey\n"
            "    and (c_city = 'UNITED KI1' or c_city = 'UNITED KI5')\n"
            "    and (s_city = 'UNITED KI1' or s_city = 'UNITED KI5')\n"
            "    and d_year >= 1992 and d_year <= 1997\n"
            "  group by c_city, s_city, d_year;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batCKcb, new resint_colbat_t("customerAN", "custkey"));
    MEASURE_OP(batCCcb, new str_colbat_t("customerAN", "city"));
    MEASURE_OP(batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(batLCcb, new resint_colbat_t("lineorderAN", "custkey"));
    MEASURE_OP(batLScb, new resint_colbat_t("lineorderAN", "suppkey"));
    MEASURE_OP(batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLRcb, new resint_colbat_t("lineorderAN", "revenue"));
    MEASURE_OP(batSScb, new resint_colbat_t("supplierAN", "suppkey"));
    MEASURE_OP(batSCcb, new str_colbat_t("supplierAN", "city"));

    ssb::after_create_columnbats();

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(batCK, copy(batCKcb));
    MEASURE_OP(batCC, copy(batCCcb));
    MEASURE_OP(batDD, copy(batDDcb));
    MEASURE_OP(batDY, copy(batDYcb));
    MEASURE_OP(batLC, copy(batLCcb));
    MEASURE_OP(batLS, copy(batLScb));
    MEASURE_OP(batLO, copy(batLOcb));
    MEASURE_OP(batLR, copy(batLRcb));
    MEASURE_OP(batSS, copy(batSScb));
    MEASURE_OP(batSC, copy(batSCcb));

    delete batCKcb;
    delete batCCcb;
    delete batDDcb;
    delete batDYcb;
    delete batLCcb;
    delete batLScb;
    delete batLOcb;
    delete batLRcb;
    delete batSScb;
    delete batSCcb;

    ssb::before_queries();

    for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
        ssb::before_query();

        // s_city = 'UNITED KI1' or s_city = 'UNITED KI5'
        MEASURE_OP_TUPLE(tuple1, (selectAN<std::equal_to, std::equal_to, OR>(batSC, const_cast<str_t>("UNITED KI1"), const_cast<str_t>("UNITED KI5")))); // OID supplier | s_city
        CLEAR_SELECT_AN(tuple1);
        auto bat2 = std::get<0>(tuple1)->mirror_head(); // OID supplier | OID supplier
        delete std::get<0>(tuple1);
        auto bat3 = batSS->reverse(); // s_suppkey | VOID supplier
        MEASURE_OP_TUPLE(tuple4, matchjoinAN(bat3, bat2, std::get<14>(*v2_resint_t::As), std::get<14>(*v2_resint_t::Ainvs), std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs))); // s_suppkey | OID supplier
        delete bat2;
        delete bat3;
        CLEAR_JOIN_AN(tuple4);
        // lo_suppkey = s_suppkey
        MEASURE_OP_TUPLE(tuple5,
                hashjoinAN(batLS, std::get<0>(tuple4), std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs), std::get<12>(*v2_resoid_t::As), std::get<12>(*v2_resoid_t::Ainvs))); // OID lineorder | OID supplier
        CLEAR_JOIN_AN(tuple5);
        auto bat6 = std::get<0>(tuple5)->mirror_head(); // OID lineorder | OID lineorder
        delete std::get<0>(tuple5);

        // c_city = 'UNITED KI1' or c_city = 'UNITED KI5'
        MEASURE_OP_TUPLE(tuple7, (selectAN<std::equal_to, std::equal_to, OR>(batCC, const_cast<str_t>("UNITED KI1"), const_cast<str_t>("UNITED KI5")))); // OID customer | c_city
        CLEAR_SELECT_AN(tuple7);
        auto bat8 = std::get<0>(tuple7)->mirror_head(); // OID customer | OID customer
        delete std::get<0>(tuple7);
        auto bat9 = batCK->reverse(); // c_custkey | VOID customer
        MEASURE_OP_TUPLE(tuple10, matchjoinAN(bat9, bat8)); // c_custkey | OID customer
        delete bat8;
        delete bat9;
        CLEAR_JOIN_AN(tuple10);
        // reduce number of l_custkey joinpartners
        MEASURE_OP_TUPLE(tuple11, matchjoinAN(bat6, batLC, std::get<13>(*v2_resint_t::As), std::get<13>(*v2_resint_t::Ainvs), std::get<11>(*v2_resoid_t::As), std::get<11>(*v2_resoid_t::Ainvs))); // OID lineorder | l_custkey
        delete bat6;
        CLEAR_JOIN_AN(tuple11);
        // lo_custkey = c_custkey
        MEASURE_OP_TUPLE(tuple12,
                hashjoinAN(std::get<0>(tuple11), std::get<0>(tuple10), std::get<10>(*v2_resoid_t::As), std::get<10>(*v2_resoid_t::Ainvs), std::get<9>(*v2_resoid_t::As),
                        std::get<9>(*v2_resoid_t::Ainvs))); // OID lineorder | OID customer
        delete std::get<0>(tuple11);
        CLEAR_JOIN_AN(tuple12);

        // d_year >= 1992 and d_year <= 1997
        MEASURE_OP_TUPLE(tuple13,
                (selectAN<std::greater_equal, std::less_equal, AND>(batDY, 1992 * batDY->tail.metaData.AN_A, 1997 * batDY->tail.metaData.AN_A, std::get<14>(*v2_resshort_t::As),
                        std::get<14>(*v2_resshort_t::Ainvs)))); // OID date | d_year
        CLEAR_SELECT_AN(tuple13);
        auto bat14 = std::get<0>(tuple13)->mirror_head(); // OID date | OID date
        delete std::get<0>(tuple13);
        MEASURE_OP_TUPLE(tuple15, matchjoinAN(bat14, batDD, std::get<8>(*v2_resoid_t::As), std::get<8>(*v2_resoid_t::Ainvs), std::get<12>(*v2_resint_t::As), std::get<12>(*v2_resint_t::Ainvs))); // OID date | d_datekey
        delete bat14;
        CLEAR_JOIN_AN(tuple15);
        auto bat16 = std::get<0>(tuple15)->reverse(); // d_datekey | OID date
        delete std::get<0>(tuple15);
        auto bat17 = std::get<0>(tuple12)->mirror_head(); // OID lineorder | OID lineorder
        delete std::get<0>(tuple12);
        MEASURE_OP_TUPLE(tuple18, matchjoinAN(bat17, batLO, std::get<7>(*v2_resoid_t::As), std::get<7>(*v2_resoid_t::Ainvs), std::get<11>(*v2_resint_t::As), std::get<11>(*v2_resint_t::Ainvs))); // OID lineorder | lo_orderdate
        delete bat17;
        CLEAR_JOIN_AN(tuple18);
        // lo_orderdate = d_datekey
        MEASURE_OP_TUPLE(tuple19,
                hashjoinAN(std::get<0>(tuple18), bat16, std::get<6>(*v2_resoid_t::As), std::get<6>(*v2_resoid_t::Ainvs), std::get<5>(*v2_resoid_t::As), std::get<5>(*v2_resoid_t::Ainvs))); // OID lineorder | OID date
        delete std::get<0>(tuple18);
        CLEAR_JOIN_AN(tuple19);

        // prepare grouping
        auto bat20 = std::get<0>(tuple19)->mirror_head(); // OID lineorder | OID lineorder
        delete std::get<0>(tuple19);
        auto bat21 = bat20->clear_head(); // VOID | OID lineorder
        delete bat20;
        MEASURE_OP_TUPLE(tupleAR, fetchjoinAN(bat21, batLR, std::get<10>(*v2_resint_t::As), std::get<10>(*v2_resint_t::Ainvs))); // VOID | lo_revenue !!!
        CLEAR_FETCHJOIN_AN(tupleAR);
        MEASURE_OP_TUPLE(tuple22, fetchjoinAN(bat21, batLS, std::get<9>(*v2_resint_t::As), std::get<9>(*v2_resint_t::Ainvs))); // VOID | lo_suppkey
        CLEAR_FETCHJOIN_AN(tuple22);
        MEASURE_OP_TUPLE(tuple23,
                hashjoinAN(std::get<0>(tuple22), std::get<0>(tuple4), std::get<4>(*v2_resoid_t::As), std::get<4>(*v2_resoid_t::Ainvs), std::get<3>(*v2_resoid_t::As), std::get<3>(*v2_resoid_t::Ainvs))); // OID | OID supplier
        delete std::get<0>(tuple4);
        delete std::get<0>(tuple22);
        CLEAR_JOIN_AN(tuple23);
        auto bat24 = std::get<0>(tuple23)->clear_head(); // VOID | OID supplier
        delete std::get<0>(tuple23);
        MEASURE_OP_TUPLE(tupleAS, fetchjoinAN(bat24, batSC)); // VOID | s_city !!!
        delete bat24;
        CLEAR_FETCHJOIN_AN(tupleAS);
        MEASURE_OP_TUPLE(tuple25, fetchjoinAN(bat21, batLC, std::get<8>(*v2_resint_t::As), std::get<8>(*v2_resint_t::Ainvs))); // VOID | lo_custkey
        CLEAR_FETCHJOIN_AN(tuple25);
        MEASURE_OP_TUPLE(tuple26,
                hashjoinAN(std::get<0>(tuple25), std::get<0>(tuple10), std::get<2>(*v2_resoid_t::As), std::get<2>(*v2_resoid_t::Ainvs), std::get<1>(*v2_resoid_t::As),
                        std::get<1>(*v2_resoid_t::Ainvs))); // OID | OID customer
        delete std::get<0>(tuple10);
        delete std::get<0>(tuple25);
        CLEAR_JOIN_AN(tuple26);
        auto bat27 = std::get<0>(tuple26)->clear_head(); // VOID | OID customer
        delete std::get<0>(tuple26);
        MEASURE_OP_TUPLE(tupleAC, fetchjoinAN(bat27, batCC)); // VOID | c_city !!!
        delete bat27;
        CLEAR_FETCHJOIN_AN(tupleAC);
        MEASURE_OP_TUPLE(tuple28, fetchjoinAN(bat21, batLO, std::get<7>(*v2_resint_t::As), std::get<7>(*v2_resint_t::Ainvs))); // VOID | lo_orderdate
        delete bat21;
        CLEAR_FETCHJOIN_AN(tuple28);
        MEASURE_OP_TUPLE(tuple29,
                hashjoinAN(std::get<0>(tuple28), bat16, std::get<0>(*v2_resoid_t::As), std::get<0>(*v2_resoid_t::Ainvs), std::get<15>(*v2_resoid_t::As), std::get<15>(*v2_resoid_t::Ainvs))); // OID | OID date
        delete bat16;
        delete std::get<0>(tuple28);
        CLEAR_JOIN_AN(tuple29);
        auto bat30 = std::get<0>(tuple29)->clear_head(); // VOID | OID date
        delete std::get<0>(tuple29);
        MEASURE_OP_TUPLE(tupleAD, fetchjoinAN(bat30, batDY, std::get<13>(*v2_resshort_t::As), std::get<13>(*v2_resshort_t::Ainvs))); // VOID | d_year !!!
        delete bat30;
        CLEAR_FETCHJOIN_AN(tupleAD);

        // grouping
        MEASURE_OP_TUPLE(tupleGD, groupbyAN(std::get<0>(tupleAD), std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs)));
        CLEAR_GROUPBY_UNARY_AN(tupleGD);
        MEASURE_OP_TUPLE(tupleGS, groupbyAN(std::get<0>(tupleAS), std::get<0>(tupleGD), std::get<1>(tupleGD)->size(), std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs)));
        delete std::get<0>(tupleGD);
        delete std::get<1>(tupleGD);
        CLEAR_GROUPBY_BINARY_AN(tupleGS);
        MEASURE_OP_TUPLE(tupleGC, groupbyAN(std::get<0>(tupleAC), std::get<0>(tupleGS), std::get<1>(tupleGS)->size(), std::get<12>(*v2_resoid_t::As), std::get<12>(*v2_resoid_t::Ainvs)));
        delete std::get<0>(tupleGS);
        delete std::get<1>(tupleGS);
        CLEAR_GROUPBY_BINARY_AN(tupleGC);

        // result
        MEASURE_OP_TUPLE(tupleRR, aggregate_sum_groupedAN<v2_resbigint_t>(std::get<0>(tupleAR), std::get<0>(tupleGC), std::get<1>(tupleGC)->size()));
        delete std::get<0>(tupleAR);
        CLEAR_GROUPEDSUM_AN(tupleRR);
        MEASURE_OP_TUPLE(tupleRD, fetchjoinAN(std::get<1>(tupleGC), std::get<0>(tupleAD), std::get<12>(*v2_resshort_t::As), std::get<12>(*v2_resshort_t::Ainvs)));
        delete std::get<0>(tupleAD);
        CLEAR_FETCHJOIN_AN(tupleRD);
        MEASURE_OP_TUPLE(tupleRC, fetchjoinAN(std::get<1>(tupleGC), std::get<0>(tupleAC)));
        delete std::get<0>(tupleAC);
        CLEAR_FETCHJOIN_AN(tupleRC);
        MEASURE_OP_TUPLE(tupleRS, fetchjoinAN(std::get<1>(tupleGC), std::get<0>(tupleAS)));
        delete std::get<0>(tupleAS);
        delete std::get<0>(tupleGC);
        delete std::get<1>(tupleGC);
        CLEAR_FETCHJOIN_AN(tupleRS);

        auto szResult = std::get<0>(tupleRR)->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            auto iter1 = std::get<0>(tupleRC)->begin();
            auto iter2 = std::get<0>(tupleRS)->begin();
            auto iter3 = std::get<0>(tupleRD)->begin();
            auto iter4 = std::get<0>(tupleRR)->begin();
            typedef typename std::remove_pointer<typename std::decay<decltype(std::get<0>(tupleRR))>::type>::type::tail_t revenue_tail_t;
            revenue_tail_t batRRAinv = static_cast<revenue_tail_t>(std::get<0>(tupleRR)->tail.metaData.AN_Ainv);
            typedef typename std::remove_pointer<typename std::decay<decltype(std::get<0>(tupleRD))>::type>::type::tail_t year_tail_t;
            year_tail_t batRDAinv = static_cast<year_tail_t>(std::get<0>(tupleRD)->tail.metaData.AN_Ainv);
            revenue_tail_t sum = 0;
            std::cerr << "+------------+------------+--------+------------+\n";
            std::cerr << "+     c_city |     s_city | d_year |    revenue |\n";
            std::cerr << "+============+============+========+============+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3, ++*iter4) {
                sum += iter4->tail();
                std::cerr << "| " << std::setw(10) << iter1->tail();
                std::cerr << " | " << std::setw(10) << iter2->tail();
                std::cerr << " | " << std::setw(6) << static_cast<year_tail_t>(iter3->tail() * batRDAinv);
                std::cerr << " | " << std::setw(10) << static_cast<revenue_tail_t>(iter4->tail() * batRRAinv) << " |\n";
            }
            std::cerr << "+============+============+========+============+\n";
            std::cerr << "\t   sum: " << static_cast<revenue_tail_t>(sum * batRRAinv) << std::endl;
            delete iter1;
            delete iter2;
            delete iter3;
            delete iter4;
        }

        delete std::get<0>(tupleRC);
        delete std::get<0>(tupleRS);
        delete std::get<0>(tupleRD);
        delete std::get<0>(tupleRR);
    }

    ssb::after_queries();

    delete batCK;
    delete batCC;
    delete batDD;
    delete batDY;
    delete batLC;
    delete batLS;
    delete batLO;
    delete batLR;
    delete batSS;
    delete batSC;

    ssb::finalize();

    return 0;
}

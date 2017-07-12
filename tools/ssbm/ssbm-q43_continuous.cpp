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
 * File:   ssbm-q43_continuous.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 28. June 2017, 15:20
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Query 4.3 Continuous Detection");

    SSBM_LOAD("dateAN", "customerAN", "supplierAN", "partAN", "lineorderAN", "SSBM Q4.3:\n"
            "select d_year, s_city, p_brand, sum(lo_revenue - lo_supplycost) as profit\n"
            "  from date, customer, supplier, part, lineorder\n"
            "  where lo_custkey = c_custkey\n"
            "    and lo_suppkey = s_suppkey\n"
            "    and lo_partkey = p_partkey\n"
            "    and lo_orderdate = d_datekey\n"
            "    and s_nation = 'UNITED STATES'\n"
            "    and (d_year = 1997 or d_year = 1998)"
            "    and p_category = 'MFGR#14'"
            "  group by d_year, s_city, p_brand;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batCCcb, new resint_colbat_t("customerAN", "custkey"));
    MEASURE_OP(batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(batLCcb, new resint_colbat_t("lineorderAN", "custkey"));
    MEASURE_OP(batLScb, new resint_colbat_t("lineorderAN", "suppkey"));
    MEASURE_OP(batLPcb, new resint_colbat_t("lineorderAN", "partkey"));
    MEASURE_OP(batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLRcb, new resint_colbat_t("lineorderAN", "revenue"));
    MEASURE_OP(batLSCcb, new resint_colbat_t("lineorderAN", "supplycost"));
    MEASURE_OP(batPPcb, new resint_colbat_t("partAN", "partkey"));
    MEASURE_OP(batPBcb, new str_colbat_t("partAN", "brand"));
    MEASURE_OP(batPCcb, new str_colbat_t("partAN", "category"));
    MEASURE_OP(batSScb, new resint_colbat_t("supplierAN", "suppkey"));
    MEASURE_OP(batSNcb, new str_colbat_t("supplierAN", "nation"));
    MEASURE_OP(batSCcb, new str_colbat_t("supplierAN", "city"));

    ssb::after_create_columnbats();

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(batCC, copy(batCCcb));
    MEASURE_OP(batDD, copy(batDDcb));
    MEASURE_OP(batDY, copy(batDYcb));
    MEASURE_OP(batLC, copy(batLCcb));
    MEASURE_OP(batLS, copy(batLScb));
    MEASURE_OP(batLP, copy(batLPcb));
    MEASURE_OP(batLO, copy(batLOcb));
    MEASURE_OP(batLR, copy(batLRcb));
    MEASURE_OP(batLSC, copy(batLSCcb));
    MEASURE_OP(batPP, copy(batPPcb));
    MEASURE_OP(batPB, copy(batPBcb));
    MEASURE_OP(batPC, copy(batPCcb));
    MEASURE_OP(batSS, copy(batSScb));
    MEASURE_OP(batSC, copy(batSCcb));
    MEASURE_OP(batSN, copy(batSNcb));

    delete batCCcb;
    delete batDDcb;
    delete batDYcb;
    delete batLCcb;
    delete batLScb;
    delete batLPcb;
    delete batLOcb;
    delete batLRcb;
    delete batLSCcb;
    delete batPPcb;
    delete batPBcb;
    delete batPCcb;
    delete batSScb;
    delete batSCcb;
    delete batSNcb;

    ssb::before_queries();

    for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
        ssb::before_query();

        // p_category = 'MFGR#14'
        MEASURE_OP_TUPLE(tuple1, (selectAN<std::equal_to>(batPC, const_cast<str_t>("MFGR#14")))); // OID part | p_mfgr
        CLEAR_SELECT_AN(tuple1);
        auto bat2 = std::get<0>(tuple1)->mirror_head(); // OID supplier | OID supplier
        delete std::get<0>(tuple1);
        auto bat3 = batPP->reverse(); // p_partkey | VOID part
        MEASURE_OP_TUPLE(tuple4, matchjoinAN(bat3, bat2)); // p_partkey | OID part
        delete bat2;
        delete bat3;
        CLEAR_JOIN_AN(tuple4);
        // lo_partkey = p_partkey
        MEASURE_OP_TUPLE(tuple5,
                hashjoinAN(batLP, std::get<0>(tuple4), std::get<15>(*v2_resoid_t::As), std::get<15>(*v2_resoid_t::Ainvs), std::get<0>(tuple4)->tail.metaData.AN_A,
                        std::get<0>(tuple4)->tail.metaData.AN_Ainv)); // OID lineorder | OID supplier
        delete std::get<0>(tuple4);
        CLEAR_JOIN_AN(tuple5);
        auto bat6 = std::get<0>(tuple5)->mirror_head(); // OID lineorder | OID lineorder
        delete std::get<0>(tuple5);

        // s_nation = 'UNITED STATES'
        MEASURE_OP_TUPLE(tuple7, selectAN<std::equal_to>(batSN, const_cast<str_t>("UNITED STATES"))); // OID supplier | s_region
        CLEAR_SELECT_AN(tuple7);
        auto bat8 = std::get<0>(tuple7)->mirror_head(); // OID supplier | OID supplier
        delete std::get<0>(tuple7);
        auto bat9 = batSS->reverse(); // s_suppkey | VOID supplier
        MEASURE_OP_TUPLE(tuple10, matchjoinAN(bat9, bat8)); // s_suppkey | OID supplier
        delete bat8;
        delete bat9;
        CLEAR_JOIN_AN(tuple10);
        // reduce number of l_suppkey joinpartners
        MEASURE_OP_TUPLE(tuple11, matchjoinAN(bat6, batLS)); // OID lineorder | lo_suppkey
        delete bat6;
        CLEAR_JOIN_AN(tuple11);
        // lo_suppkey = s_suppkey
        MEASURE_OP_TUPLE(tuple12, hashjoinAN(std::get<0>(tuple11), std::get<0>(tuple10))); // OID lineorder | OID suppkey
        delete std::get<0>(tuple11);
        CLEAR_JOIN_AN(tuple12);
        auto bat13 = std::get<0>(tuple12)->mirror_head(); // OID lineorder | OID lineorder
        delete std::get<0>(tuple12);

        // d_year = 1997 or d_year = 1998
        MEASURE_OP_TUPLE(tuple21, (selectAN<std::equal_to, std::equal_to, OR>(batDY, 1997ull * batDY->tail.metaData.AN_A, 1998ull * batDY->tail.metaData.AN_A))); // OID date | d_year
        CLEAR_SELECT_AN(tuple21);
        auto bat22 = std::get<0>(tuple21)->mirror_head(); // OID date | OID date
        delete std::get<0>(tuple21);
        auto bat23 = batDD->reverse(); // d_datekey | VOID date
        MEASURE_OP_TUPLE(tuple24, matchjoinAN(bat23, bat22)); // d_datekey | OID date
        delete bat22;
        delete bat23;
        CLEAR_JOIN_AN(tuple24);
        // reduce number of lo_datekey joinpartners
        MEASURE_OP_TUPLE(tuple25, matchjoinAN(bat13, batLO)); // OID lineorder | lo_datekey
        delete bat13;
        CLEAR_JOIN_AN(tuple25);
        // lo_orderdate = d_datekey
        MEASURE_OP_TUPLE(tuple26, hashjoinAN(std::get<0>(tuple25), std::get<0>(tuple24))); // OID lineorder | OID date
        delete std::get<0>(tuple25);
        CLEAR_JOIN_AN(tuple26);
        auto bat27 = std::get<0>(tuple26)->mirror_head(); // OID lineorder | OID lineorder
        delete std::get<0>(tuple26);

        // prepare grouping
        auto bat28 = bat27->clear_head(); // VOID | OID lineorder
        delete bat27;
        // profit
        MEASURE_OP_TUPLE(tupleAR, fetchjoinAN(bat28, batLR)); // VOID | lo_revenue
        CLEAR_FETCHJOIN_AN(tupleAR);
        MEASURE_OP_TUPLE(tupleAS, fetchjoinAN(bat28, batLSC)); // VOID | lo_supplycost
        CLEAR_FETCHJOIN_AN(tupleAS);
        MEASURE_OP_TUPLE(tupleAP, (arithmeticAN<SUB, v2_resint_t>(std::get<0>(tupleAR), std::get<0>(tupleAS)))); // VOID | lo_revenue - lo_supplycost (profit !!!)
        delete std::get<0>(tupleAR);
        delete std::get<0>(tupleAS);
        CLEAR_ARITHMETIC_AN(tupleAP);
        // s_nation
        MEASURE_OP_TUPLE(tuple29, fetchjoinAN(bat28, batLS)); // VOID | lo_suppkey
        CLEAR_FETCHJOIN_AN(tuple29);
        MEASURE_OP_TUPLE(tuple30, hashjoinAN(std::get<0>(tuple29), std::get<0>(tuple10))); // OID | OID customer
        delete std::get<0>(tuple10);
        delete std::get<0>(tuple29);
        CLEAR_JOIN_AN(tuple30);
        auto bat31 = std::get<0>(tuple30)->clear_head(); // VOID | OID customer
        delete std::get<0>(tuple30);
        MEASURE_OP_TUPLE(tupleAC, fetchjoinAN(bat31, batSC)); // VOID | s_city !!!
        delete bat31;
        CLEAR_FETCHJOIN_AN(tupleAC);
        // d_year
        MEASURE_OP_TUPLE(tuple32, fetchjoinAN(bat28, batLO)); // VOID | lo_orderdate
        CLEAR_FETCHJOIN_AN(tuple32);
        MEASURE_OP_TUPLE(tuple34, hashjoinAN(std::get<0>(tuple32), std::get<0>(tuple24))); // OID | OID date
        delete std::get<0>(tuple24);
        delete std::get<0>(tuple32);
        auto bat35 = std::get<0>(tuple34)->clear_head(); // VOID | OID date
        delete std::get<0>(tuple34);
        MEASURE_OP_TUPLE(tupleAY, fetchjoinAN(bat35, batDY)); // VOID | d_year !!!
        delete bat35;
        CLEAR_FETCHJOIN_AN(tupleAY);
        // p_category
        MEASURE_OP_TUPLE(tuple36, fetchjoinAN(bat28, batLP)); // VOID | lo_partkey
        delete bat28;
        CLEAR_FETCHJOIN_AN(tuple36);
        auto bat37 = batPP->reverse(); // p_partkey | VOID part
        MEASURE_OP_TUPLE(tuple38,
                hashjoinAN(std::get<0>(tuple36), bat37, std::get<0>(tuple36)->head.metaData.AN_A, std::get<0>(tuple36)->head.metaData.AN_Ainv, std::get<15>(*v2_resoid_t::As),
                        std::get<15>(*v2_resoid_t::Ainvs))); // OID | OID partkey
        delete std::get<0>(tuple36);
        delete bat37;
        CLEAR_JOIN_AN(tuple38);
        auto bat39 = std::get<0>(tuple38)->clear_head(); // VOID | OID partkey
        delete std::get<0>(tuple38);
        MEASURE_OP_TUPLE(tupleAB, fetchjoinAN(bat39, batPB)); // VOID p_brand !!!
        delete bat39;
        CLEAR_FETCHJOIN_AN(tupleAB);

        // grouping
        MEASURE_OP_TUPLE(tupleGY, groupbyAN(std::get<0>(tupleAY)));
        CLEAR_GROUPBY_UNARY_AN(tupleGY);
        MEASURE_OP_TUPLE(tupleGC, groupbyAN(std::get<0>(tupleAC), std::get<0>(tupleGY), std::get<1>(tupleGY)->size()));
        delete std::get<0>(tupleGY);
        delete std::get<1>(tupleGY);
        CLEAR_GROUPBY_BINARY_AN(tupleGC);
        MEASURE_OP_TUPLE(tupleGB, groupbyAN(std::get<0>(tupleAB), std::get<0>(tupleGC), std::get<1>(tupleGC)->size()));
        delete std::get<0>(tupleGC);
        delete std::get<1>(tupleGC);
        CLEAR_GROUPBY_BINARY_AN(tupleGB);

        // result
        MEASURE_OP_TUPLE(tupleRP, aggregate_sum_groupedAN<v2_resbigint_t>(std::get<0>(tupleAP), std::get<0>(tupleGB), std::get<1>(tupleGB)->size()));
        delete std::get<0>(tupleAP);
        CLEAR_GROUPEDSUM_AN(tupleRP);
        MEASURE_OP_TUPLE(tupleRY, fetchjoinAN(std::get<1>(tupleGB), std::get<0>(tupleAY)));
        delete std::get<0>(tupleAY);
        CLEAR_FETCHJOIN_AN(tupleRY);
        MEASURE_OP_TUPLE(tupleRC, fetchjoinAN(std::get<1>(tupleGB), std::get<0>(tupleAC)));
        delete std::get<0>(tupleAC);
        CLEAR_FETCHJOIN_AN(tupleRC);
        MEASURE_OP_TUPLE(tupleRB, fetchjoinAN(std::get<1>(tupleGB), std::get<0>(tupleAB)));
        delete std::get<0>(tupleAB);
        delete std::get<0>(tupleGB);
        delete std::get<1>(tupleGB);
        CLEAR_FETCHJOIN_AN(tupleRB);

        auto szResult = std::get<0>(tupleRP)->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            auto iter1 = std::get<0>(tupleRY)->begin();
            auto iter2 = std::get<0>(tupleRC)->begin();
            auto iter3 = std::get<0>(tupleRB)->begin();
            auto iter4 = std::get<0>(tupleRP)->begin();
            typedef typename std::remove_pointer<typename std::decay<decltype(std::get<0>(tupleRP))>::type>::type::tail_t profit_tail_t;
            profit_tail_t batRPAinv = static_cast<profit_tail_t>(std::get<0>(tupleRP)->tail.metaData.AN_Ainv);
            typedef typename std::remove_pointer<typename std::decay<decltype(std::get<0>(tupleRY))>::type>::type::tail_t year_tail_t;
            year_tail_t batRYAinv = static_cast<year_tail_t>(std::get<0>(tupleRY)->tail.metaData.AN_Ainv);
            profit_tail_t sum = 0;
            std::cerr << "+========+============+============+============+\n";
            std::cerr << "+ d_year |     s_city |    p_brand |     profit |\n";
            std::cerr << "+--------+------------+------------+------------+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3, ++*iter4) {
                sum += iter4->tail();
                std::cerr << "| " << std::setw(6) << static_cast<year_tail_t>(iter1->tail() * batRYAinv);
                std::cerr << " | " << std::setw(10) << iter2->tail();
                std::cerr << " | " << std::setw(10) << iter3->tail();
                std::cerr << " | " << std::setw(10) << static_cast<profit_tail_t>(iter4->tail() * batRPAinv) << " |\n";
            }
            std::cerr << "+========+============+============+============+\n";
            std::cerr << "\t   sum: " << static_cast<profit_tail_t>(sum * batRPAinv) << std::endl;
            delete iter1;
            delete iter2;
            delete iter3;
            delete iter4;
        }

        delete std::get<0>(tupleRY);
        delete std::get<0>(tupleRC);
        delete std::get<0>(tupleRB);
        delete std::get<0>(tupleRP);
    }

    ssb::after_queries();

    delete batCC;
    delete batDD;
    delete batDY;
    delete batLC;
    delete batLS;
    delete batLP;
    delete batLO;
    delete batLR;
    delete batLSC;
    delete batPP;
    delete batPB;
    delete batPC;
    delete batSS;
    delete batSC;
    delete batSN;

    ssb::finalize();

    return 0;
}

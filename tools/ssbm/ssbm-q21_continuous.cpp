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
 * File:   ssbm-q21_continuous.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 20. November 2016, 19:13
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"

int main(int argc, char** argv) {
    SSBM_REQUIRED_VARIABLES("SSBM Query 2.1 Continuous Detection\n===================================", 34, "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I",
            "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");

    SSBM_LOAD("dateAN", "lineorderAN", "partAN", "supplierAN", "SSBM Q2.1:\n"
            "select sum(lo_revenue), d_year, p_brand\n"
            "  from lineorder, part, supplier, date\n"
            "  where lo_orderdate = d_datekey\n"
            "    and lo_partkey = p_partkey\n"
            "    and lo_suppkey = s_suppkey\n"
            "    and p_category = 'MFGR#12'\n"
            "    and s_region = 'AMERICA'\n"
            "  group by d_year, p_brand;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(batLPcb, new resint_colbat_t("lineorderAN", "partkey"));
    MEASURE_OP(batLScb, new resint_colbat_t("lineorderAN", "suppkey"));
    MEASURE_OP(batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLRcb, new resint_colbat_t("lineorderAN", "revenue"));
    MEASURE_OP(batPPcb, new resint_colbat_t("partAN", "partkey"));
    MEASURE_OP(batPCcb, new str_colbat_t("partAN", "category"));
    MEASURE_OP(batPBcb, new str_colbat_t("partAN", "brand"));
    MEASURE_OP(batSScb, new resint_colbat_t("supplierAN", "suppkey"));
    MEASURE_OP(batSRcb, new str_colbat_t("supplierAN", "region"));

    ssb::after_create_columnbats();

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(batDDenc, copy(batDDcb));
    MEASURE_OP(batDYenc, copy(batDYcb));
    MEASURE_OP(batLPenc, copy(batLPcb));
    MEASURE_OP(batLSenc, copy(batLScb));
    MEASURE_OP(batLOenc, copy(batLOcb));
    MEASURE_OP(batLRenc, copy(batLRcb));
    MEASURE_OP(batPPenc, copy(batPPcb));
    MEASURE_OP(batPC, copy(batPCcb));
    MEASURE_OP(batPB, copy(batPBcb));
    MEASURE_OP(batSSenc, copy(batSScb));
    MEASURE_OP(batSR, copy(batSRcb));

    delete batDDcb;
    delete batDYcb;
    delete batLPcb;
    delete batLScb;
    delete batLOcb;
    delete batLRcb;
    delete batPPcb;
    delete batPCcb;
    delete batPBcb;
    delete batSScb;
    delete batSRcb;

    ssb::before_queries();

    for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
        ssb::before_query();

        // s_region = 'AMERICA'
        MEASURE_OP_PAIR(pair1, selectAN<std::equal_to>(batSR, const_cast<str_t>("AMERICA"))); // OID supplier | s_region
        CLEAR_SELECT_AN(pair1);
        auto bat2 = std::get<0>(pair1)->mirror_head(); // OID supplier | OID supplier
        delete std::get<0>(pair1);
        auto bat3 = batSSenc->reverse(); // s_suppkey | OID supplier
        MEASURE_OP_TUPLE(tuple4, matchjoinAN(bat3, bat2)); // s_suppkey | OID supplier
        CLEAR_JOIN_AN(tuple4);
        delete bat2;
        delete bat3;
        // lo_suppkey = s_suppkey
        MEASURE_OP_TUPLE(tuple5,
                hashjoinAN(batLSenc, std::get<0>(tuple4), std::get<v2_resoid_t::As->size() - 1>(*v2_resoid_t::As), std::get<v2_resoid_t::Ainvs->size() - 1>(*v2_resoid_t::Ainvs),
                        std::get<0>(tuple4)->tail.metaData.AN_A, std::get<0>(tuple4)->tail.metaData.AN_Ainv)); // OID lineorder | OID supplier
        CLEAR_JOIN_AN(tuple5);
        delete std::get<0>(tuple4);
        // join with LO_PARTKEY to already reduce the join partners
        auto bat6 = std::get<0>(tuple5)->mirror_head(); // OID lineorder | OID Lineorder
        delete std::get<0>(tuple5);
        MEASURE_OP_TUPLE(tuple7, matchjoinAN(bat6, batLPenc)); // OID lineorder | lo_partkey (where s_region = 'AMERICA')
        CLEAR_JOIN_AN(tuple7);
        delete bat6;

        // p_category = 'MFGR#12'
        MEASURE_OP_PAIR(pair8, selectAN<std::equal_to>(batPC, const_cast<str_t>("MFGR#12"))); // OID part | p_category
        CLEAR_SELECT_AN(pair8);
        auto bat9 = std::get<0>(pair8)->mirror_head(); // OID part | OID part
        delete std::get<0>(pair8);
        auto batA = batPPenc->reverse(); // p_partkey | OID part
        MEASURE_OP_TUPLE(tupleB, matchjoinAN(batA, bat9)); // p_partkey | OID Part where p_category = 'MFGR#12'
        CLEAR_JOIN_AN(tupleB);
        delete batA;
        delete bat9;
        MEASURE_OP_TUPLE(tupleC, hashjoinAN(std::get<0>(tuple7), std::get<0>(tupleB))); // OID lineorder | OID part (where s_region = 'AMERICA' and p_category = 'MFGR#12')
        CLEAR_JOIN_AN(tupleC);
        delete std::get<0>(tuple7);
        delete std::get<0>(tupleB);

        // join with date now!
        auto batD = std::get<0>(tupleC)->mirror_head(); // OID lineorder | OID lineorder  (where ...)
        delete std::get<0>(tupleC);
        MEASURE_OP_TUPLE(tupleE, matchjoinAN(batD, batLOenc)); // OID lineorder | lo_orderdate (where ...)
        CLEAR_JOIN_AN(tupleE);
        delete batD;
        auto batF = batDDenc->reverse(); // d_datekey | OID date
        MEASURE_OP_TUPLE(tupleG,
                hashjoinAN(std::get<0>(tupleE), batF, std::get<0>(tupleE)->head.metaData.AN_A, std::get<0>(tupleE)->head.metaData.AN_Ainv, std::get<v2_resoid_t::As->size() - 1>(*v2_resoid_t::As),
                        std::get<v2_resoid_t::Ainvs->size() - 1>(*v2_resoid_t::Ainvs))); // OID lineorder | OID date (where ..., joined with date)
        CLEAR_JOIN_AN(tupleG);
        delete std::get<0>(tupleE);
        delete batF;

        // now prepare grouped sum and check inputs
        auto batW = std::get<0>(tupleG)->mirror_head(); // OID lineorder | OID lineorder
        MEASURE_OP_TUPLE(tupleX, matchjoinAN(batW, batLPenc)); // OID lineorder | lo_partkey
        CLEAR_JOIN_AN(tupleX);
        auto batY = batPPenc->reverse(); // p_partkey | OID part
        MEASURE_OP_TUPLE(tupleZ,
                hashjoinAN(std::get<0>(tupleX), batY, std::get<0>(tupleX)->head.metaData.AN_A, std::get<0>(tupleX)->head.metaData.AN_Ainv,
                        std::get<ANParametersSelector<v2_resoid_t>::As->size() - 1>(*ANParametersSelector<v2_resoid_t>::As),
                        std::get<ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1>(*ANParametersSelector<v2_resoid_t>::Ainvs))); // OID lineorder | OID part
        CLEAR_JOIN_AN(tupleZ);
        delete std::get<0>(tupleX);
        delete batY;
        MEASURE_OP_TUPLE(tupleAY, matchjoinAN(std::get<0>(tupleG), batDYenc)); // OID lineorder | d_year
        CLEAR_JOIN_AN(tupleAY);
        auto batAY2 = std::get<0>(tupleAY)->clear_head();
        delete std::get<0>(tupleAY);
        MEASURE_OP_TUPLE(tupleAB, matchjoinAN(std::get<0>(tupleZ), batPB)); // OID lineorder | p_brand
        CLEAR_JOIN_AN(tupleAB);
        delete std::get<0>(tupleZ);
        auto batAB2 = std::get<0>(tupleAB)->clear_head();
        delete std::get<0>(tupleAB);
        MEASURE_OP_TUPLE(tupleAR, matchjoinAN(batW, batLRenc)); // OID lineorder | lo_revenue (where ...)
        CLEAR_JOIN_AN(tupleAR);
        auto batAR2 = std::get<0>(tupleAR)->clear_head();
        delete std::get<0>(tupleAR);
        delete batW;
        MEASURE_OP_TUPLE(tupleGY, groupbyAN(batAY2));
        CLEAR_GROUPBY_AN(tupleGY);
        MEASURE_OP_TUPLE(tupleGB, groupbyAN(batAB2, std::get<0>(tupleGY)));
        CLEAR_GROUPBY_AN(tupleGB);
        delete std::get<0>(tupleGY);
        delete std::get<1>(tupleGY);
        MEASURE_OP_TUPLE(tupleRR, aggregate_sum_groupedAN<v2_resbigint_t>(batAR2, std::get<0>(tupleGB), std::get<1>(tupleGB)->size()));
        CLEAR_GROUPEDSUM_AN(tupleRR);
        MEASURE_OP_TUPLE(tupleRY, fetchjoinAN(std::get<1>(tupleGB), batAY2));
        CLEAR_FETCHJOIN_AN(tupleRY);
        delete batAY2;
        MEASURE_OP_TUPLE(tupleRB, fetchjoinAN(std::get<1>(tupleGB), batAB2));
        CLEAR_FETCHJOIN_AN(tupleRB);
        delete batAB2;
        delete std::get<0>(tupleGB);
        delete std::get<1>(tupleGB);

        auto szResult = std::get<0>(tupleRR)->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            size_t sum = 0;
            auto iter1 = std::get<0>(tupleRR)->begin();
            auto iter2 = std::get<0>(tupleRY)->begin();
            auto iter3 = std::get<0>(tupleRB)->begin();
            typedef typename std::remove_pointer<typename std::remove_reference<decltype(std::get<0>(tupleRR))>::type>::type::tail_t revenue_tail_t;
            revenue_tail_t batRRAinv = static_cast<revenue_tail_t>(std::get<0>(tupleRR)->tail.metaData.AN_Ainv);
            typedef typename std::remove_pointer<typename std::remove_reference<decltype(std::get<0>(tupleRY))>::type>::type::tail_t year_tail_t;
            year_tail_t batRYAinv = static_cast<year_tail_t>(std::get<0>(tupleRY)->tail.metaData.AN_Ainv);
            std::cerr << "+------------+--------+-----------+\n";
            std::cerr << "| lo_revenue | d_year | p_brand   |\n";
            std::cerr << "+============+========+===========+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3) {
                sum += iter1->tail() * batRRAinv;
                std::cerr << "| " << std::setw(10) << iter1->tail() * batRRAinv;
                std::cerr << " | " << std::setw(6) << iter2->tail() * batRYAinv;
                std::cerr << " | " << std::setw(9) << iter3->tail() << " |\n";
            }
            std::cerr << "+============+========+===========+\n";
            std::cerr << "\t   sum: " << sum << std::endl;
            delete iter1;
            delete iter2;
            delete iter3;
        }

        delete std::get<0>(tupleRR);
        delete std::get<0>(tupleRY);
        delete std::get<0>(tupleRB);
    }

    ssb::after_queries();

    delete batDDenc;
    delete batDYenc;
    delete batLPenc;
    delete batLSenc;
    delete batLOenc;
    delete batLRenc;
    delete batPPenc;
    delete batPC;
    delete batPB;
    delete batSSenc;
    delete batSR;

    ssb::finalize();

    return 0;
}

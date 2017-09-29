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
 * File:   ssbm-q22_continuous_reenc.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 29. May 2017, 22:27
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Query 2.2 Continuous Detection With Reencoding");

    ssb::loadTables( {"dateAN", "lineorderAN", "partAN", "supplierAN"}, "SSBM Q2.2:\n"
            "select sum(lo_revenue), d_year, p_brand\n"
            "  from lineorder, date, part, supplier\n"
            "  where lo_orderdate = d_datekey\n"
            "    and lo_partkey = p_partkey\n"
            "    and lo_suppkey = s_suppkey\n"
            "    and p_brand between 'MFGR#2221' and 'MFGR#2228'\n"
            "    and s_region = 'ASIA'\n"
            "  group by d_year, p_brand;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(batLPcb, new resint_colbat_t("lineorderAN", "partkey"));
    MEASURE_OP(batLScb, new resint_colbat_t("lineorderAN", "suppkey"));
    MEASURE_OP(batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLRcb, new resint_colbat_t("lineorderAN", "revenue"));
    MEASURE_OP(batPPcb, new resint_colbat_t("partAN", "partkey"));
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
    delete batPBcb;
    delete batSScb;
    delete batSRcb;

    ssb::before_queries();

    for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
        ssb::before_query();

        // s_region = 'ASIA'
        MEASURE_OP_PAIR(pair1, selectAN<std::equal_to>(batSR, const_cast<str_t>("ASIA"))); // OID supplier | s_region
        CLEAR_SELECT_AN(pair1);
        auto bat2 = std::get<0>(pair1)->mirror_head(); // OID supplier | OID supplier
        delete std::get<0>(pair1);
        auto bat3 = batSSenc->reverse(); // s_suppkey | OID supplier
        MEASURE_OP_TUPLE(tuple4, matchjoinAN(bat3, bat2, std::get<14>(*v2_resint_t::As), std::get<14>(*v2_resint_t::Ainvs), std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs))); // s_suppkey | OID supplier
        CLEAR_JOIN_AN(tuple4);
        delete bat2;
        delete bat3;
        // lo_suppkey = s_suppkey
        MEASURE_OP_TUPLE(tuple5,
                hashjoinAN(batLSenc, std::get<0>(tuple4), std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs), std::get<12>(*v2_resoid_t::As), std::get<12>(*v2_resoid_t::Ainvs))); // OID lineorder | OID supplier
        CLEAR_JOIN_AN(tuple5);
        delete std::get<0>(tuple4);
        // join with LO_PARTKEY to already reduce the join partners
        auto bat6 = std::get<0>(tuple5)->mirror_head(); // OID lineorder | OID Lineorder
        delete std::get<0>(tuple5);
        MEASURE_OP_TUPLE(tuple7, matchjoinAN(bat6, batLPenc, std::get<11>(*v2_resoid_t::As), std::get<11>(*v2_resoid_t::Ainvs), std::get<13>(*v2_resint_t::As), std::get<13>(*v2_resint_t::Ainvs))); // OID lineorder | lo_partkey (where s_region = 'AMERICA')
        CLEAR_JOIN_AN(tuple7);
        delete bat6;

        // p_brand between 'MFGR#2221' and 'MFGR#2228'
        MEASURE_OP_PAIR(pair8, (selectAN<std::greater_equal, std::less_equal, ahead::and_is>(batPB, const_cast<str_t>("MFGR#2221"), const_cast<str_t>("MFGR#2228")))); // OID part | p_brand
        CLEAR_SELECT_AN(pair8);
        auto bat9 = std::get<0>(pair8)->mirror_head(); // OID part | OID part
        auto batA = batPPenc->reverse(); // p_partkey | OID part
        MEASURE_OP_TUPLE(tupleB, matchjoinAN(batA, bat9, std::get<12>(*v2_resint_t::As), std::get<12>(*v2_resint_t::Ainvs), std::get<10>(*v2_resoid_t::As), std::get<10>(*v2_resoid_t::Ainvs))); // p_partkey | OID Part where p_category = 'MFGR#12'
        CLEAR_JOIN_AN(tupleB);
        delete batA;
        delete bat9;
        MEASURE_OP_TUPLE(tupleC,
                hashjoinAN(std::get<0>(tuple7), std::get<0>(tupleB), std::get<9>(*v2_resoid_t::As), std::get<9>(*v2_resoid_t::Ainvs), std::get<8>(*v2_resoid_t::As), std::get<8>(*v2_resoid_t::Ainvs))); // OID lineorder | OID part (where s_region = 'AMERICA' and p_category = 'MFGR#12')
        CLEAR_JOIN_AN(tupleC);
        delete std::get<0>(tuple7);
        delete std::get<0>(tupleB);

        // join with date now!
        auto batE = std::get<0>(tupleC)->mirror_head(); // OID lineorder | OID lineorder  (where ...)
        delete std::get<0>(tupleC);
        MEASURE_OP_TUPLE(tupleF, matchjoinAN(batE, batLOenc, std::get<7>(*v2_resoid_t::As), std::get<7>(*v2_resoid_t::Ainvs), std::get<11>(*v2_resint_t::As), std::get<11>(*v2_resint_t::Ainvs))); // OID lineorder | lo_orderdate (where ...)
        CLEAR_JOIN_AN(tupleF);
        delete batE;
        auto batH = batDDenc->reverse(); // d_datekey | OID date
        MEASURE_OP_TUPLE(tupleI,
                hashjoinAN(std::get<0>(tupleF), batH, std::get<6>(*v2_resoid_t::As), std::get<6>(*v2_resoid_t::Ainvs), std::get<5>(*v2_resoid_t::As), std::get<5>(*v2_resoid_t::Ainvs))); // OID lineorder | OID date (where ..., joined with date)
        CLEAR_JOIN_AN(tupleI);
        delete std::get<0>(tupleF);
        delete batH;

        // now prepare grouped sum and check inputs
        auto batW = std::get<0>(tupleI)->mirror_head(); // OID lineorder | OID lineorder
        MEASURE_OP_TUPLE(tupleX, matchjoinAN(batW, batLPenc, std::get<4>(*v2_resoid_t::As), std::get<4>(*v2_resoid_t::Ainvs), std::get<10>(*v2_resint_t::As), std::get<10>(*v2_resint_t::Ainvs))); // OID lineorder | lo_partkey
        CLEAR_JOIN_AN(tupleX);
        auto batY = batPPenc->reverse(); // p_partkey | OID part
        MEASURE_OP_TUPLE(tupleZ,
                hashjoinAN(std::get<0>(tupleX), batY, std::get<3>(*v2_resoid_t::As), std::get<3>(*v2_resoid_t::Ainvs), std::get<2>(*v2_resoid_t::As), std::get<2>(*v2_resoid_t::Ainvs))); // OID lineorder | OID part
        CLEAR_JOIN_AN(tupleZ);
        delete std::get<0>(tupleX);
        delete batY;
        auto batI = std::get<0>(tupleI)->clear_head();
        delete std::get<0>(tupleI);
        MEASURE_OP_TUPLE(tupleAY, fetchjoinAN(batI, batDYenc, std::get<14>(*v2_resshort_t::As), std::get<14>(*v2_resshort_t::Ainvs))); // OID lineorder | d_year
        CLEAR_FETCHJOIN_AN(tupleAY);
        delete batI;
        auto batZ = std::get<0>(tupleZ)->clear_head();
        delete std::get<0>(tupleZ);
        MEASURE_OP_TUPLE(tupleAB, fetchjoinAN(batZ, batPB)); // OID lineorder | p_brand
        CLEAR_FETCHJOIN_AN(tupleAB);
        delete batZ;
        auto batW2 = batW->clear_head();
        delete batW;
        MEASURE_OP_TUPLE(tupleAR, fetchjoinAN(batW2, batLRenc, std::get<9>(*v2_resint_t::As), std::get<9>(*v2_resint_t::Ainvs))); // OID lineorder | lo_revenue (where ...)
        CLEAR_FETCHJOIN_AN(tupleAR);
        delete batW2;

        // grouping
        MEASURE_OP_TUPLE(tupleGY, groupbyAN(std::get<0>(tupleAY))); // we need no reencoding here
        CLEAR_GROUPBY_UNARY_AN(tupleGY);
        MEASURE_OP_TUPLE(tupleGB, groupbyAN(std::get<0>(tupleAB), std::get<0>(tupleGY), std::get<1>(tupleGY)->size())); // we need no reencoding here
        CLEAR_GROUPBY_BINARY_AN(tupleGB);
        delete std::get<0>(tupleGY);
        delete std::get<1>(tupleGY);

        // result
        MEASURE_OP_TUPLE(tupleRR, aggregate_sum_groupedAN<v2_resbigint_t>(std::get<0>(tupleAR), std::get<0>(tupleGB), std::get<1>(tupleGB)->size()));
        CLEAR_GROUPEDSUM_AN(tupleRR);
        delete std::get<0>(tupleAR);
        MEASURE_OP_TUPLE(tupleRY, fetchjoinAN(std::get<1>(tupleGB), std::get<0>(tupleAY), std::get<13>(*v2_resshort_t::As), std::get<13>(*v2_resshort_t::Ainvs)));
        CLEAR_FETCHJOIN_AN(tupleRY);
        delete std::get<0>(tupleAY);
        MEASURE_OP_TUPLE(tupleRB, fetchjoinAN(std::get<1>(tupleGB), std::get<0>(tupleAB)));
        CLEAR_FETCHJOIN_AN(tupleRB);
        delete std::get<0>(tupleAB);
        delete std::get<0>(tupleGB);
        delete std::get<1>(tupleGB);

        auto szResult = std::get<0>(tupleRR)->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            auto iter1 = std::get<0>(tupleRR)->begin();
            auto iter2 = std::get<0>(tupleRY)->begin();
            auto iter3 = std::get<0>(tupleRB)->begin();
            typedef typename std::remove_pointer<typename std::decay<decltype(std::get<0>(tupleRR))>::type>::type::tail_t revenue_tail_t;
            revenue_tail_t batRRAinv = static_cast<revenue_tail_t>(std::get<0>(tupleRR)->tail.metaData.AN_Ainv);
            typedef typename std::remove_pointer<typename std::decay<decltype(std::get<0>(tupleRY))>::type>::type::tail_t year_tail_t;
            year_tail_t batRYAinv = static_cast<year_tail_t>(std::get<0>(tupleRY)->tail.metaData.AN_Ainv);
            revenue_tail_t sum = 0;
            std::cerr << "+============+========+===========+\n";
            std::cerr << "| lo_revenue | d_year | p_brand   |\n";
            std::cerr << "+------------+--------+-----------+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3) {
                sum += iter1->tail();
                std::cerr << "| " << std::setw(10) << static_cast<revenue_tail_t>(iter1->tail() * batRRAinv);
                std::cerr << " | " << std::setw(6) << static_cast<year_tail_t>(iter2->tail() * batRYAinv);
                std::cerr << " | " << std::setw(9) << iter3->tail() << " |\n";
            }
            std::cerr << "+============+========+===========+\n";
            std::cerr << "\t   sum: " << static_cast<revenue_tail_t>(sum * batRRAinv) << std::endl;
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
    delete batPB;
    delete batSSenc;
    delete batSR;

    ssb::finalize();

    return 0;
}

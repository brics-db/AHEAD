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
 * File:   ssbm-q23_continuous_reenc.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 31. May 2017, 15:36
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"

int main(int argc, char** argv) {
    SSBM_REQUIRED_VARIABLES("SSBM Query 2.3 Continuous Detection With Reencoding\n===================================================", 34, "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C",
            "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");

    SSBM_LOAD("dateAN", "lineorderAN", "partAN", "supplierAN", "SSBM Q2.3:\n"
            "select sum(lo_revenue), d_year, p_brand\n"
            "  from lineorder, date, part, supplier\n"
            "  where lo_orderdate = d_datekey\n"
            "    and lo_partkey = p_partkey\n"
            "    and lo_suppkey = s_suppkey\n"
            "    and p_brand = 'MFGR#2239'\n"
            "    and s_region = 'EUROPE'\n"
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

        // s_region = 'EUROPE'
        MEASURE_OP_PAIR(pair1, selectAN<std::equal_to>(batSR, const_cast<str_t>("EUROPE"))); // OID supplier | s_region
        CLEAR_SELECT_AN(pair1);
        auto bat2 = std::get<0>(pair1)->mirror_head(); // OID supplier | OID supplier
        delete std::get<0>(pair1);
        auto bat3 = batSSenc->reverse(); // s_suppkey | OID supplier
        MEASURE_OP_TUPLE(tuple4, matchjoinAN(bat3, bat2, std::get<14>(*v2_resint_t::As), std::get<14>(*v2_resint_t::Ainvs), std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs))); // s_suppkey | OID supplier
        CLEAR_HASHJOIN_AN(tuple4);
        delete bat2;
        delete bat3;
        // lo_suppkey = s_suppkey
        MEASURE_OP_TUPLE(tuple5,
                hashjoinAN(batLSenc, std::get<0>(tuple4), std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs), std::get<12>(*v2_resoid_t::As), std::get<12>(*v2_resoid_t::Ainvs))); // OID lineorder | OID supplier
        CLEAR_HASHJOIN_AN(tuple5);
        delete std::get<0>(tuple4);
        // join with LO_PARTKEY to already reduce the join partners
        auto bat6 = std::get<0>(tuple5)->mirror_head(); // OID lineorder | OID Lineorder
        delete std::get<0>(tuple5);
        MEASURE_OP_TUPLE(tuple7, matchjoinAN(bat6, batLPenc, std::get<11>(*v2_resoid_t::As), std::get<11>(*v2_resoid_t::Ainvs), std::get<13>(*v2_resint_t::As), std::get<13>(*v2_resint_t::Ainvs))); // OID lineorder | lo_partkey (where s_region = 'AMERICA')
        CLEAR_HASHJOIN_AN(tuple7);
        delete bat6;

        // p_brand = 'MFGR#2239'
        MEASURE_OP_PAIR(pair8, selectAN<std::equal_to>(batPB, const_cast<str_t>("MFGR#2239"))); // OID part | p_brand
        CLEAR_SELECT_AN(pair8);
        auto bat9 = std::get<0>(pair8)->mirror_head(); // OID part | OID part
        auto batA = batPPenc->reverse(); // p_partkey | OID part
        MEASURE_OP_TUPLE(tupleB, matchjoinAN(batA, bat9, std::get<12>(*v2_resint_t::As), std::get<12>(*v2_resint_t::Ainvs), std::get<10>(*v2_resoid_t::As), std::get<10>(*v2_resoid_t::Ainvs))); // p_partkey | OID Part where p_category = 'MFGR#12'
        CLEAR_HASHJOIN_AN(tupleB);
        delete batA;
        delete bat9;
        MEASURE_OP_TUPLE(tupleC,
                hashjoinAN(std::get<0>(tuple7), std::get<0>(tupleB), std::get<9>(*v2_resoid_t::As), std::get<9>(*v2_resoid_t::Ainvs), std::get<8>(*v2_resoid_t::As), std::get<8>(*v2_resoid_t::Ainvs))); // OID lineorder | OID part (where s_region = 'AMERICA' and p_category = 'MFGR#12')
        CLEAR_HASHJOIN_AN(tupleC);
        delete std::get<0>(tuple7);
        delete std::get<0>(tupleB);

        // join with date now!
        auto batD = std::get<0>(tupleC)->mirror_head(); // OID lineorder | OID lineorder  (where ...)
        delete std::get<0>(tupleC);
        MEASURE_OP_TUPLE(tupleE, matchjoinAN(batD, batLOenc, std::get<7>(*v2_resoid_t::As), std::get<7>(*v2_resoid_t::Ainvs), std::get<11>(*v2_resint_t::As), std::get<11>(*v2_resint_t::Ainvs))); // OID lineorder | lo_orderdate (where ...)
        CLEAR_HASHJOIN_AN(tupleE);
        delete batD;
        auto batF = batDDenc->reverse(); // d_datekey | OID date
        MEASURE_OP_TUPLE(tupleG,
                hashjoinAN(std::get<0>(tupleE), batF, std::get<6>(*v2_resoid_t::As), std::get<6>(*v2_resoid_t::Ainvs), std::get<5>(*v2_resoid_t::As), std::get<5>(*v2_resoid_t::Ainvs))); // OID lineorder | OID date (where ..., joined with date)
        CLEAR_HASHJOIN_AN(tupleG);
        delete std::get<0>(tupleE);
        delete batF;

        // now prepare grouped sum and check inputs
        auto batH = std::get<0>(tupleG)->mirror_head(); // OID lineorder | OID lineorder
        MEASURE_OP_TUPLE(tupleI, matchjoinAN(batH, batLPenc, std::get<4>(*v2_resoid_t::As), std::get<4>(*v2_resoid_t::Ainvs), std::get<3>(*v2_resoid_t::As), std::get<3>(*v2_resoid_t::Ainvs))); // OID lineorder | lo_partkey
        CLEAR_HASHJOIN_AN(tupleI);
        auto batK = batPPenc->reverse(); // p_partkey | OID part
        MEASURE_OP_TUPLE(tupleL,
                hashjoinAN(std::get<0>(tupleI), batK, std::get<2>(*v2_resoid_t::As), std::get<2>(*v2_resoid_t::Ainvs), std::get<1>(*v2_resoid_t::As), std::get<1>(*v2_resoid_t::Ainvs))); // OID lineorder | OID part
        CLEAR_HASHJOIN_AN(tupleL);
        delete std::get<0>(tupleI);
        delete batK;
        MEASURE_OP_TUPLE(tupleM, hashjoinAN(std::get<0>(tupleL), std::get<0>(pair8), std::get<0>(*v2_resoid_t::As), std::get<0>(*v2_resoid_t::Ainvs))); // OID lineorder | p_brand
        CLEAR_HASHJOIN_AN(tupleM);
        delete std::get<0>(tupleL);
        delete std::get<0>(pair8);

        MEASURE_OP_TUPLE(tupleN,
                hashjoinAN(std::get<0>(tupleG), batDYenc, std::get<15>(*v2_resoid_t::As), std::get<15>(*v2_resoid_t::Ainvs), std::get<14>(*v2_resshort_t::As), std::get<14>(*v2_resshort_t::Ainvs))); // OID lineorder | d_year
        CLEAR_HASHJOIN_AN(tupleN);
        delete std::get<0>(tupleG);

        MEASURE_OP_TUPLE(tupleO, matchjoinAN(batH, batLRenc, std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs), std::get<10>(*v2_resint_t::As), std::get<10>(*v2_resint_t::Ainvs))); // OID lineorder | lo_revenue (where ...)
        CLEAR_HASHJOIN_AN(tupleO);
        delete batH;

        MEASURE_OP_TUPLE(tupleP,
                groupedSumAN<v2_resbigint_t>(std::get<0>(tupleO), std::get<0>(tupleN), std::get<0>(tupleM), std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs),
                        std::get<13>(*v2_resshort_t::As), std::get<13>(*v2_resshort_t::Ainvs), std::get<12>(*v2_resoid_t::As), std::get<12>(*v2_resoid_t::Ainvs), 0, 0)); // the last two parameters are ignored anyways, because the third input BAT has v2_str_t tail!
        CLEAR_GROUPEDSUM_AN(tupleP);
        delete std::get<0>(tupleM);
        delete std::get<0>(tupleN);
        delete std::get<0>(tupleO);

        auto szResult = std::get<0>(tupleP)->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            size_t sum = 0;
            auto iter0 = std::get<0>(tupleP)->begin();
            auto iter1 = std::get<1>(tupleP)->begin();
            auto iter2 = std::get<2>(tupleP)->begin();
            auto iter3 = std::get<3>(tupleP)->begin();
            auto iter4 = std::get<4>(tupleP)->begin();
            // we need the following typedefs to cast the inverses to the correct length
            typedef std::remove_pointer<std::remove_reference<decltype(std::get<0>(tupleP))>::type>::type tupleP_0_t;
            typedef typename TypeMap<tupleP_0_t::v2_tail_t>::v2_encoded_t::type_t K0_tail_enc_t;
            typedef typename TypeMap<tupleP_0_t::v2_tail_t>::v2_base_t::type_t K0_tail_unenc_t;
            typedef std::remove_pointer<std::remove_reference<decltype(std::get<1>(tupleP))>::type>::type tupleP_1_t;
            typedef typename TypeMap<tupleP_1_t::v2_tail_t>::v2_encoded_t::type_t K1_tail_enc_t;
            typedef typename TypeMap<tupleP_1_t::v2_tail_t>::v2_base_t::type_t K1_tail_unenc_t;
            typedef std::remove_pointer<std::remove_reference<decltype(std::get<2>(tupleP))>::type>::type tupleP_2_t;
            typedef typename TypeMap<tupleP_2_t::v2_tail_t>::v2_encoded_t::type_t K2_tail_enc_t;
            typedef typename TypeMap<tupleP_2_t::v2_tail_t>::v2_base_t::type_t K2_tail_unenc_t;
            typedef std::remove_pointer<std::remove_reference<decltype(std::get<3>(tupleP))>::type>::type tupleP_3_t;
            typedef typename TypeMap<tupleP_3_t::v2_tail_t>::v2_encoded_t::type_t K3_tail_enc_t;
            typedef typename TypeMap<tupleP_3_t::v2_tail_t>::v2_base_t::type_t K3_tail_unenc_t;
            // OK now std::get the correct inverses
            auto Ainv0 = static_cast<K0_tail_enc_t>(std::get<0>(tupleP)->tail.metaData.AN_Ainv);
            auto Ainv1 = static_cast<K1_tail_enc_t>(std::get<1>(tupleP)->tail.metaData.AN_Ainv);
            auto Ainv2 = static_cast<K2_tail_enc_t>(std::get<2>(tupleP)->tail.metaData.AN_Ainv);
            auto Ainv3 = static_cast<K3_tail_enc_t>(std::get<3>(tupleP)->tail.metaData.AN_Ainv);
            std::cerr << "+------------+--------+-----------+\n";
            std::cerr << "| lo_revenue | d_year | p_brand   |\n";
            std::cerr << "+============+========+===========+\n";
            for (; iter0->hasNext(); ++*iter0, ++*iter1, ++*iter3) {
                auto value0 = static_cast<K0_tail_unenc_t>(iter0->tail() * Ainv0);
                sum += value0;
                std::cerr << "| " << std::setw(10) << value0;
                auto pos2 = static_cast<K1_tail_unenc_t>(iter1->tail() * Ainv1);
                iter2->position(pos2);
                std::cerr << " | " << std::setw(6) << static_cast<K2_tail_unenc_t>(iter2->tail() * Ainv2);
                auto pos4 = static_cast<K3_tail_unenc_t>(iter3->tail() * Ainv3);
                iter4->position(pos4);
                std::cerr << " | " << std::setw(9) << iter4->tail() << " |\n";
            }
            std::cerr << "+============+========+===========+\n";
            std::cerr << "\t   sum: " << sum << std::endl;
            delete iter0;
            delete iter1;
            delete iter2;
            delete iter3;
            delete iter4;
        }
        delete std::get<0>(tupleP);
        delete std::get<1>(tupleP);
        delete std::get<2>(tupleP);
        delete std::get<3>(tupleP);
        delete std::get<4>(tupleP);
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

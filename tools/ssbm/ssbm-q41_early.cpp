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
 * File:   ssbm-q41_early.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 20. June 2017, 13:22
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Query 4.1 Early Detection");

    SSBM_LOAD("dateAN", "customerAN", "supplierAN", "partAN", "lineorderAN", "SSBM Q4.1:\n"
            "select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit\n"
            "  from date, customer, supplier, part, lineorder\n"
            "  where lo_custkey = c_custkey\n"
            "    and lo_suppkey = s_suppkey\n"
            "    and lo_partkey = p_partkey\n"
            "    and lo_orderdate = d_datekey\n"
            "    and c_region = 'AMERICA'\n"
            "    and s_region = 'AMERICA'\n"
            "    and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')"
            "  group by d_year, c_nation;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batCCcb, new resint_colbat_t("customerAN", "custkey"));
    MEASURE_OP(batCRcb, new str_colbat_t("customerAN", "region"));
    MEASURE_OP(batCNcb, new str_colbat_t("customerAN", "nation"));
    MEASURE_OP(batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(batLCcb, new resint_colbat_t("lineorderAN", "custkey"));
    MEASURE_OP(batLScb, new resint_colbat_t("lineorderAN", "suppkey"));
    MEASURE_OP(batLPcb, new resint_colbat_t("lineorderAN", "partkey"));
    MEASURE_OP(batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLRcb, new resint_colbat_t("lineorderAN", "revenue"));
    MEASURE_OP(batLSCcb, new resint_colbat_t("lineorderAN", "supplycost"));
    MEASURE_OP(batPPcb, new resint_colbat_t("partAN", "partkey"));
    MEASURE_OP(batPMcb, new str_colbat_t("partAN", "mfgr"));
    MEASURE_OP(batSScb, new resint_colbat_t("supplierAN", "suppkey"));
    MEASURE_OP(batSRcb, new str_colbat_t("supplierAN", "region"));

    ssb::after_create_columnbats();

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(batCCenc, copy(batCCcb));
    MEASURE_OP(batCR, copy(batCRcb));
    MEASURE_OP(batCN, copy(batCNcb));
    MEASURE_OP(batDDenc, copy(batDDcb));
    MEASURE_OP(batDYenc, copy(batDYcb));
    MEASURE_OP(batLCenc, copy(batLCcb));
    MEASURE_OP(batLSenc, copy(batLScb));
    MEASURE_OP(batLPenc, copy(batLPcb));
    MEASURE_OP(batLOenc, copy(batLOcb));
    MEASURE_OP(batLRenc, copy(batLRcb));
    MEASURE_OP(batLSCenc, copy(batLSCcb));
    MEASURE_OP(batPPenc, copy(batPPcb));
    MEASURE_OP(batPM, copy(batPMcb));
    MEASURE_OP(batSSenc, copy(batSScb));
    MEASURE_OP(batSR, copy(batSRcb));

    delete batCCcb;
    delete batCRcb;
    delete batCNcb;
    delete batDDcb;
    delete batDYcb;
    delete batLCcb;
    delete batLScb;
    delete batLPcb;
    delete batLOcb;
    delete batLRcb;
    delete batLSCcb;
    delete batPPcb;
    delete batPMcb;
    delete batSScb;
    delete batSRcb;

    ssb::before_queries();

    for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
        ssb::before_query();

        // 0) Eager Check
        MEASURE_OP_TUPLE(tupleCC, checkAndDecodeAN(batCCenc));
        CLEAR_CHECKANDDECODE_AN(tupleCC);
        auto batCC = std::get<0>(tupleCC);
        MEASURE_OP_TUPLE(tupleDD, checkAndDecodeAN(batDDenc));
        CLEAR_CHECKANDDECODE_AN(tupleDD);
        auto batDD = std::get<0>(tupleDD);
        MEASURE_OP_TUPLE(tupleDY, checkAndDecodeAN(batDYenc));
        CLEAR_CHECKANDDECODE_AN(tupleDY);
        auto batDY = std::get<0>(tupleDY);
        MEASURE_OP_TUPLE(tupleLC, checkAndDecodeAN(batLCenc));
        CLEAR_CHECKANDDECODE_AN(tupleLC);
        auto batLC = std::get<0>(tupleLC);
        MEASURE_OP_TUPLE(tupleLS, checkAndDecodeAN(batLSenc));
        CLEAR_CHECKANDDECODE_AN(tupleLS);
        auto batLS = std::get<0>(tupleLS);
        MEASURE_OP_TUPLE(tupleLP, checkAndDecodeAN(batLPenc));
        CLEAR_CHECKANDDECODE_AN(tupleLP);
        auto batLP = std::get<0>(tupleLP);
        MEASURE_OP_TUPLE(tupleLO, checkAndDecodeAN(batLOenc));
        CLEAR_CHECKANDDECODE_AN(tupleLO);
        auto batLO = std::get<0>(tupleLO);
        MEASURE_OP_TUPLE(tupleLR, checkAndDecodeAN(batLRenc));
        CLEAR_CHECKANDDECODE_AN(tupleLR);
        auto batLR = std::get<0>(tupleLR);
        MEASURE_OP_TUPLE(tupleLSC, checkAndDecodeAN(batLSCenc));
        CLEAR_CHECKANDDECODE_AN(tupleLSC);
        auto batLSC = std::get<0>(tupleLSC);
        MEASURE_OP_TUPLE(tuplePP, checkAndDecodeAN(batPPenc));
        CLEAR_CHECKANDDECODE_AN(tuplePP);
        auto batPP = std::get<0>(tuplePP);
        MEASURE_OP_TUPLE(tupleSS, checkAndDecodeAN(batSSenc));
        CLEAR_CHECKANDDECODE_AN(tupleSS);
        auto batSS = std::get<0>(tupleSS);

        // p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2'
        MEASURE_OP(bat1, (select<std::equal_to, std::equal_to, ahead::or_is>(batPM, const_cast<str_t>("MFGR#1"), const_cast<str_t>("MFGR#2")))); // OID part | p_mfgr
        auto bat2 = bat1->mirror_head(); // OID supplier | OID supplier
        delete bat1;
        auto bat3 = batPP->reverse(); // p_partkey | VOID part
        MEASURE_OP(bat4, matchjoin(bat3, bat2)); // p_partkey | OID part
        delete bat2;
        delete bat3;
        // lo_partkey = p_partkey
        MEASURE_OP(bat5, hashjoin(batLP, bat4)); // OID lineorder | OID supplier
        delete bat4;
        auto bat6 = bat5->mirror_head(); // OID lineorder | OID lineorder
        delete bat5;

        // s_region = 'AMERICA'
        MEASURE_OP(bat7, select<std::equal_to>(batSR, const_cast<str_t>("AMERICA"))); // OID supplier | s_region
        auto bat8 = bat7->mirror_head(); // OID supplier | OID supplier
        delete bat7;
        auto bat9 = batSS->reverse(); // s_suppkey | VOID supplier
        MEASURE_OP(bat10, matchjoin(bat9, bat8)); // s_suppkey | OID supplier
        delete bat8;
        delete bat9;
        // reduce number of l_suppkey joinpartners
        MEASURE_OP(bat11, matchjoin(bat6, batLS)); // OID lineorder | lo_suppkey
        delete bat6;
        // lo_suppkey = s_suppkey
        MEASURE_OP(bat12, hashjoin(bat11, bat10)); // OID lineorder | OID suppkey
        delete bat10;
        delete bat11;
        auto bat13 = bat12->mirror_head(); // OID lineorder | OID lineorder
        delete bat12;

        // c_region = 'AMERICA'
        MEASURE_OP(bat14, select<std::equal_to>(batCR, const_cast<str_t>("AMERICA"))); // OID customer | c_region
        auto bat15 = bat14->mirror_head(); // OID customer | OID customer
        delete bat14;
        auto bat16 = batCC->reverse(); // c_custkey | VOID customer
        MEASURE_OP(bat17, matchjoin(bat16, bat15)); // c_custkey | OID customer
        delete bat15;
        delete bat16;
        // reduce number of l_custkey joinpartners
        MEASURE_OP(bat18, matchjoin(bat13, batLC)); // OID lineorder | lo_custkey
        delete bat13;
        // lo_custkey = c_custkey
        MEASURE_OP(bat19, hashjoin(bat18, bat17)); // OID lineorder | OID customer
        delete bat18;

        // prepare grouping
        auto bat20 = bat19->mirror_head(); // OID lineorder | OID lineorder
        delete bat19;
        auto bat21 = bat20->clear_head(); // VOID | OID lineorder
        delete bat20;
        MEASURE_OP(batAR, fetchjoin(bat21, batLR)); // VOID | lo_revenue
        MEASURE_OP(batAS, fetchjoin(bat21, batLSC)); // VOID | lo_supplycost
        MEASURE_OP(batAP, (arithmetic<ahead::sub, v2_int_t>(batAR, batAS))); // VOID | lo_revenue - lo_supplycost !!
        delete batAR;
        delete batAS;
        MEASURE_OP(bat22, fetchjoin(bat21, batLC)); // VOID | lo_custkey
        MEASURE_OP(bat23, hashjoin(bat22, bat17)); // OID | OID customer
        delete bat17;
        delete bat22;
        auto bat24 = bat23->clear_head(); // VOID | OID customer
        delete bat23;
        MEASURE_OP(batAN, fetchjoin(bat24, batCN)); // VOID | c_nation !!!
        delete bat24;
        MEASURE_OP(bat25, fetchjoin(bat21, batLO)); // VOID | lo_orderdate
        delete bat21;
        auto bat29 = batDD->reverse(); // d_datekey | VOID date
        MEASURE_OP(bat30, hashjoin(bat25, bat29)); // OID | OID date
        delete bat25;
        delete bat29;
        auto bat31 = bat30->clear_head(); // VOID | OID date
        delete bat30;
        MEASURE_OP(batAY, fetchjoin(bat31, batDY)); // VOID | d_year !!!
        delete bat31;

        // delete decoded columns
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
        delete batSS;

        // grouping
        MEASURE_OP_PAIR(pairGY, groupby(batAY));
        MEASURE_OP_PAIR(pairGN, groupby(batAN, std::get<0>(pairGY), std::get<1>(pairGY)->size()));
        delete std::get<0>(pairGY);
        delete std::get<1>(pairGY);

        // result
        MEASURE_OP(batRP, aggregate_sum_grouped<v2_bigint_t>(batAP, std::get<0>(pairGN), std::get<1>(pairGN)->size()));
        delete batAP;
        MEASURE_OP(batRN, fetchjoin(std::get<1>(pairGN), batAN));
        delete batAN;
        MEASURE_OP(batRY, fetchjoin(std::get<1>(pairGN), batAY));
        delete batAY;
        delete std::get<0>(pairGN);
        delete std::get<1>(pairGN);

        auto szResult = batRP->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            size_t sum = 0;
            auto iter1 = batRY->begin();
            auto iter2 = batRN->begin();
            auto iter3 = batRP->begin();
            std::cerr << "+========+============+============+\n";
            std::cerr << "+ d_year |   c_nation |     profit |\n";
            std::cerr << "+--------+------------+------------+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3) {
                sum += iter3->tail();
                std::cerr << "| " << std::setw(6) << iter1->tail();
                std::cerr << " | " << std::setw(10) << iter2->tail();
                std::cerr << " | " << std::setw(10) << iter3->tail() << " |\n";
            }
            std::cerr << "+========+============+============+\n";
            std::cerr << "\t   sum: " << sum << std::endl;
            delete iter1;
            delete iter2;
            delete iter3;
        }

        delete batRY;
        delete batRN;
        delete batRP;
    }

    ssb::after_queries();

    delete batCCenc;
    delete batCR;
    delete batCN;
    delete batDDenc;
    delete batDYenc;
    delete batLCenc;
    delete batLSenc;
    delete batLPenc;
    delete batLOenc;
    delete batLRenc;
    delete batLSCenc;
    delete batPPenc;
    delete batPM;
    delete batSSenc;
    delete batSR;

    ssb::finalize();

    return 0;
}

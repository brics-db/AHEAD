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
 * File:   ssbm-q43_late.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 20. June 2017, 14:55
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Query 4.3 Late Detection");

    ssb::loadTables( {"dateAN", "customerAN", "supplierAN", "partAN", "lineorderAN"}, "SSBM Q4.3:\n"
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
    MEASURE_OP(batCCenc, copy(batCCcb));
    MEASURE_OP(batDDenc, copy(batDDcb));
    MEASURE_OP(batDYenc, copy(batDYcb));
    MEASURE_OP(batLCenc, copy(batLCcb));
    MEASURE_OP(batLSenc, copy(batLScb));
    MEASURE_OP(batLPenc, copy(batLPcb));
    MEASURE_OP(batLOenc, copy(batLOcb));
    MEASURE_OP(batLRenc, copy(batLRcb));
    MEASURE_OP(batLSCenc, copy(batLSCcb));
    MEASURE_OP(batPPenc, copy(batPPcb));
    MEASURE_OP(batPB, copy(batPBcb));
    MEASURE_OP(batPC, copy(batPCcb));
    MEASURE_OP(batSSenc, copy(batSScb));
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
        MEASURE_OP(bat1, (select<std::equal_to>(batPC, const_cast<str_t>("MFGR#14")))); // OID part | p_mfgr
        auto bat2 = bat1->mirror_head(); // OID supplier | OID supplier
        delete bat1;
        auto bat3 = batPPenc->reverse(); // p_partkey | VOID part
        MEASURE_OP(bat4, matchjoin(bat3, bat2)); // p_partkey | OID part
        delete bat2;
        delete bat3;
        // lo_partkey = p_partkey
        MEASURE_OP(bat5, hashjoin(batLPenc, bat4)); // OID lineorder | OID supplier
        delete bat4;
        auto bat6 = bat5->mirror_head(); // OID lineorder | OID lineorder
        delete bat5;

        // s_nation = 'UNITED STATES'
        MEASURE_OP(bat7, select<std::equal_to>(batSN, const_cast<str_t>("UNITED STATES"))); // OID supplier | s_region
        auto bat8 = bat7->mirror_head(); // OID supplier | OID supplier
        delete bat7;
        auto bat9 = batSSenc->reverse(); // s_suppkey | VOID supplier
        MEASURE_OP(bat10, matchjoin(bat9, bat8)); // s_suppkey | OID supplier
        delete bat8;
        delete bat9;
        // reduce number of l_suppkey joinpartners
        MEASURE_OP(bat11, matchjoin(bat6, batLSenc)); // OID lineorder | lo_suppkey
        delete bat6;
        // lo_suppkey = s_suppkey
        MEASURE_OP(bat12, hashjoin(bat11, bat10)); // OID lineorder | OID suppkey
        delete bat11;
        auto bat13 = bat12->mirror_head(); // OID lineorder | OID lineorder
        delete bat12;

        // d_year = 1997 or d_year = 1998
        MEASURE_OP(bat21, (select<std::equal_to, std::equal_to, ahead::or_is>(batDYenc, 1997 * batDYenc->tail.metaData.AN_A, 1998 * batDYenc->tail.metaData.AN_A))); // OID date | d_year
        auto bat22 = bat21->mirror_head(); // OID date | OID date
        delete bat21;
        auto bat23 = batDDenc->reverse(); // d_datekey | VOID date
        MEASURE_OP(bat24, matchjoin(bat23, bat22)); // d_datekey | OID date
        delete bat22;
        delete bat23;
        // reduce number of lo_datekey joinpartners
        MEASURE_OP(bat25, matchjoin(bat13, batLOenc)); // OID lineorder | lo_datekey
        delete bat13;
        // lo_orderdate = d_datekey
        MEASURE_OP(bat26, hashjoin(bat25, bat24)); // OID lineorder | OID date
        delete bat25;
        auto bat27 = bat26->mirror_head(); // OID lineorder | OID lineorder
        delete bat26;

        // prepare grouping
        auto bat28 = bat27->clear_head(); // VOID | OID lineorder
        delete bat27;
        // profit
        MEASURE_OP(batARenc, fetchjoin(bat28, batLRenc)); // VOID | lo_revenue
        MEASURE_OP(batASenc, fetchjoin(bat28, batLSCenc)); // VOID | lo_supplycost
        MEASURE_OP(batAPenc, (arithmetic<ahead::sub, v2_resint_t>(batARenc, batASenc))); // VOID | lo_revenue - lo_supplycost (profit !!!)
        delete batARenc;
        delete batASenc;
        // s_nation
        MEASURE_OP(bat29, fetchjoin(bat28, batLSenc)); // VOID | lo_suppkey
        MEASURE_OP(bat30, hashjoin(bat29, bat10)); // OID | OID customer
        delete bat10;
        delete bat29;
        auto bat31 = bat30->clear_head(); // VOID | OID customer
        delete bat30;
        MEASURE_OP(batAC, fetchjoin(bat31, batSC)); // VOID | s_city !!!
        delete bat31;
        // d_year
        MEASURE_OP(bat32, fetchjoin(bat28, batLOenc)); // VOID | lo_orderdate
        MEASURE_OP(bat34, hashjoin(bat32, bat24)); // OID | OID date
        delete bat24;
        delete bat32;
        auto bat35 = bat34->clear_head(); // VOID | OID date
        delete bat34;
        MEASURE_OP(batAYenc, fetchjoin(bat35, batDYenc)); // VOID | d_year !!!
        delete bat35;
        // p_category
        MEASURE_OP(bat36, fetchjoin(bat28, batLPenc)); // VOID | lo_partkey
        delete bat28;
        auto bat37 = batPPenc->reverse(); // p_partkey | VOID part
        MEASURE_OP(bat38, hashjoin(bat36, bat37)); // OID | OID partkey
        delete bat36;
        delete bat37;
        auto bat39 = bat38->clear_head(); // VOID | OID partkey
        delete bat38;
        MEASURE_OP(batAB, fetchjoin(bat39, batPB)); // VOID p_brand !!!
        delete bat39;

        // check and decode
        MEASURE_OP_TUPLE(tupleAP, checkAndDecodeAN(batAPenc));
        CLEAR_CHECKANDDECODE_AN(tupleAP);
        auto batAP = std::get<0>(tupleAP);
        delete batAPenc;
        MEASURE_OP_TUPLE(tupleAY, checkAndDecodeAN(batAYenc));
        CLEAR_CHECKANDDECODE_AN(tupleAY);
        auto batAY = std::get<0>(tupleAY);
        delete batAYenc;

        // grouping
        MEASURE_OP_PAIR(pairGY, groupby(batAY));
        MEASURE_OP_PAIR(pairGC, groupby(batAC, std::get<0>(pairGY), std::get<1>(pairGY)->size()));
        delete std::get<0>(pairGY);
        delete std::get<1>(pairGY);
        MEASURE_OP_PAIR(pairGB, groupby(batAB, std::get<0>(pairGC), std::get<1>(pairGC)->size()));
        delete std::get<0>(pairGC);
        delete std::get<1>(pairGC);

        // result
        MEASURE_OP(batRP, aggregate_sum_grouped<v2_bigint_t>(batAP, std::get<0>(pairGB), std::get<1>(pairGB)->size()));
        delete batAP;
        MEASURE_OP(batRY, fetchjoin(std::get<1>(pairGB), batAY));
        delete batAY;
        MEASURE_OP(batRC, fetchjoin(std::get<1>(pairGB), batAC));
        delete batAC;
        MEASURE_OP(batRB, fetchjoin(std::get<1>(pairGB), batAB));
        delete batAB;
        delete std::get<0>(pairGB);
        delete std::get<1>(pairGB);

        auto szResult = batRP->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            bigint_t sum = 0;
            auto iter1 = batRY->begin();
            auto iter2 = batRC->begin();
            auto iter3 = batRB->begin();
            auto iter4 = batRP->begin();
            std::cerr << "+========+============+============+============+\n";
            std::cerr << "+ d_year |     s_city |    p_brand |     profit |\n";
            std::cerr << "+--------+------------+------------+------------+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3, ++*iter4) {
                sum += iter4->tail();
                std::cerr << "| " << std::setw(6) << iter1->tail();
                std::cerr << " | " << std::setw(10) << iter2->tail();
                std::cerr << " | " << std::setw(10) << iter3->tail();
                std::cerr << " | " << std::setw(10) << iter4->tail() << " |\n";
            }
            std::cerr << "+========+============+============+============+\n";
            std::cerr << "\t   sum: " << sum << std::endl;
            delete iter1;
            delete iter2;
            delete iter3;
            delete iter4;
        }

        delete batRY;
        delete batRC;
        delete batRB;
        delete batRP;
    }

    ssb::after_queries();

    delete batCCenc;
    delete batDDenc;
    delete batDYenc;
    delete batLCenc;
    delete batLSenc;
    delete batLPenc;
    delete batLOenc;
    delete batLRenc;
    delete batLSCenc;
    delete batPPenc;
    delete batPB;
    delete batPC;
    delete batSSenc;
    delete batSC;
    delete batSN;

    ssb::finalize();

    return 0;
}

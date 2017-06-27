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
 * File:   ssbm-q31_late.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 20. June 2017,  14:16
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Query 3.1 Late Detection");

    SSBM_LOAD("customerAN", "lineorderAN", "supplierAN", "dateAN", "SSBM Q3.1:\n"
            "select c_nation, s_nation, d_year, sum(lo_revenue) as revenue\n"
            "  from customer, lineorder, supplier, date\n"
            "  where lo_custkey = c_custkey\n"
            "    and lo_suppkey = s_suppkey\n"
            "    and lo_orderdate = d_datekey\n"
            "    and c_region = 'ASIA'\n"
            "    and s_region = 'ASIA'\n"
            "    and d_year >= 1992 and d_year <= 1997\n"
            "  group by c_nation, s_nation, d_year;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batCCcb, new resint_colbat_t("customerAN", "custkey"));
    MEASURE_OP(batCRcb, new str_colbat_t("customerAN", "region"));
    MEASURE_OP(batCNcb, new str_colbat_t("customerAN", "nation"));
    MEASURE_OP(batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(batLCcb, new resint_colbat_t("lineorderAN", "custkey"));
    MEASURE_OP(batLScb, new resint_colbat_t("lineorderAN", "suppkey"));
    MEASURE_OP(batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLRcb, new resint_colbat_t("lineorderAN", "revenue"));
    MEASURE_OP(batSScb, new resint_colbat_t("supplierAN", "suppkey"));
    MEASURE_OP(batSRcb, new str_colbat_t("supplierAN", "region"));
    MEASURE_OP(batSNcb, new str_colbat_t("supplierAN", "nation"));

    ssb::after_create_columnbats();

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(batCCenc, copy(batCCcb));
    MEASURE_OP(batCR, copy(batCRcb));
    MEASURE_OP(batCN, copy(batCNcb));
    MEASURE_OP(batDDenc, copy(batDDcb));
    MEASURE_OP(batDYenc, copy(batDYcb));
    MEASURE_OP(batLCenc, copy(batLCcb));
    MEASURE_OP(batLSenc, copy(batLScb));
    MEASURE_OP(batLOenc, copy(batLOcb));
    MEASURE_OP(batLRenc, copy(batLRcb));
    MEASURE_OP(batSSenc, copy(batSScb));
    MEASURE_OP(batSR, copy(batSRcb));
    MEASURE_OP(batSN, copy(batSNcb));

    delete batCCcb;
    delete batCRcb;
    delete batCNcb;
    delete batDDcb;
    delete batDYcb;
    delete batLCcb;
    delete batLScb;
    delete batLOcb;
    delete batLRcb;
    delete batSScb;
    delete batSRcb;
    delete batSNcb;

    ssb::before_queries();

    for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
        ssb::before_query();

        // s_region = 'ASIA'
        MEASURE_OP(bat1, select<std::equal_to>(batSR, const_cast<str_t>("ASIA"))); // OID supplier | s_region
        auto bat2 = bat1->mirror_head(); // OID supplier | OID supplier
        delete bat1;
        auto bat3 = batSSenc->reverse(); // s_suppkey | VOID supplier
        MEASURE_OP(bat4, matchjoin(bat3, bat2)); // s_suppkey | OID supplier
        delete bat2;
        delete bat3;
        // lo_suppkey = s_suppkey
        MEASURE_OP(bat5, hashjoin(batLSenc, bat4)); // OID lineorder | OID supplier
        auto bat6 = bat5->mirror_head(); // OID lineorder | OID lineorder
        delete bat5;

        // c_region = 'ASIA'
        MEASURE_OP(bat7, select<std::equal_to>(batCR, const_cast<str_t>("ASIA"))); // OID customer | c_region
        auto bat8 = bat7->mirror_head(); // OID customer | OID customer
        delete bat7;
        auto bat9 = batCCenc->reverse(); // c_custkey | VOID customer
        MEASURE_OP(bat10, matchjoin(bat9, bat8)); // c_custkey | OID customer
        delete bat8;
        delete bat9;
        // reduce number of l_custkey joinpartners
        MEASURE_OP(bat11, matchjoin(bat6, batLCenc)); // OID lineorder | l_custkey
        delete bat6;
        // lo_custkey = c_custkey
        MEASURE_OP(bat12, hashjoin(bat11, bat10)); // OID lineorder | OID customer
        delete bat11;

        // d_year >= 1992 and d_year <= 1997
        MEASURE_OP(bat13, (select<std::greater_equal, std::less_equal, AND>(batDYenc, 1992 * batDYenc->tail.metaData.AN_A, 1997 * batDYenc->tail.metaData.AN_A))); // OID date | d_year
        auto bat14 = bat13->mirror_head(); // OID date | OID date
        delete bat13;
        MEASURE_OP(bat15, matchjoin(bat14, batDDenc)); // OID date | d_datekey
        delete bat14;
        auto bat16 = bat15->reverse(); // d_datekey | OID date
        delete bat15;
        auto bat17 = bat12->mirror_head(); // OID lineorder | OID lineorder
        delete bat12;
        MEASURE_OP(bat18, matchjoin(bat17, batLOenc)); // OID lineorder | lo_orderdate
        delete bat17;
        // lo_orderdate = d_datekey
        MEASURE_OP(bat19, hashjoin(bat18, bat16)); // OID lineorder | OID date
        delete bat18;

        // prepare grouping
        auto bat20 = bat19->mirror_head(); // OID lineorder | OID lineorder
        delete bat19;
        auto bat21 = bat20->clear_head(); // VOID | OID lineorder
        delete bat20;
        MEASURE_OP(batARenc, fetchjoin(bat21, batLRenc)); // VOID | lo_revenue !!!
        MEASURE_OP(bat22, fetchjoin(bat21, batLSenc)); // VOID | lo_suppkey
        MEASURE_OP(bat23, hashjoin(bat22, bat4)); // OID | OID supplier
        delete bat4;
        delete bat22;
        auto bat24 = bat23->clear_head(); // VOID | OID supplier
        delete bat23;
        MEASURE_OP(batAS, fetchjoin(bat24, batSN)); // VOID | s_nation !!!
        delete bat24;
        MEASURE_OP(bat25, fetchjoin(bat21, batLCenc)); // VOID | lo_custkey
        MEASURE_OP(bat26, hashjoin(bat25, bat10)); // OID | OID customer
        delete bat10;
        delete bat25;
        auto bat27 = bat26->clear_head(); // VOID | OID customer
        delete bat26;
        MEASURE_OP(batAC, fetchjoin(bat27, batCN)); // VOID | c_nation !!!
        delete bat27;
        MEASURE_OP(bat28, fetchjoin(bat21, batLOenc)); // VOID | lo_orderdate
        delete bat21;
        MEASURE_OP(bat29, hashjoin(bat28, bat16)); // OID | OID date
        delete bat16;
        delete bat28;
        auto bat30 = bat29->clear_head(); // VOID | OID date
        delete bat29;
        MEASURE_OP(batADenc, fetchjoin(bat30, batDYenc)); // VOID | d_year !!!
        delete bat30;

        // check and decode
        MEASURE_OP_TUPLE(tupleAR, checkAndDecodeAN(batARenc));
        CLEAR_CHECKANDDECODE_AN(tupleAR);
        auto batAR = std::get<0>(tupleAR);
        delete batARenc;
        MEASURE_OP_TUPLE(tupleAD, checkAndDecodeAN(batADenc));
        CLEAR_CHECKANDDECODE_AN(tupleAD);
        auto batAD = std::get<0>(tupleAD);
        delete batADenc;

        // grouping
        MEASURE_OP_PAIR(pairGD, groupby(batAD));
        MEASURE_OP_PAIR(pairGS, groupby(batAS, std::get<0>(pairGD), std::get<1>(pairGD)->size()));
        delete std::get<0>(pairGD);
        delete std::get<1>(pairGD);
        MEASURE_OP_PAIR(pairGC, groupby(batAC, std::get<0>(pairGS), std::get<1>(pairGS)->size()));
        delete std::get<0>(pairGS);
        delete std::get<1>(pairGS);

        // result
        MEASURE_OP(batRR, aggregate_sum_grouped<v2_bigint_t>(batAR, std::get<0>(pairGC), std::get<1>(pairGC)->size()));
        delete batAR;
        MEASURE_OP(batRD, fetchjoin(std::get<1>(pairGC), batAD));
        delete batAD;
        MEASURE_OP(batRC, fetchjoin(std::get<1>(pairGC), batAC));
        delete batAC;
        MEASURE_OP(batRS, fetchjoin(std::get<1>(pairGC), batAS));
        delete batAS;
        delete std::get<0>(pairGC);
        delete std::get<1>(pairGC);

        auto szResult = batRR->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            bigint_t sum = 0;
            auto iter1 = batRC->begin();
            auto iter2 = batRS->begin();
            auto iter3 = batRD->begin();
            auto iter4 = batRR->begin();
            std::cerr << "+=================+=================+========+============+\n";
            std::cerr << "+        c_nation |        s_nation | d_year |    revenue |\n";
            std::cerr << "+-----------------+-----------------+--------+------------+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3, ++*iter4) {
                sum += iter4->tail();
                std::cerr << "| " << std::setw(15) << iter1->tail();
                std::cerr << " | " << std::setw(15) << iter2->tail();
                std::cerr << " | " << std::setw(6) << iter3->tail();
                std::cerr << " | " << std::setw(10) << iter4->tail() << " |\n";
            }
            std::cerr << "+=================+=================+========+============+\n";
            std::cerr << "\t   sum: " << sum << std::endl;
            delete iter1;
            delete iter2;
            delete iter3;
            delete iter4;
        }

        delete batRC;
        delete batRS;
        delete batRD;
        delete batRR;
    }

    ssb::after_queries();

    delete batCCenc;
    delete batCR;
    delete batCN;
    delete batDDenc;
    delete batDYenc;
    delete batLCenc;
    delete batLSenc;
    delete batLOenc;
    delete batLRenc;
    delete batSSenc;
    delete batSR;
    delete batSN;

    ssb::finalize();

    return 0;
}

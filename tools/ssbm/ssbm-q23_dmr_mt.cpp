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
 * File:   ssbm-q23_dmr_mt.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 31. May 2017, 15:36
 */

#include <omp.h>
#include "ssb.hpp"
#include "macros.hpp"
#include <util/ModularRedundant.hpp>

typedef typename std::tuple<BAT<v2_void_t, v2_bigint_t>*, BAT<v2_void_t, v2_shortint_t>*, BAT<v2_void_t, v2_str_t>*> result_tuple_t;
typedef DMRValue<result_tuple_t> DMR;

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Query 2.3 DMR Parallel");

    ssb::loadTables( {"date", "lineorder", "part", "supplier"}, "SSBM Q2.3:\n"
            "select sum(lo_revenue), d_year, p_brand\n"
            "  from lineorder, date, part, supplier\n"
            "  where lo_orderdate = d_datekey\n"
            "    and lo_partkey = p_partkey\n"
            "    and lo_suppkey = s_suppkey\n"
            "    and p_brand = 'MFGR#2239'\n"
            "    and s_region = 'EUROPE'\n"
            "  group by d_year, p_brand;");

    /* Measure loading ColumnBats */
    MEASURE_OP(batDDcb, new int_colbat_t("date", "datekey"));
    MEASURE_OP(batDYcb, new shortint_colbat_t("date", "year"));
    MEASURE_OP(batLPcb, new int_colbat_t("lineorder", "partkey"));
    MEASURE_OP(batLScb, new int_colbat_t("lineorder", "suppkey"));
    MEASURE_OP(batLOcb, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(batLRcb, new int_colbat_t("lineorder", "revenue"));
    MEASURE_OP(batPPcb, new int_colbat_t("part", "partkey"));
    MEASURE_OP(batPBcb, new str_colbat_t("part", "brand"));
    MEASURE_OP(batSScb, new int_colbat_t("supplier", "suppkey"));
    MEASURE_OP(batSRcb, new str_colbat_t("supplier", "region"));

    ssb::after_create_columnbats();

    /* Measure converting (copying) ColumnBats to TempBats */
    int_tmpbat_t * batDDs[DMR::modularity];
    shortint_tmpbat_t * batDYs[DMR::modularity];
    int_tmpbat_t * batLPs[DMR::modularity];
    int_tmpbat_t * batLSs[DMR::modularity];
    int_tmpbat_t * batLOs[DMR::modularity];
    int_tmpbat_t * batLRs[DMR::modularity];
    int_tmpbat_t * batPPs[DMR::modularity];
    str_tmpbat_t * batPBs[DMR::modularity];
    int_tmpbat_t * batSSs[DMR::modularity];
    str_tmpbat_t * batSRs[DMR::modularity];

    for (size_t k = 0; k < DMR::modularity; ++k) {
        MEASURE_OP(batDDs, [k], copy(batDDcb), batDDs[k]);
        MEASURE_OP(batDYs, [k], copy(batDYcb), batDYs[k]);
        MEASURE_OP(batLPs, [k], copy(batLPcb), batLPs[k]);
        MEASURE_OP(batLSs, [k], copy(batLScb), batLSs[k]);
        MEASURE_OP(batLOs, [k], copy(batLOcb), batLOs[k]);
        MEASURE_OP(batLRs, [k], copy(batLRcb), batLRs[k]);
        MEASURE_OP(batPPs, [k], copy(batPPcb), batPPs[k]);
        MEASURE_OP(batPBs, [k], copy(batPBcb), batPBs[k]);
        MEASURE_OP(batSSs, [k], copy(batSScb), batSSs[k]);
        MEASURE_OP(batSRs, [k], copy(batSRcb), batSRs[k]);
    }

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

        DMR results( {nullptr, nullptr, nullptr});

#pragma omp parallel for
        for (size_t k = 0; k < DMR::modularity; ++k) {
            // s_region = 'EUROPE'
            MEASURE_OP(bat1, select<std::equal_to>(batSRs[k], const_cast<str_t>("EUROPE"))); // OID supplier | s_region
            auto bat2 = bat1->mirror_head(); // OID supplier | OID supplier
            delete bat1;
            auto bat3 = batSSs[k]->reverse(); // s_suppkey | OID supplier
            MEASURE_OP(bat4, matchjoin(bat3, bat2)); // s_suppkey | OID supplier
            delete bat2;
            delete bat3;
            // lo_suppkey = s_suppkey
            MEASURE_OP(bat5, hashjoin(batLSs[k], bat4)); // OID lineorder | OID supplier
            delete bat4;
            // join with LO_PARTKEY to already reduce the join partners
            auto bat6 = bat5->mirror_head(); // OID lineorder | OID Lineorder
            delete bat5;
            MEASURE_OP(bat7, matchjoin(bat6, batLPs[k])); // OID lineorder | lo_partkey (where s_region = 'AMERICA')
            delete bat6;

            // p_brand = 'MFGR#2239'
            MEASURE_OP(bat8, select<std::equal_to>(batPBs[k], const_cast<str_t>("MFGR#2239"))); // OID part | p_brand
            auto bat9 = bat8->mirror_head(); // OID part | OID part
            delete bat8;
            auto batA = batPPs[k]->reverse(); // p_partkey | OID part
            MEASURE_OP(batB, matchjoin(batA, bat9)); // p_partkey | OID Part where p_category = 'MFGR#12'
            delete batA;
            delete bat9;
            MEASURE_OP(batC, hashjoin(bat7, batB)); // OID lineorder | OID part (where s_region = 'AMERICA' and p_category = 'MFGR#12')
            delete bat7;
            delete batB;

            // join with date now!
            auto batE = batC->mirror_head(); // OID lineorder | OID lineorder  (where ...)
            delete batC;
            MEASURE_OP(batF, matchjoin(batE, batLOs[k])); // OID lineorder | lo_orderdate (where ...)
            delete batE;
            auto batH = batDDs[k]->reverse(); // d_datekey | OID date
            MEASURE_OP(batI, hashjoin(batF, batH)); // OID lineorder | OID date (where ..., joined with date)
            delete batF;
            delete batH;

            // now prepare grouped sum
            auto batW = batI->mirror_head(); // OID lineorder | OID lineorder
            MEASURE_OP(batX, matchjoin(batW, batLPs[k])); // OID lineorder | lo_partkey
            auto batY = batPPs[k]->reverse(); // p_partkey | OID part
            MEASURE_OP(batZ, hashjoin(batX, batY)); // OID lineorder | OID part
            delete batX;
            delete batY;
            auto batI2 = batI->clear_head();
            delete batI;
            MEASURE_OP(batAY, fetchjoin(batI2, batDYs[k])); // OID lineorder | d_year
            delete batI2;
            auto batZ2 = batZ->clear_head();
            delete batZ;
            MEASURE_OP(batAB, fetchjoin(batZ2, batPBs[k])); // OID lineorder | p_brand
            delete batZ2;
            auto batW2 = batW->clear_head();
            delete batW;
            MEASURE_OP(batAR, fetchjoin(batW2, batLRs[k])); // OID lineorder | lo_revenue (where ...)
            delete batW2;
            MEASURE_OP_PAIR(pairGY, groupby(batAY));
            MEASURE_OP_PAIR(pairGB, groupby(batAB, std::get<0>(pairGY), std::get<1>(pairGY)->size()));
            delete std::get<0>(pairGY);
            delete std::get<1>(pairGY);
            MEASURE_OP(batRR, aggregate_sum_grouped<v2_bigint_t>(batAR, std::get<0>(pairGB), std::get<1>(pairGB)->size()));
            delete batAR;
            MEASURE_OP(batRY, fetchjoin(std::get<1>(pairGB), batAY));
            delete batAY;
            MEASURE_OP(batRB, fetchjoin(std::get<1>(pairGB), batAB));
            delete batAB;
            delete std::get<0>(pairGB);
            delete std::get<1>(pairGB);

#pragma omp critical
            {
                results[k] = std::make_tuple(batRR, batRY, batRB);
            }
        }

        // 5) Voting and Result Printing
        try {
            result_tuple_t content = ahead::vote_majority_tuple(results);
            oid_t szResult = std::get<0>(content)->size();
            ssb::after_query(i, szResult);
            if (ssb::ssb_config.PRINT_RESULT && i == 0) {
                size_t sum = 0;
                auto iter1 = std::get<0>(content)->begin();
                auto iter2 = std::get<1>(content)->begin();
                auto iter3 = std::get<2>(content)->begin();
                std::cerr << "+============+========+===========+\n";
                std::cerr << "| lo_revenue | d_year | p_brand   |\n";
                std::cerr << "+------------+--------+-----------+\n";
                for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter3) {
                    sum += iter1->tail();
                    std::cerr << "| " << std::setw(10) << iter1->tail();
                    std::cerr << " | " << std::setw(6) << iter2->tail();
                    std::cerr << " | " << std::setw(9) << iter3->tail() << " |\n";
                }
                std::cerr << "+============+========+===========+\n";
                std::cerr << "\t   sum: " << sum << std::endl;
                delete iter1;
                delete iter2;
                delete iter3;
            }
        } catch (std::exception & ex) {
            ssb::after_query(i, ex);
            if (ssb::ssb_config.PRINT_RESULT && i == 0) {
                std::cerr << "+============+========+===========+\n";
                std::cerr << "| lo_revenue | d_year | p_brand   |\n";
                std::cerr << "+------------+--------+-----------+\n";
                std::cerr << "| " << ex.what() << "|\n";
                std::cerr << "+============+========+===========+\n";
            }
        }

        for (size_t k = 0; k < DMR::modularity; ++k) {
            delete std::get<0>(results[k]);
            delete std::get<1>(results[k]);
            delete std::get<2>(results[k]);
        }
    }

    ssb::after_queries();

    for (size_t k = 0; k < DMR::modularity; ++k) {
        delete batDDs[k];
        delete batDYs[k];
        delete batLPs[k];
        delete batLSs[k];
        delete batLOs[k];
        delete batLRs[k];
        delete batPPs[k];
        delete batPBs[k];
        delete batSSs[k];
        delete batSRs[k];
    }

    ssb::finalize();

    return 0;
}

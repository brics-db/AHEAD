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
 * File:   ssbm-q22_early.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 29. May 2017, 22:27
 */

#include <column_operators/OperatorsAN.hpp>
#include "ssb.hpp"

int main(int argc, char** argv) {
    SSBM_REQUIRED_VARIABLES("SSBM Query 2.2 Early Detection\n==============================", 34, "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L",
            "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");

    SSBM_LOAD("dateAN", "lineorderAN", "partAN", "supplierAN", "SSBM Q2.2:\n"
            "select sum(lo_revenue), d_year, p_brand\n"
            "  from lineorder, part, supplier, date\n"
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

        // 0) Eager Check
        MEASURE_OP_TUPLE(tupleDD, checkAndDecodeAN(batDDenc));CLEAR_CHECKANDDECODE_AN(tupleDD);
        MEASURE_OP_TUPLE(tupleDY, checkAndDecodeAN(batDYenc));CLEAR_CHECKANDDECODE_AN(tupleDY);
        MEASURE_OP_TUPLE(tupleLP, checkAndDecodeAN(batLPenc));CLEAR_CHECKANDDECODE_AN(tupleLP);
        MEASURE_OP_TUPLE(tupleLS, checkAndDecodeAN(batLSenc));CLEAR_CHECKANDDECODE_AN(tupleLS);
        MEASURE_OP_TUPLE(tupleLO, checkAndDecodeAN(batLOenc));CLEAR_CHECKANDDECODE_AN(tupleLO);
        MEASURE_OP_TUPLE(tupleLR, checkAndDecodeAN(batLRenc));CLEAR_CHECKANDDECODE_AN(tupleLR);
        MEASURE_OP_TUPLE(tuplePP, checkAndDecodeAN(batPPenc));CLEAR_CHECKANDDECODE_AN(tuplePP);
        MEASURE_OP_TUPLE(tupleSS, checkAndDecodeAN(batSSenc));CLEAR_CHECKANDDECODE_AN(tupleSS);

        // s_region = 'AMERICA'
        MEASURE_OP(bat1, select<std::equal_to>(batSR, const_cast<str_t>("AMERICA"))); // OID supplier | s_region
        auto bat2 = bat1->mirror_head(); // OID supplier | OID supplier
        delete bat1;
        auto bat3 = std::get<0>(tupleSS)->reverse(); // s_suppkey | OID supplier
        delete std::get<0>(tupleSS);
        MEASURE_OP(bat4, matchjoin(bat3, bat2)); // s_suppkey | OID supplier
        delete bat2;
        delete bat3;
        // lo_suppkey = s_suppkey
        MEASURE_OP(bat5, hashjoin(std::get<0>(tupleLS), bat4)); // OID lineorder | OID supplier
        delete std::get<0>(tupleLS);
        delete bat4;
        // join with LO_PARTKEY to already reduce the join partners
        auto bat6 = bat5->mirror_head(); // OID lineorder | OID Lineorder
        delete bat5;
        MEASURE_OP(bat7, matchjoin(bat6, std::get<0>(tupleLP))); // OID lineorder | lo_partkey (where s_region = 'AMERICA')
        delete bat6;

        // p_brand between 'MFGR#2221' and 'MFGR#2228'
        MEASURE_OP(bat8, select(batPB, const_cast<str_t>("MFGR#2221"), const_cast<str_t>("MFGR#2228"))); // OID part | p_brand
        auto bat9 = bat8->mirror_head(); // OID part | OID part
        auto batA = std::get<0>(tuplePP)->reverse(); // p_partkey | OID part
        MEASURE_OP(batB, matchjoin(batA, bat9)); // p_partkey | OID Part where p_category = 'MFGR#12'
        delete batA;
        delete bat9;
        MEASURE_OP(batC, hashjoin(bat7, batB)); // OID lineorder | OID part (where s_region = 'AMERICA' and p_category = 'MFGR#12')
        delete bat7;
        delete batB;

        // join with date now!
        auto batE = batC->mirror_head(); // OID lineorder | OID lineorder  (where ...)
        delete batC;
        MEASURE_OP(batF, matchjoin(batE, std::get<0>(tupleLO))); // OID lineorder | lo_orderdate (where ...)
        delete batE;
        delete std::get<0>(tupleLO);
        auto batH = std::get<0>(tupleDD)->reverse(); // d_datekey | OID date
        delete std::get<0>(tupleDD);
        MEASURE_OP(batI, hashjoin(batF, batH)); // OID lineorder | OID date (where ..., joined with date)
        delete batF;
        delete batH;

        // now prepare grouped sum
        auto batW = batI->mirror_head(); // OID lineorder | OID lineorder
        MEASURE_OP(batX, matchjoin(batW, std::get<0>(tupleLP))); // OID lineorder | lo_partkey
        delete std::get<0>(tupleLP);
        auto batY = std::get<0>(tuplePP)->reverse(); // p_partkey | OID part
        delete std::get<0>(tuplePP);
        MEASURE_OP(batZ, hashjoin(batX, batY)); // OID lineorder | OID part
        delete batX;
        delete batY;
        MEASURE_OP(batA1, hashjoin(batZ, bat8)); // OID lineorder | p_brand
        delete batZ;
        delete bat8;

        MEASURE_OP(batA2, hashjoin(batI, std::get<0>(tupleDY))); // OID lineorder | d_year
        delete batI;
        delete std::get<0>(tupleDY);

        MEASURE_OP(batA3, matchjoin(batW, std::get<0>(tupleLR))); // OID lineorder | lo_revenue (where ...)
        delete batW;
        delete std::get<0>(tupleLR);

        MEASURE_OP_TUPLE(tupleK, groupedSum<v2_bigint_t>(batA3, batA2, batA1));
        delete batA1;
        delete batA2;
        delete batA3;

        auto szResult = std::get<0>(tupleK)->size();

        ssb::after_query(i, szResult);

        if (ssb::ssb_config.PRINT_RESULT && i == 0) {
            size_t sum = 0;
            auto iter1 = std::get<0>(tupleK)->begin();
            auto iter2 = std::get<1>(tupleK)->begin();
            auto iter3 = std::get<2>(tupleK)->begin();
            auto iter4 = std::get<3>(tupleK)->begin();
            auto iter5 = std::get<4>(tupleK)->begin();
            std::cerr << "+------------+--------+-----------+\n";
            std::cerr << "| lo_revenue | d_year | p_brand   |\n";
            std::cerr << "+============+========+===========+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter4) {
                sum += iter1->tail();
                std::cerr << "| " << std::setw(10) << iter1->tail();
                iter3->position(iter2->tail());
                std::cerr << " | " << std::setw(6) << iter3->tail();
                iter5->position(iter4->tail());
                std::cerr << " | " << std::setw(9) << iter5->tail() << " |\n";
            }
            std::cerr << "+============+========+===========+\n";
            std::cerr << "\t   sum: " << sum << std::endl;
            delete iter1;
            delete iter2;
            delete iter3;
            delete iter4;
            delete iter5;
        }
        delete std::get<0>(tupleK);
        delete std::get<1>(tupleK);
        delete std::get<2>(tupleK);
        delete std::get<3>(tupleK);
        delete std::get<4>(tupleK);
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

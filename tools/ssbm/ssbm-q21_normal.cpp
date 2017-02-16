// Copyright (c) 2016 Till Kolditz
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
 * File:   ssbm-q21.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 6. November 2016, 22:25
 */

#include "ssbm.hpp"

int
main (int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    std::vector<StopWatch::rep> totalTimes(CONFIG.NUM_RUNS);
    const size_t NUM_OPS = 34;
    cstr_t OP_NAMES[NUM_OPS] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};
    SSBM_REQUIRED_VARIABLES
    size_t x = 0;
    StopWatch sw1, sw2;
    size_t rssBeforeLoad, rssAfterLoad, rssAfterCopy, rssAfterQueries;

    std::cout << "SSBM Query 2.1 Normal\n=====================" << std::endl;

    MetaRepositoryManager::init(CONFIG.DB_PATH.c_str());

    rssBeforeLoad = getPeakRSS(size_enum_t::KB);

    sw1.start();
    // loadTable(CONFIG.DB_PATH, "customer", CONFIG);
    loadTable(CONFIG.DB_PATH, "date", CONFIG);
    loadTable(CONFIG.DB_PATH, "lineorder", CONFIG);
    loadTable(CONFIG.DB_PATH, "part", CONFIG);
    loadTable(CONFIG.DB_PATH, "supplier", CONFIG);
    sw1.stop();
    std::cout << "Total loading time: " << sw1 << " ns." << std::endl;

    rssAfterLoad = getPeakRSS(size_enum_t::KB);

    // select lo_revenue, d_year, p_brand from lineorder, part, supplier, date where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region = 'AMERICA'
    if (CONFIG.VERBOSE) {
        std::cout << "SSBM Q2.1:\n";
        std::cout << "select sum(lo_revenue), d_year, p_brand\n";
        std::cout << "  from lineorder, part, supplier, date\n";
        std::cout << "  where lo_orderdate = d_datekey\n";
        std::cout << "    and lo_partkey = p_partkey\n";
        std::cout << "    and lo_suppkey = s_suppkey\n";
        std::cout << "    and p_category = 'MFGR#12'\n";
        std::cout << "    and s_region = 'AMERICA'\n";
        std::cout << "  group by d_year, p_brand;" << std::endl;
    }

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDDcb, new int_colbat_t("date", "datekey"));
    MEASURE_OP(sw1, x, batDYcb, new shortint_colbat_t("date", "year"));
    MEASURE_OP(sw1, x, batLPcb, new int_colbat_t("lineorder", "partkey"));
    MEASURE_OP(sw1, x, batLScb, new int_colbat_t("lineorder", "suppkey"));
    MEASURE_OP(sw1, x, batLOcb, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(sw1, x, batLRcb, new int_colbat_t("lineorder", "revenue"));
    MEASURE_OP(sw1, x, batPPcb, new int_colbat_t("part", "partkey"));
    MEASURE_OP(sw1, x, batPCcb, new str_colbat_t("part", "category"));
    MEASURE_OP(sw1, x, batPBcb, new str_colbat_t("part", "brand"));
    MEASURE_OP(sw1, x, batSScb, new int_colbat_t("supplier", "suppkey"));
    MEASURE_OP(sw1, x, batSRcb, new str_colbat_t("supplier", "region"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDD, v2::bat::ops::copy(batDDcb));
    MEASURE_OP(sw1, x, batDY, v2::bat::ops::copy(batDYcb));
    MEASURE_OP(sw1, x, batLP, v2::bat::ops::copy(batLPcb));
    MEASURE_OP(sw1, x, batLS, v2::bat::ops::copy(batLScb));
    MEASURE_OP(sw1, x, batLO, v2::bat::ops::copy(batLOcb));
    MEASURE_OP(sw1, x, batLR, v2::bat::ops::copy(batLRcb));
    MEASURE_OP(sw1, x, batPP, v2::bat::ops::copy(batPPcb));
    MEASURE_OP(sw1, x, batPC, v2::bat::ops::copy(batPCcb));
    MEASURE_OP(sw1, x, batPB, v2::bat::ops::copy(batPBcb));
    MEASURE_OP(sw1, x, batSS, v2::bat::ops::copy(batSScb));
    MEASURE_OP(sw1, x, batSR, v2::bat::ops::copy(batSRcb));

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

    rssAfterCopy = getPeakRSS(size_enum_t::KB);

    if (CONFIG.VERBOSE) {
        COUT_HEADLINE;
        COUT_RESULT(0, x);
        std::cout << std::endl;
    }

    SSBM_BEFORE_QUERY

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // s_region = 'AMERICA'
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::select<std::equal_to>(batSR, const_cast<str_t>("AMERICA"))); // OID supplier | s_region
        auto bat2 = bat1->mirror_head(); // OID supplier | OID supplier
        delete bat1;
        auto bat3 = batSS->reverse(); // s_suppkey | OID supplier
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::matchjoin(bat3, bat2)); // s_suppkey | OID supplier
        delete bat2;
        delete bat3;
        // lo_suppkey = s_suppkey
        MEASURE_OP(sw2, x, bat5, v2::bat::ops::hashjoin(batLS, bat4)); // OID lineorder | OID supplier
        delete bat4;
        // join with LO_PARTKEY to already reduce the join partners
        auto bat6 = bat5->mirror_head(); // OID lineorder | OID Lineorder
        delete bat5;
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::matchjoin(bat6, batLP)); // OID lineorder | lo_partkey (where s_region = 'AMERICA')
        delete bat6;

        // p_category = 'MFGR#12'
        MEASURE_OP(sw2, x, bat8, v2::bat::ops::select<std::equal_to>(batPC, const_cast<str_t>("MFGR#12"))); // OID part | p_category
        // p_brand = 'MFGR#121'
        // MEASURE_OP(sw2, x, bat8, v2::bat::ops::select<equal_to>(batPB, "MFGR#121")); // OID part | p_brand
        auto bat9 = bat8->mirror_head(); // OID part | OID part
        delete bat8;
        auto batA = batPP->reverse(); // p_partkey | OID part
        MEASURE_OP(sw2, x, batB, v2::bat::ops::matchjoin(batA, bat9)); // p_partkey | OID Part where p_category = 'MFGR#12'
        delete batA;
        delete bat9;
        MEASURE_OP(sw2, x, batC, v2::bat::ops::hashjoin(bat7, batB)); // OID lineorder | OID part (where s_region = 'AMERICA' and p_category = 'MFGR#12')
        delete bat7;
        delete batB;

        // join with date now!
        auto batE = batC->mirror_head(); // OID lineorder | OID lineorder  (where ...)
        delete batC;
        MEASURE_OP(sw2, x, batF, v2::bat::ops::matchjoin(batE, batLO)); // OID lineorder | lo_orderdate (where ...)
        delete batE;
        auto batH = batDD->reverse(); // d_datekey | OID date
        MEASURE_OP(sw2, x, batI, v2::bat::ops::hashjoin(batF, batH)); // OID lineorder | OID date (where ..., joined with date)
        delete batF;
        delete batH;

        // now prepare grouped sum
        auto batW = batI->mirror_head(); // OID lineorder | OID lineorder
        MEASURE_OP(sw2, x, batX, v2::bat::ops::matchjoin(batW, batLP)); // OID lineorder | lo_partkey
        auto batY = batPP->reverse(); // p_partkey | OID part
        MEASURE_OP(sw2, x, batZ, v2::bat::ops::hashjoin(batX, batY)); // OID lineorder | OID part
        delete batX;
        delete batY;
        MEASURE_OP(sw2, x, batA1, v2::bat::ops::hashjoin(batZ, batPB)); // OID lineorder | p_brand
        delete batZ;

        MEASURE_OP(sw2, x, batA2, v2::bat::ops::hashjoin(batI, batDY)); // OID lineorder | d_year
        delete batI;

        MEASURE_OP(sw2, x, batA3, v2::bat::ops::matchjoin(batW, batLR)); // OID lineorder | lo_revenue (where ...)
        delete batW;

        MEASURE_OP_TUPLE(sw2, x, tupleK, v2::bat::ops::groupedSum<v2_bigint_t>(batA3, batA2, batA1));
        delete batA1;
        delete batA2;
        delete batA3;

        totalTimes[i] = sw1.stop();

        std::cout << "(" << std::setw(2) << i << ")\n\tresult-size: " << std::get<0>(tupleK)->size() << "\n\t  time: " << sw1 << " ns.\n";

        if (CONFIG.PRINT_RESULT && i == 0) {
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

        COUT_HEADLINE;
        COUT_RESULT(0, x, OP_NAMES);
    }

    rssAfterQueries = getPeakRSS(size_enum_t::KB);

    if (CONFIG.VERBOSE) {
        std::cout << "Memory statistics (Resident Set size in KB):\n" << std::setw(16) << "before load: " << rssBeforeLoad << "\n" << std::setw(16) << "after load: " << rssAfterLoad << "\n" << std::setw(16) << "after copy: " << rssAfterCopy << "\n" << std::setw(16) << "after queries: " << rssAfterQueries << "\n";
    }

    std::cout << "TotalTimes:";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << '\n' << std::setw(2) << i << '\t' << totalTimes[i];
    }

    std::cout << "\nMemory:\n" << rssBeforeLoad << '\n' << rssAfterLoad << '\n' << rssAfterCopy << '\n' << rssAfterQueries << std::endl;

    delete batDD;
    delete batDY;
    delete batLP;
    delete batLS;
    delete batLO;
    delete batLR;
    delete batPP;
    delete batPC;
    delete batPB;
    delete batSS;
    delete batSR;

    TransactionManager::destroyInstance();

    SSBM_FINALIZE

    return 0;
}

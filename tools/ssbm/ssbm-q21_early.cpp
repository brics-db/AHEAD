// Copyright (c) 2016 Till Kolditz
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

/* 
 * File:   ssbm-q21_early.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 20. November 2016, 19:13
 */

#include "ssbm.hpp"

int main(int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    StopWatch::rep totalTimes[CONFIG.NUM_RUNS] = {0};
    const size_t NUM_OPS = 42;
    cstr_t OP_NAMES[NUM_OPS] = {"-8", "-7", " - 6", " - 5", " - 4", " - 3", " - 2", " - 1", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};
    StopWatch::rep opTimes[NUM_OPS] = {0};
    size_t batSizes[NUM_OPS] = {0};
    size_t batConsumptions[NUM_OPS] = {0};
    bool hasTwoTypes[NUM_OPS] = {false};
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];
    string emptyString;
    size_t x = 0;

    std::cout << "SSBM Query 2.1 Early Detection\n=====================" << std::endl;

    boost::filesystem::path p(CONFIG.DB_PATH);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    StopWatch sw1, sw2;

    sw1.start();
    // loadTable(baseDir, "customerAN", CONFIG);
    loadTable(baseDir, "dateAN", CONFIG);
    loadTable(baseDir, "lineorderAN", CONFIG);
    loadTable(baseDir, "partAN", CONFIG);
    loadTable(baseDir, "supplierAN", CONFIG);
    sw1.stop();
    std::cout << "Total loading time: " << sw1 << " ns." << std::endl;

    // select lo_revenue, d_year, p_brand from lineorder, part, supplier, date where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region = 'AMERICA'
    if (CONFIG.VERBOSE) {
        std::cout << "\nSSBM Q2.1:\n";
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
    MEASURE_OP(sw1, x, batDDcb, new resint_colbat_t("date", "datekey"));
    MEASURE_OP(sw1, x, batDYcb, new resshort_colbat_t("date", "year"));
    MEASURE_OP(sw1, x, batLPcb, new resint_colbat_t("lineorder", "partkey"));
    MEASURE_OP(sw1, x, batLScb, new resint_colbat_t("lineorder", "suppkey"));
    MEASURE_OP(sw1, x, batLOcb, new resint_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(sw1, x, batLRcb, new resint_colbat_t("lineorder", "revenue"));
    MEASURE_OP(sw1, x, batPPcb, new resint_colbat_t("part", "partkey"));
    MEASURE_OP(sw1, x, batPCcb, new str_colbat_t("part", "category"));
    MEASURE_OP(sw1, x, batPBcb, new str_colbat_t("part", "brand"));
    MEASURE_OP(sw1, x, batSScb, new resint_colbat_t("supplier", "suppkey"));
    MEASURE_OP(sw1, x, batSRcb, new str_colbat_t("supplier", "region"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDDenc, v2::bat::ops::copy(batDDcb));
    MEASURE_OP(sw1, x, batDYenc, v2::bat::ops::copy(batDYcb));
    MEASURE_OP(sw1, x, batLPenc, v2::bat::ops::copy(batLPcb));
    MEASURE_OP(sw1, x, batLSenc, v2::bat::ops::copy(batLScb));
    MEASURE_OP(sw1, x, batLOenc, v2::bat::ops::copy(batLOcb));
    MEASURE_OP(sw1, x, batLRenc, v2::bat::ops::copy(batLRcb));
    MEASURE_OP(sw1, x, batPPenc, v2::bat::ops::copy(batPPcb));
    MEASURE_OP(sw1, x, batPC, v2::bat::ops::copy(batPCcb));
    MEASURE_OP(sw1, x, batPB, v2::bat::ops::copy(batPBcb));
    MEASURE_OP(sw1, x, batSSenc, v2::bat::ops::copy(batSScb));
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

    if (CONFIG.VERBOSE) {
        COUT_HEADLINE;
        COUT_RESULT(0, x);
        std::cout << std::endl;
    }

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 0) Eager Check
        MEASURE_OP_TUPLE(sw2, x, tupleDD, v2::bat::ops::checkAndDecodeAN(batDDenc));
        CLEAR_CHECKANDDECODE_AN(tupleDD);
        MEASURE_OP_TUPLE(sw2, x, tupleDY, v2::bat::ops::checkAndDecodeAN(batDYenc));
        CLEAR_CHECKANDDECODE_AN(tupleDY);
        MEASURE_OP_TUPLE(sw2, x, tupleLP, v2::bat::ops::checkAndDecodeAN(batLPenc));
        CLEAR_CHECKANDDECODE_AN(tupleLP);
        MEASURE_OP_TUPLE(sw2, x, tupleLS, v2::bat::ops::checkAndDecodeAN(batLSenc));
        CLEAR_CHECKANDDECODE_AN(tupleLS);
        MEASURE_OP_TUPLE(sw2, x, tupleLO, v2::bat::ops::checkAndDecodeAN(batLOenc));
        CLEAR_CHECKANDDECODE_AN(tupleLO);
        MEASURE_OP_TUPLE(sw2, x, tupleLR, v2::bat::ops::checkAndDecodeAN(batLRenc));
        CLEAR_CHECKANDDECODE_AN(tupleLR);
        MEASURE_OP_TUPLE(sw2, x, tuplePP, v2::bat::ops::checkAndDecodeAN(batPPenc));
        CLEAR_CHECKANDDECODE_AN(tuplePP);
        MEASURE_OP_TUPLE(sw2, x, tupleSS, v2::bat::ops::checkAndDecodeAN(batSSenc));
        CLEAR_CHECKANDDECODE_AN(tupleSS);

        // s_region = 'AMERICA'
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::select<equal_to>(batSR, "AMERICA")); // OID supplier | s_region
        MEASURE_OP(sw2, x, bat2, bat1->mirror_head()); // OID supplier | OID supplier
        delete bat1;
        MEASURE_OP(sw2, x, bat3, get<0>(tupleSS)->reverse()); // s_suppkey | OID supplier
        delete get<0>(tupleSS);
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::hashjoin(bat3, bat2)); // s_suppkey | OID supplier
        delete bat2;
        delete bat3;
        // lo_suppkey = s_suppkey
        MEASURE_OP(sw2, x, bat5, v2::bat::ops::hashjoin(get<0>(tupleLS), bat4)); // OID lineorder | OID supplier
        delete get<0>(tupleLS);
        delete bat4;
        // join with LO_PARTKEY to already reduce the join partners
        MEASURE_OP(sw2, x, bat6, bat5->mirror_head()); // OID lineorder | OID Lineorder
        delete bat5;
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::hashjoin(bat6, get<0>(tupleLP))); // OID lineorder | lo_partkey (where s_region = 'AMERICA')
        delete bat6;

        // p_category = 'MFGR#12'
        MEASURE_OP(sw2, x, bat8, v2::bat::ops::select<equal_to>(batPC, "MFGR#12")); // OID part | p_category
        // p_brand = 'MFGR#121'
        // MEASURE_OP(sw2, x, bat8, v2::bat::ops::select<equal_to>(batPB, "MFGR#121")); // OID part | p_brand
        MEASURE_OP(sw2, x, bat9, bat8->mirror_head()); // OID part | OID part
        delete bat8;
        MEASURE_OP(sw2, x, batA, get<0>(tuplePP)->reverse()); // p_partkey | OID part
        MEASURE_OP(sw2, x, batB, v2::bat::ops::hashjoin(batA, bat9)); // p_partkey | OID Part where p_category = 'MFGR#12'
        delete batA;
        delete bat9;
        MEASURE_OP(sw2, x, batC, v2::bat::ops::hashjoin(bat7, batB)); // OID lineorder | OID part (where s_region = 'AMERICA' and p_category = 'MFGR#12')
        delete bat7;
        delete batB;

        // join with date now!
        MEASURE_OP(sw2, x, batE, batC->mirror_head()); // OID lineorder | OID lineorder  (where ...)
        delete batC;
        MEASURE_OP(sw2, x, batF, v2::bat::ops::hashjoin(batE, get<0>(tupleLO))); // OID lineorder | lo_orderdate (where ...)
        delete batE;
        delete get<0>(tupleLO);
        MEASURE_OP(sw2, x, batH, get<0>(tupleDD)->reverse()); // d_datekey | OID date
        delete get<0>(tupleDD);
        MEASURE_OP(sw2, x, batI, v2::bat::ops::hashjoin(batF, batH)); // OID lineorder | OID date (where ..., joined with date)
        delete batF;
        delete batH;

        // now prepare grouped sum
        MEASURE_OP(sw2, x, batW, batI->mirror_head()); // OID lineorder | OID lineorder
        MEASURE_OP(sw2, x, batX, v2::bat::ops::hashjoin(batW, get<0>(tupleLP))); // OID lineorder | lo_partkey
        delete get<0>(tupleLP);
        MEASURE_OP(sw2, x, batY, get<0>(tuplePP)->reverse()); // p_partkey | OID part
        delete get<0>(tuplePP);
        MEASURE_OP(sw2, x, batZ, v2::bat::ops::hashjoin(batX, batY)); // OID lineorder | OID part
        delete batX;
        delete batY;
        MEASURE_OP(sw2, x, batA1, v2::bat::ops::hashjoin(batZ, batPB)); // OID lineorder | p_brand
        delete batZ;

        MEASURE_OP(sw2, x, batA2, v2::bat::ops::hashjoin(batI, get<0>(tupleDY))); // OID lineorder | d_year
        delete batI;
        delete get<0>(tupleDY);

        MEASURE_OP(sw2, x, batA3, v2::bat::ops::hashjoin(batW, get<0>(tupleLR))); // OID lineorder | lo_revenue (where ...)
        delete batW;
        delete get<0>(tupleLR);

        MEASURE_OP_TUPLE(sw2, x, tupleK, v2::bat::ops::groupedSum<v2_bigint_t>(batA3, batA2, batA1));
        delete batA1;
        delete batA2;
        delete batA3;

        totalTimes[i] = sw1.stop();

        std::cout << "\n(" << setw(2) << i << ")\n\tresult: " << get<0>(tupleK)->size() << "\n\t  time: " << sw1 << " ns.";

        if (CONFIG.VERBOSE && i == 0) {
            size_t sum = 0;
            auto iter1 = get<0>(tupleK)->begin();
            auto iter2 = get<1>(tupleK)->begin();
            auto iter3 = get<2>(tupleK)->begin();
            auto iter4 = get<3>(tupleK)->begin();
            auto iter5 = get<4>(tupleK)->begin();
            std::cerr << "+------------+--------+-----------+\n";
            std::cerr << "| lo_revenue | d_year | p_brand   |\n";
            std::cerr << "+============+========+===========+\n";
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter4) {
                sum += iter1->tail();
                std::cerr << "| " << setw(10) << iter1->tail();
                iter3->position(iter2->tail());
                std::cerr << " | " << setw(6) << iter3->tail();
                iter5->position(iter4->tail());
                std::cerr << " | " << setw(9) << iter5->tail() << " |\n";
            }
            std::cout << "\t   sum: " << sum << std::endl;
            delete iter1;
            delete iter2;
            delete iter3;
            delete iter4;
            delete iter5;
        }

        COUT_HEADLINE;
        COUT_RESULT(0, x, OP_NAMES);
    }

    if (CONFIG.VERBOSE) {
        std::cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.";
    }

    std::cout << "\nTotalTimes:";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << '\n' << setw(2) << i << '\t' << totalTimes[i];
    }
    std::cout << std::endl;

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

    TransactionManager::destroyInstance();

    return 0;
}
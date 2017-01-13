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
 * File:   ssbm-q21_continuous.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 20. November 2016, 19:13
 */

#include "ssbm.hpp"
#include <column_operators/OperatorsAN.hpp>

int
main (int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    std::vector<StopWatch::rep> totalTimes(CONFIG.NUM_RUNS);
    const size_t NUM_OPS = 34;
    cstr_t OP_NAMES[NUM_OPS] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};
    StopWatch::rep opTimes[NUM_OPS] = {0};
    size_t batSizes[NUM_OPS] = {0};
    size_t batConsumptions[NUM_OPS] = {0};
    bool hasTwoTypes[NUM_OPS] = {false};
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];
    std::string emptyString;
    size_t x = 0;
    StopWatch sw1, sw2;

    std::cout << "SSBM Query 2.1 Continuous Detection\n===================================" << std::endl;

    MetaRepositoryManager::init(CONFIG.DB_PATH.c_str());

    sw1.start();
    // loadTable(CONFIG.DB_PATH, "customerAN", CONFIG);
    loadTable(CONFIG.DB_PATH, "dateAN", CONFIG);
    loadTable(CONFIG.DB_PATH, "lineorderAN", CONFIG);
    loadTable(CONFIG.DB_PATH, "partAN", CONFIG);
    loadTable(CONFIG.DB_PATH, "supplierAN", CONFIG);
    sw1.stop();
    std::cout << "Total loading time: " << sw1 << " ns." << std::endl;

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

        // s_region = 'AMERICA'
        MEASURE_OP_PAIR(sw2, x, pair1, v2::bat::ops::selectAN<std::equal_to>(batSR, const_cast<str_t>("AMERICA"))); // OID supplier | s_region
        CLEAR_SELECT_AN(pair1);
        auto bat2 = std::get<0>(pair1)->mirror_head(); // OID supplier | OID supplier
        delete std::get<0>(pair1);
        auto bat3 = batSSenc->reverse(); // s_suppkey | OID supplier
        MEASURE_OP_TUPLE(sw2, x, tuple4, v2::bat::ops::matchjoinAN(bat3, bat2)); // s_suppkey | OID supplier
        CLEAR_HASHJOIN_AN(tuple4);
        delete bat2;
        delete bat3;
        // lo_suppkey = s_suppkey
        MEASURE_OP_TUPLE(sw2, x, tuple5, v2::bat::ops::hashjoinAN(batLSenc, std::get<0>(tuple4), std::get < v2_resoid_t::As->size() - 1 > (*v2_resoid_t::As), std::get < v2_resoid_t::Ainvs->size() - 1 > (*v2_resoid_t::Ainvs), std::get<0>(tuple4)->tail.metaData.AN_A, std::get<0>(tuple4)->tail.metaData.AN_Ainv)); // OID lineorder | OID supplier
        CLEAR_HASHJOIN_AN(tuple5);
        delete std::get<0>(tuple4);
        // join with LO_PARTKEY to already reduce the join partners
        auto bat6 = std::get<0>(tuple5)->mirror_head(); // OID lineorder | OID Lineorder
        delete std::get<0>(tuple5);
        MEASURE_OP_TUPLE(sw2, x, tuple7, v2::bat::ops::matchjoinAN(bat6, batLPenc)); // OID lineorder | lo_partkey (where s_region = 'AMERICA')
        CLEAR_HASHJOIN_AN(tuple7);
        delete bat6;

        // p_category = 'MFGR#12'
        MEASURE_OP_PAIR(sw2, x, pair8, v2::bat::ops::selectAN<std::equal_to>(batPC, const_cast<str_t>("MFGR#12"))); // OID part | p_category
        CLEAR_SELECT_AN(pair8);
        // p_brand = 'MFGR#121'
        // MEASURE_OP(sw2, x, bat8, v2::bat::ops::select<equal_to>(batPB, "MFGR#121")); // OID part | p_brand
        auto bat9 = std::get<0>(pair8)->mirror_head(); // OID part | OID part
        delete std::get<0>(pair8);
        auto batA = batPPenc->reverse(); // p_partkey | OID part
        MEASURE_OP_TUPLE(sw2, x, tupleB, v2::bat::ops::matchjoinAN(batA, bat9)); // p_partkey | OID Part where p_category = 'MFGR#12'
        CLEAR_HASHJOIN_AN(tupleB);
        delete batA;
        delete bat9;
        MEASURE_OP_TUPLE(sw2, x, tupleC, v2::bat::ops::hashjoinAN(std::get<0>(tuple7), std::get<0>(tupleB))); // OID lineorder | OID part (where s_region = 'AMERICA' and p_category = 'MFGR#12')
        CLEAR_HASHJOIN_AN(tupleC);
        delete std::get<0>(tuple7);
        delete std::get<0>(tupleB);

        // join with date now!
        auto batD = std::get<0>(tupleC)->mirror_head(); // OID lineorder | OID lineorder  (where ...)
        delete std::get<0>(tupleC);
        MEASURE_OP_TUPLE(sw2, x, tupleE, v2::bat::ops::matchjoinAN(batD, batLOenc)); // OID lineorder | lo_orderdate (where ...)
        CLEAR_HASHJOIN_AN(tupleE);
        delete batD;
        auto batF = batDDenc->reverse(); // d_datekey | OID date
        MEASURE_OP_TUPLE(sw2, x, tupleG, v2::bat::ops::hashjoinAN(std::get<0>(tupleE), batF, std::get<0>(tupleE)->head.metaData.AN_A, std::get<0>(tupleE)->head.metaData.AN_Ainv, std::get < v2_resoid_t::As->size() - 1 > (*v2_resoid_t::As), std::get < v2_resoid_t::Ainvs->size() - 1 > (*v2_resoid_t::Ainvs))); // OID lineorder | OID date (where ..., joined with date)
        CLEAR_HASHJOIN_AN(tupleG);
        delete std::get<0>(tupleE);
        delete batF;

        // now prepare grouped sum and check inputs
        auto batW = std::get<0>(tupleG)->mirror_head(); // OID lineorder | OID lineorder
        MEASURE_OP_TUPLE(sw2, x, tupleX, v2::bat::ops::matchjoinAN(batW, batLPenc)); // OID lineorder | lo_partkey
        CLEAR_HASHJOIN_AN(tupleX);
        auto batY = batPPenc->reverse(); // p_partkey | OID part
        MEASURE_OP_TUPLE(sw2, x, tupleZ, v2::bat::ops::hashjoinAN(std::get<0>(tupleX), batY, std::get<0>(tupleX)->head.metaData.AN_A, std::get<0>(tupleX)->head.metaData.AN_Ainv, std::get < ANParametersSelector<v2_resoid_t>::As->size() - 1 > (*ANParametersSelector<v2_resoid_t>::As), std::get < ANParametersSelector<v2_resoid_t>::Ainvs->size() - 1 > (*ANParametersSelector<v2_resoid_t>::Ainvs))); // OID lineorder | OID part
        CLEAR_HASHJOIN_AN(tupleZ);
        delete std::get<0>(tupleX);
        delete batY;
        MEASURE_OP_TUPLE(sw2, x, tupleA1, v2::bat::ops::hashjoinAN(std::get<0>(tupleZ), batPB)); // OID lineorder | p_brand
        CLEAR_HASHJOIN_AN(tupleA1);
        delete std::get<0>(tupleZ);

        MEASURE_OP_TUPLE(sw2, x, tupleA2, v2::bat::ops::hashjoinAN(std::get<0>(tupleG), batDYenc)); // OID lineorder | d_year
        CLEAR_HASHJOIN_AN(tupleA2);
        delete std::get<0>(tupleG);

        MEASURE_OP_TUPLE(sw2, x, tupleA3, v2::bat::ops::matchjoinAN(batW, batLRenc)); // OID lineorder | lo_revenue (where ...)
        CLEAR_HASHJOIN_AN(tupleA3);
        delete batW;

        MEASURE_OP_TUPLE(sw2, x, tupleK, v2::bat::ops::groupedSumAN<v2_resbigint_t>(std::get<0>(tupleA3), std::get<0>(tupleA2), std::get<0>(tupleA1)));
        CLEAR_GROUPEDSUM_AN(tupleK);
        delete std::get<0>(tupleA1);
        delete std::get<0>(tupleA2);
        delete std::get<0>(tupleA3);

        totalTimes[i] = sw1.stop();

        std::cout << "(" << std::setw(2) << i << ")\n\tresult-size: " << std::get<0>(tupleK)->size() << "\n\t  time: " << sw1 << " ns.\n";

        if (CONFIG.PRINT_RESULT && i == 0) {
            size_t sum = 0;
            auto iter0 = std::get<0>(tupleK)->begin();
            auto iter1 = std::get<1>(tupleK)->begin();
            auto iter2 = std::get<2>(tupleK)->begin();
            auto iter3 = std::get<3>(tupleK)->begin();
            auto iter4 = std::get<4>(tupleK)->begin();
            // we need the following typedefs to cast the inverses to the correct length
            typedef std::remove_pointer < std::remove_reference < decltype(std::get<0>(tupleK))>::type>::type tupleK_0_t;
            typedef typename TypeMap<tupleK_0_t::v2_tail_t>::v2_encoded_t::type_t K0_tail_enc_t;
            typedef typename TypeMap<tupleK_0_t::v2_tail_t>::v2_base_t::type_t K0_tail_unenc_t;
            typedef std::remove_pointer < std::remove_reference < decltype(std::get<1>(tupleK))>::type>::type tupleK_1_t;
            typedef typename TypeMap<tupleK_1_t::v2_tail_t>::v2_encoded_t::type_t K1_tail_enc_t;
            typedef typename TypeMap<tupleK_1_t::v2_tail_t>::v2_base_t::type_t K1_tail_unenc_t;
            typedef std::remove_pointer < std::remove_reference < decltype(std::get<2>(tupleK))>::type>::type tupleK_2_t;
            typedef typename TypeMap<tupleK_2_t::v2_tail_t>::v2_encoded_t::type_t K2_tail_enc_t;
            typedef typename TypeMap<tupleK_2_t::v2_tail_t>::v2_base_t::type_t K2_tail_unenc_t;
            typedef std::remove_pointer < std::remove_reference < decltype(std::get<3>(tupleK))>::type>::type tupleK_3_t;
            typedef typename TypeMap<tupleK_3_t::v2_tail_t>::v2_encoded_t::type_t K3_tail_enc_t;
            typedef typename TypeMap<tupleK_3_t::v2_tail_t>::v2_base_t::type_t K3_tail_unenc_t;
            // OK now get the correct inverses
            auto Ainv0 = static_cast<K0_tail_enc_t>(std::get<0>(tupleK)->tail.metaData.AN_Ainv);
            auto Ainv1 = static_cast<K1_tail_enc_t>(std::get<1>(tupleK)->tail.metaData.AN_Ainv);
            auto Ainv2 = static_cast<K2_tail_enc_t>(std::get<2>(tupleK)->tail.metaData.AN_Ainv);
            auto Ainv3 = static_cast<K3_tail_enc_t>(std::get<3>(tupleK)->tail.metaData.AN_Ainv);
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
        delete std::get<0>(tupleK);
        delete std::get<1>(tupleK);
        delete std::get<2>(tupleK);
        delete std::get<3>(tupleK);
        delete std::get<4>(tupleK);

        COUT_HEADLINE;
        COUT_RESULT(0, x, OP_NAMES);
    }

    if (CONFIG.VERBOSE) {
        std::cout << "peak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.\n";
    }

    std::cout << "TotalTimes:";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << '\n' << std::setw(2) << i << '\t' << totalTimes[i];
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

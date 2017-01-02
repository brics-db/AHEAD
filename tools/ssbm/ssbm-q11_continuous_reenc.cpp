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
 * File:   ssbm-q11_continuous_reenc.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 8. December 2016, 01:32
 */

#include "ssbm.hpp"
#include <column_operators/OperatorsAN.hpp>

int
main (int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    std::vector<StopWatch::rep> totalTimes(CONFIG.NUM_RUNS);
    const size_t NUM_OPS = 24;
    cstr_t OP_NAMES[NUM_OPS] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P"};
    StopWatch::rep opTimes[NUM_OPS] = {0};
    size_t batSizes[NUM_OPS] = {0};
    size_t batConsumptions[NUM_OPS] = {0};
    bool hasTwoTypes[NUM_OPS] = {false};
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];
    string emptyString;
    size_t x = 0;
    StopWatch sw1, sw2;

    std::cout << "SSBM Query 1.1 Continuous Detection With Reencoding\n===================================================" << std::endl;

    MetaRepositoryManager::init(CONFIG.DB_PATH);

    sw1.start();
    // loadTable(CONFIG.DB_PATH, "customerAN", CONFIG);
    loadTable(CONFIG.DB_PATH, "dateAN", CONFIG);
    loadTable(CONFIG.DB_PATH, "lineorderAN", CONFIG);
    // loadTable(CONFIG.DB_PATH, "partAN", CONFIG);
    // loadTable(CONFIG.DB_PATH, "supplierAN", CONFIG);
    sw1.stop();
    std::cout << "Total loading time: " << sw1 << " ns." << std::endl;

    if (CONFIG.VERBOSE) {
        std::cout << "SSBM Q1.1:\n";
        std::cout << "select sum(lo_revenue), d_year, p_brand\n";
        std::cout << "  from lineorder, part, supplier, date\n";
        std::cout << "  where lo_orderdate = d_datekey\n";
        std::cout << "    and d_year = 1993\n";
        std::cout << "    and lo_discount between 1 and 3\n";
        std::cout << "    and lo_quantity < 25;" << std::endl;
    }

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(sw1, x, batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new restiny_colbat_t("lineorderAN", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new restiny_colbat_t("lineorderAN", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new resint_colbat_t("lineorderAN", "extendedprice"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDYenc, v2::bat::ops::copy(batDYcb));
    MEASURE_OP(sw1, x, batDDenc, v2::bat::ops::copy(batDDcb));
    MEASURE_OP(sw1, x, batLQenc, v2::bat::ops::copy(batLQcb));
    MEASURE_OP(sw1, x, batLDenc, v2::bat::ops::copy(batLDcb));
    MEASURE_OP(sw1, x, batLOenc, v2::bat::ops::copy(batLOcb));
    MEASURE_OP(sw1, x, batLEenc, v2::bat::ops::copy(batLEcb));
    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;

    if (CONFIG.VERBOSE) {
        COUT_HEADLINE;
        COUT_RESULT(0, x);
        std::cout << std::endl;
    }

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 1) select from lineorder
        MEASURE_OP_PAIR(sw2, x, pair1, v2::bat::ops::selectAN<less>(batLQenc, 25 * batLQenc->tail.metaData.AN_A)); // lo_quantity < 25
        delete pair1.second;
        MEASURE_OP_PAIR(sw2, x, pair2, v2::bat::ops::selectAN(batLDenc, 1 * batLDenc->tail.metaData.AN_A, 3 * batLDenc->tail.metaData.AN_A)); // lo_discount between 1 and 3
        delete pair2.second;
        MEASURE_OP(sw2, x, bat3, pair1.first->mirror_head()); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete pair1.first;
        MEASURE_OP_TUPLE(sw2, x, tuple4, v2::bat::ops::hashjoinAN(bat3, pair2.first, std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs), std::get<6>(*v2_restiny_t::As), std::get<6>(*v2_restiny_t::Ainvs))); // join selection
        delete bat3;
        delete pair2.first;
        CLEAR_HASHJOIN_AN(tuple4);
        MEASURE_OP(sw2, x, bat5, (get<0>(tuple4)->mirror_head())); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP_TUPLE(sw2, x, tuple6, v2::bat::ops::hashjoinAN(bat5, batLOenc, std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs), std::get<14>(*v2_resint_t::As), std::get<14>(*v2_resint_t::Ainvs))); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        CLEAR_HASHJOIN_AN(tuple6);

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP_PAIR(sw2, x, pair7, (v2::bat::ops::selectAN<equal_to>(batDYenc, 1993 * batDYenc->tail.metaData.AN_A))); // d_year = 1993
        if (pair7.second) delete pair7.second;
        MEASURE_OP(sw2, x, bat8, (pair7.first->mirror_head())); // prepare joined selection over d_year and d_datekey
        delete pair7.first;
        MEASURE_OP_TUPLE(sw2, x, tuple9, (v2::bat::ops::hashjoinAN(bat8, batDDenc, std::get<14>(*v2_resoid_t::As), std::get<14>(*v2_resoid_t::Ainvs), std::get<14>(*v2_resint_t::As), std::get<14>(*v2_resint_t::Ainvs)))); // only those d_datekey where d_year...
        delete bat8;
        CLEAR_HASHJOIN_AN(tuple9);

        // 3) join lineorder and date
        MEASURE_OP(sw2, x, batA, (get<0>(tuple9)->reverse()));
        delete get<0>(tuple9);
        MEASURE_OP_TUPLE(sw2, x, tupleB, (v2::bat::ops::hashjoinAN(get<0>(tuple6), batA, std::get<12>(*v2_resoid_t::As), std::get<12>(*v2_resoid_t::Ainvs), std::get<13>(*v2_resoid_t::As), std::get<13>(*v2_resoid_t::Ainvs)))); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete get<0>(tuple6);
        delete batA;
        CLEAR_HASHJOIN_AN(tupleB);
        // batB has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP(sw2, x, batC, (get<0>(tupleB)->mirror_head())); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete get<0>(tupleB);
        MEASURE_OP_TUPLE(sw2, x, tupleD, (v2::bat::ops::hashjoinAN(batC, batLEenc, std::get<11>(*v2_resoid_t::As), std::get<11>(*v2_resoid_t::Ainvs), std::get<14>(*v2_resint_t::As), std::get<14>(*v2_resint_t::Ainvs))));
        CLEAR_HASHJOIN_AN(tupleD);
        MEASURE_OP_TUPLE(sw2, x, tupleE, (v2::bat::ops::hashjoinAN(batC, get<0>(tuple4), std::get<11>(*v2_resoid_t::As), std::get<11>(*v2_resoid_t::Ainvs), std::get<5>(*v2_restiny_t::As), std::get<5>(*v2_restiny_t::Ainvs))));
        delete batC;
        delete get<0>(tuple4);
        CLEAR_HASHJOIN_AN(tupleE);

        // 4) result
        MEASURE_OP_TUPLE(sw2, x, tupleF, (v2::bat::ops::aggregate_mul_sumAN<v2_resbigint_t>(get<0>(tupleD), get<0>(tupleE))));
        delete get<0>(tupleD);
        delete get<0>(tupleE);
        delete get<1>(tupleF);
        delete get<2>(tupleF);
        auto iter = get<0>(tupleF)->begin();
        auto result = iter->tail();
        delete iter;
        auto Ainv = get<0>(tupleF)->tail.metaData.AN_Ainv;
        delete get<0>(tupleF);

        totalTimes[i] = sw1.stop();

        std::cout << "(" << setw(2) << i << ")\n\tresult: " << (result * Ainv) << " (encoded: " << result << ")\n\t  time: " << sw1 << " ns.\n";
        COUT_HEADLINE;
        COUT_RESULT(0, x, OP_NAMES);
    }

    if (CONFIG.VERBOSE) {
        std::cout << "peak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.\n";
    }

    std::cout << "TotalTimes:";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << '\n' << setw(2) << i << '\t' << totalTimes[i];
    }
    std::cout << std::endl;

    delete batDYenc;
    delete batDDenc;
    delete batLQenc;
    delete batLDenc;
    delete batLOenc;
    delete batLEenc;

    TransactionManager::destroyInstance();

    return 0;
}
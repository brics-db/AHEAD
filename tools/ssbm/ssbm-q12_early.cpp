/* 
 * File:   ssbm-q12_eager.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 30. October 2016, 00:01
 */

#include "ssbm.hpp"

int main(int argc, char** argv) {
    ssbmconf_t CONFIG = initSSBM(argc, argv);
    StopWatch::rep totalTimes[CONFIG.NUM_RUNS] = {0};
    const size_t NUM_OPS = 30;
    cstr_t OP_NAMES[NUM_OPS] = {"-6", "-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P"};
    StopWatch::rep opTimes[NUM_OPS] = {0};
    size_t batSizes[NUM_OPS] = {0};
    size_t batConsumptions[NUM_OPS] = {0};
    bool hasTwoTypes[NUM_OPS] = {false};
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];
    string emptyString;
    size_t x = 0;

    cout << "ssbm-q12_eager\n==============" << endl;

    boost::filesystem::path p(CONFIG.DB_PATH);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    StopWatch sw1, sw2;

    sw1.start();
    // loadTable(baseDir, "customerAN");
    loadTable(baseDir, "dateAN");
    loadTable(baseDir, "lineorderAN");
    // loadTable(baseDir, "partAN");
    // loadTable(baseDir, "supplierAN");
    sw1.stop();
    cout << "Total loading time: " << sw1 << " ns." << endl;

    cout << "\nSSBM Q1.2:\nselect sum(lo_extendedprice * lo_discount) as revenue\n  from lineorder, date\n  where lo_orderdate = d_datekey\n    and d_yearmonthnum = 199401\n    and lo_discount between 4 and 6\n    and lo_quantity  between 26 and 35;" << endl;

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new resint_colbat_t("dateAN", "yearmonthnum"));
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

    COUT_HEADLINE;
    COUT_RESULT(0, x);
    cout << endl;

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 0) Eager Check
        MEASURE_OP_TUPLE(sw2, x, tupleDY, v2::bat::ops::checkAndDecodeAN(batDYenc));
        CLEAR_CHECKANDDECODE_AN(tupleDY);
        MEASURE_OP_TUPLE(sw2, x, tupleDD, v2::bat::ops::checkAndDecodeAN(batDDenc));
        CLEAR_CHECKANDDECODE_AN(tupleDD);
        MEASURE_OP_TUPLE(sw2, x, tupleLQ, v2::bat::ops::checkAndDecodeAN(batLQenc));
        CLEAR_CHECKANDDECODE_AN(tupleLQ);
        MEASURE_OP_TUPLE(sw2, x, tupleLD, v2::bat::ops::checkAndDecodeAN(batLDenc));
        CLEAR_CHECKANDDECODE_AN(tupleLD);
        MEASURE_OP_TUPLE(sw2, x, tupleLO, v2::bat::ops::checkAndDecodeAN(batLOenc));
        CLEAR_CHECKANDDECODE_AN(tupleLO);
        MEASURE_OP_TUPLE(sw2, x, tupleLE, v2::bat::ops::checkAndDecodeAN(batLEenc));
        CLEAR_CHECKANDDECODE_AN(tupleLE);

        // 1) select from lineorder
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::select(get<0>(tupleLQ), 26, 35)); // lo_quantity between 26 and 35
        delete get<0>(tupleLQ);
        MEASURE_OP(sw2, x, bat2, v2::bat::ops::select(get<0>(tupleLD), 4, 6)); // lo_discount between 4 and 6
        delete get<0>(tupleLD);
        MEASURE_OP(sw2, x, bat3, bat1->mirror_head()); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::hashjoin(bat3, bat2)); // join selection
        delete bat3;
        delete bat2;
        MEASURE_OP(sw2, x, bat5, bat4->mirror_head()); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP(sw2, x, bat6, v2::bat::ops::hashjoin(bat5, get<0>(tupleLO))); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        delete get<0>(tupleLO);

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::select<equal_to>(get<0>(tupleDY), 199401)); // d_yearmonthnum = 199401
        delete get<0>(tupleDY);
        MEASURE_OP(sw2, x, bat8, bat7->mirror_head()); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(sw2, x, bat9, v2::bat::ops::hashjoin(bat8, get<0>(tupleDD))); // only those d_datekey where d_year...
        delete bat8;
        delete get<0>(tupleDD);

        // 3) join lineorder and date
        MEASURE_OP(sw2, x, batA, bat9->reverse());
        delete bat9;
        MEASURE_OP(sw2, x, batB, v2::bat::ops::hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete batA;
        delete bat6;
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP(sw2, x, batC, batB->mirror_head()); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        MEASURE_OP(sw2, x, batD, v2::bat::ops::hashjoin(batC, get<0>(tupleLE)));
        delete get<0>(tupleLE);
        PRINT_BAT(sw1, printBat(batD->begin(), "lo_extprice where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25"));
        MEASURE_OP(sw2, x, batE, v2::bat::ops::hashjoin(batC, bat4));
        delete batC;
        delete bat4;

        // 4) result
        MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(batD, batE, 0));
        delete batD;
        delete batE;

        totalTimes[i] = sw1.stop();

        cout << "\n(" << setw(2) << i << ")\n\tresult: " << result << "\n\t  time: " << sw1 << " ns.";
        COUT_HEADLINE;
        COUT_RESULT(0, x, OP_NAMES);
    }

    cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.\n";
    cout << "TotalTimes:";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        cout << '\n' << setw(2) << i << '\t' << totalTimes[i];
    }
    cout << endl;

    delete batDYenc;
    delete batDDenc;
    delete batLQenc;
    delete batLDenc;
    delete batLOenc;
    delete batLEenc;

    TransactionManager::destroyInstance();

    return 0;
}
/* 
 * File:   ssbm-q12_lazy.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 30. October 2016, 00:03
 */

#include "ssbm.hpp"

int main(int argc, char** argv) {
    ssbmconf_t CONFIG = initSSBM(argc, argv);
    StopWatch::rep totalTimes[CONFIG.NUM_RUNS] = {0};
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

    cout << "ssbm-q12_lazy\n=============" << endl;

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

        // LAZY MODE !!! NO AN-OPERATORS UNTIL DECODING !!!

        // 1) select from lineorder
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::select(batLQenc, static_cast<restiny_t> (26 * v2_restiny_t::A), static_cast<restiny_t> (35 * v2_restiny_t::A))); // lo_quantity between 26 and 35
        PRINT_BAT(sw1, printBat(bat1->begin(), "lo_quantity between 26 and 35"));
        MEASURE_OP(sw2, x, bat2, v2::bat::ops::select(batLDenc, static_cast<restiny_t> (4 * v2_restiny_t::A), static_cast<restiny_t> (6 * v2_restiny_t::A))); // lo_discount between 4 and 6
        PRINT_BAT(sw1, printBat(bat2->begin(), "lo_discount between 4 and 6"));
        MEASURE_OP(sw2, x, bat3, bat1->mirror_head()); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::hashjoin(bat3, bat2)); // join selection
        delete bat3;
        delete bat2;
        MEASURE_OP(sw2, x, bat5, bat4->mirror_head()); // prepare joined selection with lo_orderdate (contains positions in tail)
        PRINT_BAT(sw1, printBat(bat5->begin(), "lo_discount where lo_quantity between 26 and 35 and lo_discount between 4 and 6"));
        MEASURE_OP(sw2, x, bat6, v2::bat::ops::hashjoin(bat5, batLOenc)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        PRINT_BAT(sw1, printBat(bat6->begin(), "lo_orderdates where lo_quantity between 26 and 35 and lo_discount between 4 and 6"));

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::select<equal_to>(batDYenc, static_cast<resint_t> (199401) * v2_resint_t::A)); // d_yearmonthnum = 199401
        PRINT_BAT(sw1, printBat(bat7->begin(), "d_yearmonthnum = 199401"));
        MEASURE_OP(sw2, x, bat8, bat7->mirror_head()); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(sw2, x, bat9, v2::bat::ops::hashjoin(bat8, batDDenc)); // only those d_datekey where d_year...
        delete bat8;
        PRINT_BAT(sw1, printBat(bat9->begin(), "d_datekey where d_yearmonthnum = 199401"));

        // 3) join lineorder and date
        MEASURE_OP(sw2, x, batA, bat9->reverse());
        delete bat9;
        MEASURE_OP(sw2, x, batB, v2::bat::ops::hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete bat6;
        delete batA;
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP(sw2, x, batC, batB->mirror_head()); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        // BatF only contains the 
        MEASURE_OP(sw2, x, batD, v2::bat::ops::hashjoin(batC, batLEenc));
        PRINT_BAT(sw1, printBat(batD->begin(), "lo_extprice where d_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35"));
        MEASURE_OP(sw2, x, batE, v2::bat::ops::hashjoin(batC, bat4));
        delete batC;
        delete bat4;
        PRINT_BAT(sw1, printBat(batE->begin(), "lo_discount where d_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35"));

        // 4) lazy decode and result
        MEASURE_OP_TUPLE(sw2, x, tupleF, v2::bat::ops::checkAndDecodeAN(batD));
        CLEAR_CHECKANDDECODE_AN(tupleF);
        delete batD;
        MEASURE_OP_TUPLE(sw2, x, tupleG, v2::bat::ops::checkAndDecodeAN(batE));
        CLEAR_CHECKANDDECODE_AN(tupleG);
        delete batE;
        MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(get<0>(tupleF), get<0>(tupleG)));
        delete get<0>(tupleF);
        delete get<0>(tupleG);

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
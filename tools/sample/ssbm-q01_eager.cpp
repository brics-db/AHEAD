/* 
 * File:   ssbm-q01.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include "ssbm.hpp"

int main(int argc, char** argv) {
    cout << "ssbm-q01_eager\n==============" << endl;

    boost::filesystem::path p(argc == 1 ? argv[0] : argv[1]);
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

    cout << "\nSSBM Q1.1:\nselect lo_extendedprice\n  from lineorder, date\n  where lo_orderdate = d_datekey\n    and d_year = 1993\n    and lo_discount between 1 and 3\n    and lo_quantity  < 25;" << endl;

    const size_t NUM_RUNS = 10;
    StopWatch::rep totalTimes[NUM_RUNS] = {0};
    const size_t NUM_OPS = 32;
    StopWatch::rep opTimes[NUM_OPS] = {0};
    size_t batSizes[NUM_OPS] = {0};
    size_t batConsumptions[NUM_OPS] = {0};
    bool hasTwoTypes[NUM_OPS] = {false};
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];

    const size_t LEN_TYPES = 16;
    string emptyString;
    size_t x = 0;

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new resshort_col_t("dateAN", "year"));
    MEASURE_OP(sw1, x, batDDcb, new resint_col_t("dateAN", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new restiny_col_t("lineorderAN", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new restiny_col_t("lineorderAN", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new resint_col_t("lineorderAN", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new resint_col_t("lineorderAN", "extendedprice"));

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

    cout << "\n\t  name\t" << setw(13) << "time [ns]\t" << setw(10) << "size [#]\t" << setw(10) << "consum [B]\t" << setw(LEN_TYPES) << "type head\t" << setw(LEN_TYPES) << "type tail";
    for (size_t i = 0; i < x; ++i) {
        cout << "\n\t  op" << setw(2) << i << "\t" << setw(13) << hrc_duration(opTimes[i]) << "\t" << setw(10) << batSizes[i] << "\t" << setw(10) << batConsumptions[i] << "\t" << setw(LEN_TYPES) << headTypes[i].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[i] ? tailTypes[i].pretty_name() : emptyString);
    }
    cout << endl;

    for (size_t i = 0; i < NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 0) Eager Check
        MEASURE_OP(sw2, x, auto, batDYpair, (v2::bat::ops::checkAndDecodeA<shortint_t>(batDYenc, TypeSelector<shortint_t>::A_INV, TypeSelector<shortint_t>::A_UNENC_MAX_U)), batDYpair.first->size(), batDYpair.first->consumption());
        auto batDY = batDYpair.first;
        SAVE_TYPE(x - 1, batDY);
        delete batDYpair.second;
        MEASURE_OP(sw2, x, auto, batDDpair, (v2::bat::ops::checkAndDecodeA<int_t>(batDDenc, TypeSelector<int_t>::A_INV, TypeSelector<int_t>::A_UNENC_MAX_U)), batDDpair.first->size(), batDDpair.first->consumption());
        auto batDD = batDDpair.first;
        SAVE_TYPE(x - 1, batDD);
        delete batDDpair.second;
        MEASURE_OP(sw2, x, auto, batLQpair, (v2::bat::ops::checkAndDecodeA<tinyint_t>(batLQenc, TypeSelector<tinyint_t>::A_INV, TypeSelector<tinyint_t>::A_UNENC_MAX_U)), batLQpair.first->size(), batLQpair.first->consumption());
        auto batLQ = batLQpair.first;
        SAVE_TYPE(x - 1, batLQ);
        delete batLQpair.second;
        MEASURE_OP(sw2, x, auto, batLDpair, (v2::bat::ops::checkAndDecodeA<tinyint_t>(batLDenc, TypeSelector<tinyint_t>::A_INV, TypeSelector<tinyint_t>::A_UNENC_MAX_U)), batLDpair.first->size(), batLDpair.first->consumption());
        auto batLD = batLDpair.first;
        SAVE_TYPE(x - 1, batLD);
        delete batLDpair.second;
        MEASURE_OP(sw2, x, auto, batLOpair, (v2::bat::ops::checkAndDecodeA<int_t>(batLOenc, TypeSelector<int_t>::A_INV, TypeSelector<int_t>::A_UNENC_MAX_U)), batLOpair.first->size(), batLOpair.first->consumption());
        auto batLO = batLOpair.first;
        SAVE_TYPE(x - 1, batLO);
        delete batLOpair.second;
        MEASURE_OP(sw2, x, auto, batLEpair, (v2::bat::ops::checkAndDecodeA<int_t>(batLEenc, TypeSelector<int_t>::A_INV, TypeSelector<int_t>::A_UNENC_MAX_U)), batLEpair.first->size(), batLEpair.first->consumption());
        auto batLE = batLEpair.first;
        SAVE_TYPE(x - 1, batLE);
        delete batLEpair.second;

        // 1) select from lineorder
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::selection_lt(batLQ, static_cast<tinyint_t> (25))); // lo_quantity < 25
        PRINT_BAT(sw1, printBat(bat1->begin(), "lo_quantity < 25"));
        delete batLQ;
        MEASURE_OP(sw2, x, bat2, v2::bat::ops::selection_bt(batLD, static_cast<tinyint_t> (1), static_cast<tinyint_t> (3))); // lo_discount between 1 and 3
        PRINT_BAT(sw1, printBat(bat2->begin(), "lo_discount between 1 and 3"));
        delete batLD;
        MEASURE_OP(sw2, x, bat3, v2::bat::ops::mirror(bat1)); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::col_hashjoin(bat3, bat2)); // join selection
        delete bat3;
        delete bat2;
        MEASURE_OP(sw2, x, bat5, v2::bat::ops::mirror(bat4)); // prepare joined selection with lo_orderdate (contains positions in tail)
        PRINT_BAT(sw1, printBat(bat5->begin(), "lo_discount where lo_quantity < 25 and lo_discount between 1 and 3"));
        MEASURE_OP(sw2, x, bat6, v2::bat::ops::col_hashjoin(bat5, batLO)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        PRINT_BAT(sw1, printBat(bat6->begin(), "lo_orderdates where lo_quantity < 25 and lo_discount between 1 and 3"));
        delete batLO;

        // 1) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::selection_eq(batDY, static_cast<shortint_t> (1993))); // d_year = 1993
        delete batDY;
        PRINT_BAT(sw1, printBat(bat7->begin(), "d_year = 1993"));
        MEASURE_OP(sw2, x, bat8, v2::bat::ops::mirror(bat7)); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(sw2, x, bat9, v2::bat::ops::col_hashjoin(bat8, batDD)); // only those d_datekey where d_year...
        delete bat8;
        delete batDD;
        PRINT_BAT(sw1, printBat(bat9->begin(), "d_datekey where d_year = 1993"));

        // 3) join lineorder and date
        MEASURE_OP(sw2, x, batA, v2::bat::ops::reverse(bat9));
        delete bat9;
        MEASURE_OP(sw2, x, batB, v2::bat::ops::col_hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete batA;
        delete bat6;
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP(sw2, x, batC, v2::bat::ops::mirror(batB)); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        MEASURE_OP(sw2, x, batD, v2::bat::ops::col_hashjoin(batC, batLE));
        delete batLE;
        PRINT_BAT(sw1, printBat(batD->begin(), "lo_extprice where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25"));
        MEASURE_OP(sw2, x, batE, v2::bat::ops::col_hashjoin(batC, bat4));
        delete batC;
        delete bat4;
        MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(batD, batE, 0));
        delete batD;
        delete batE;

        totalTimes[i] = sw1.stop();

        cout << "\n(" << setw(2) << i << ")\n\tresult: " << result << "\n\t  time: " << setw(13) << sw1 << " ns.";
        cout << "\n\t  name\t" << setw(13) << "time [ns]\t" << setw(10) << "size [#]\t" << setw(10) << "consum [B]\t" << setw(LEN_TYPES) << "type head\t" << setw(LEN_TYPES) << "type tail";
        for (size_t j = 0; j < x; ++j) {
            cout << "\n\t  op" << setw(2) << j << "\t" << setw(13) << hrc_duration(opTimes[j]) << "\t" << setw(10) << batSizes[j] << "\t" << setw(10) << batConsumptions[j] << "\t" << setw(LEN_TYPES) << headTypes[j].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[j] ? tailTypes[j].pretty_name() : emptyString);
        }
    }

    cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.\n";
    cout << "TotalTimes:";
    for (size_t i = 0; i < NUM_RUNS; ++i) {
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

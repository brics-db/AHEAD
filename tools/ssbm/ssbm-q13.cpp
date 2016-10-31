/* 
 * File:   ssbm-q13.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 30. October 2016, 01:07
 */

#include "ssbm.hpp"

int main(int argc, char** argv) {
    cout << "ssbm-q13\n========" << endl;

    boost::filesystem::path p(argc == 1 ? argv[0] : argv[1]);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    StopWatch sw1, sw2;

    sw1.start();
    // loadTable(baseDir, "customer");
    loadTable(baseDir, "date");
    loadTable(baseDir, "lineorder");
    // loadTable(baseDir, "part");
    // loadTable(baseDir, "supplier");
    sw1.stop();
    cout << "Total loading time: " << sw1 << " ns." << endl;

    cout << "\nSSBM Q1.3:\nselect sum(lo_extendedprice * lo_discount) as revenue\n  from lineorder, date\n  where lo_orderdate = d_datekey\n    and d_weeknuminyear = 6\n    and d_year = 1994\n    and lo_discount between 5 and 7\n    and lo_quantity  between 26 and 35;" << endl;

    const size_t NUM_RUNS = 10;
    StopWatch::rep totalTimes[NUM_RUNS] = {0};
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

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new shortint_colbat_t("date", "year"));
    MEASURE_OP(sw1, x, batDDcb, new int_colbat_t("date", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new tinyint_colbat_t("lineorder", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new tinyint_colbat_t("lineorder", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new int_colbat_t("lineorder", "extendedprice"));
    MEASURE_OP(sw1, x, batDWcb, new tinyint_colbat_t("date", "weeknuminyear"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDY, v2::bat::ops::copy(batDYcb));
    MEASURE_OP(sw1, x, batDD, v2::bat::ops::copy(batDDcb));
    MEASURE_OP(sw1, x, batLQ, v2::bat::ops::copy(batLQcb));
    MEASURE_OP(sw1, x, batLD, v2::bat::ops::copy(batLDcb));
    MEASURE_OP(sw1, x, batLO, v2::bat::ops::copy(batLOcb));
    MEASURE_OP(sw1, x, batLE, v2::bat::ops::copy(batLEcb));
    MEASURE_OP(sw1, x, batDW, v2::bat::ops::copy(batDWcb));
    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;
    delete batDWcb;

    COUT_HEADLINE;
    COUT_RESULT(0, x);
    cout << endl;

    for (size_t i = 0; i < NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 1) select from lineorder
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::select(batLQ, 26, 35)); // lo_quantity between 26 and 35
        MEASURE_OP(sw2, x, bat2, v2::bat::ops::select(batLD, 5, 7)); // lo_discount between 5 and 7
        MEASURE_OP(sw2, x, bat3, bat1->mirror_head()); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::hashjoin(bat3, bat2)); // join selection
        delete bat2;
        delete bat3;
        MEASURE_OP(sw2, x, bat5, bat4->mirror_head()); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP(sw2, x, bat6, v2::bat::ops::hashjoin(bat5, batLO)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;

        // 1) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::select<equal_to>(batDY, 1994)); // d_year = 1994
        MEASURE_OP(sw2, x, bat8, bat7->mirror_head()); // prepare joined selection over d_year and d_weeknuminyear
        delete bat7;
        MEASURE_OP(sw2, x, bat9, v2::bat::ops::select<equal_to>(batDW, 6)); // d_weeknuminyear = 6
        MEASURE_OP(sw2, x, batA, v2::bat::ops::hashjoin(bat8, bat9));
        delete bat8;
        delete bat9;
        MEASURE_OP(sw2, x, batB, batA->mirror_head());
        delete batA;
        MEASURE_OP(sw2, x, batC, v2::bat::ops::hashjoin(batB, batDD)); // only those d_datekey where d_year and d_weeknuminyear...
        delete batB;
        MEASURE_OP(sw2, x, batD, batC->reverse());
        delete batC;

        // 3) join lineorder and date
        MEASURE_OP(sw2, x, batE, v2::bat::ops::hashjoin(bat6, batD)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete bat6;
        delete batD;
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP(sw2, x, batF, batE->mirror_head()); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batE;
        // BatF only contains the 
        MEASURE_OP(sw2, x, batG, v2::bat::ops::hashjoin(batF, batLE));
        MEASURE_OP(sw2, x, batH, v2::bat::ops::hashjoin(batF, bat4));
        delete batF;
        delete bat4;
        MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(batG, batH, 0));
        delete batG;
        delete batH;

        totalTimes[i] = sw1.stop();

        cout << "\n(" << setw(2) << i << ")\n\tresult: " << result << "\n\t  time: " << sw1 << " ns.";
        COUT_HEADLINE;
        COUT_RESULT(0, x, OP_NAMES);
    }

    cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.\n";
    cout << "TotalTimes:";
    for (size_t i = 0; i < NUM_RUNS; ++i) {
        cout << '\n' << setw(2) << i << '\t' << totalTimes[i];
    }
    cout << endl;

    delete batDY;
    delete batDD;
    delete batLQ;
    delete batLD;
    delete batLO;
    delete batLE;

    TransactionManager::destroyInstance();

    return 0;
}

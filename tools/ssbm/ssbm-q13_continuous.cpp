/* 
 * File:   ssbm-q13_lazy.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 31. October 2016, 22:54
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

    cout << "ssbm-q13_lazy\n========" << endl;

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

    cout << "\nSSBM Q1.3:\nselect sum(lo_extendedprice * lo_discount) as revenue\n  from lineorder, date\n  where lo_orderdate = d_datekey\n    and d_weeknuminyear = 6\n    and d_year = 1994\n    and lo_discount between 5 and 7\n    and lo_quantity  between 26 and 35;" << endl;

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(sw1, x, batDDcb, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new restiny_colbat_t("lineorderAN", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new restiny_colbat_t("lineorderAN", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new resint_colbat_t("lineorderAN", "extendedprice"));
    MEASURE_OP(sw1, x, batDWcb, new restiny_colbat_t("dateAN", "weeknuminyear"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDYenc, v2::bat::ops::copy(batDYcb));
    MEASURE_OP(sw1, x, batDDenc, v2::bat::ops::copy(batDDcb));
    MEASURE_OP(sw1, x, batLQenc, v2::bat::ops::copy(batLQcb));
    MEASURE_OP(sw1, x, batLDenc, v2::bat::ops::copy(batLDcb));
    MEASURE_OP(sw1, x, batLOenc, v2::bat::ops::copy(batLOcb));
    MEASURE_OP(sw1, x, batLEenc, v2::bat::ops::copy(batLEcb));
    MEASURE_OP(sw1, x, batDWenc, v2::bat::ops::copy(batDWcb));
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

    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 1) select from lineorder
        MEASURE_OP_PAIR(sw2, x, pair1, v2::bat::ops::selectAN(batLQenc, 26 * v2_restiny_t::A, 35 * v2_restiny_t::A)); // lo_quantity between 26 and 35
        delete pair1.second;
        MEASURE_OP_PAIR(sw2, x, pair2, v2::bat::ops::selectAN(batLDenc, 5 * v2_restiny_t::A, 7 * v2_restiny_t::A)); // lo_discount between 5 and 7
        delete pair2.second;
        MEASURE_OP(sw2, x, bat3, pair1.first->mirror_head()); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete pair1.first;
        MEASURE_OP_TUPLE(sw2, x, tuple4, v2::bat::ops::hashjoinAN(bat3, pair2.first)); // join selection
        delete pair2.first;
        delete bat3;
        CLEAR_HASHJOIN_AN(tuple4);
        MEASURE_OP(sw2, x, bat5, get<0>(tuple4)->mirror_head()); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP_TUPLE(sw2, x, tuple6, v2::bat::ops::hashjoinAN(bat5, batLOenc)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        CLEAR_HASHJOIN_AN(tuple6);

        // 2) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP_PAIR(sw2, x, pair7, v2::bat::ops::selectAN<equal_to>(batDYenc, 1994)); // d_year = 1994
        delete pair7.second;
        MEASURE_OP(sw2, x, bat8, pair7.first->mirror_head()); // prepare joined selection over d_year and d_weeknuminyear
        delete pair7.first;
        MEASURE_OP_PAIR(sw2, x, pair9, v2::bat::ops::selectAN<equal_to>(batDWenc, 6)); // d_weeknuminyear = 6
        delete pair9.second;
        MEASURE_OP_TUPLE(sw2, x, tupleA, v2::bat::ops::hashjoinAN(bat8, pair9.first));
        delete bat8;
        delete pair9.first;
        CLEAR_HASHJOIN_AN(tupleA);
        MEASURE_OP(sw2, x, batB, get<0>(tupleA)->mirror_head());
        delete get<0>(tupleA);
        MEASURE_OP_TUPLE(sw2, x, tupleC, v2::bat::ops::hashjoinAN(batB, batDDenc)); // only those d_datekey where d_year and d_weeknuminyear...
        delete batB;
        CLEAR_HASHJOIN_AN(tupleC);

        // 3) join lineorder and date
        MEASURE_OP(sw2, x, batD, get<0>(tupleC)->reverse());
        delete get<0>(tupleC);
        MEASURE_OP_TUPLE(sw2, x, tupleE, v2::bat::ops::hashjoinAN(get<0>(tuple6), batD)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        CLEAR_HASHJOIN_AN(tupleE);
        delete get<0>(tuple6);
        delete batD;
        // batE has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP(sw2, x, batF, get<0>(tupleE)->mirror_head()); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        MEASURE_OP_TUPLE(sw2, x, tupleG, v2::bat::ops::hashjoinAN(batF, batLEenc));
        CLEAR_HASHJOIN_AN(tupleG);
        MEASURE_OP_TUPLE(sw2, x, tupleH, v2::bat::ops::hashjoinAN(batF, get<0>(tuple4)));
        CLEAR_HASHJOIN_AN(tupleH);
        delete batF;
        delete get<0>(tuple4);

        // 4) result
        MEASURE_OP_TUPLE(sw2, x, tupleI, (v2::bat::ops::aggregate_mul_sumAN<v2_resbigint_t>(get<0>(tupleG), get<0>(tupleH))));
        delete get<0>(tupleG);
        delete get<0>(tupleH);
        delete get<1>(tupleI);
        delete get<2>(tupleI);
        auto iter = get<0>(tupleI)->begin();
        auto result = iter->tail();
        delete iter;
        delete get<0>(tupleI);

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
    delete batDWenc;

    TransactionManager::destroyInstance();

    return 0;
}

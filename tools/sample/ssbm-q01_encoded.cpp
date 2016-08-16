/* 
 * File:   ssbm-q01.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include "ssbm.hpp"

int main(int argc, char** argv) {
    cout << "ssbm-q01_lazy\n=============" << endl;

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

    COUT_HEADLINE;
    COUT_RESULT(0, x);
    cout << endl;

    for (size_t i = 0; i < NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 1) select from lineorder
        MEASURE_OP_PAIR(sw2, x, pair1, v2::bat::ops::selection_AN<v2_tinyint_t>(selection_type_t::LT, batLQenc, static_cast<restiny_t> (25) * ::A_TINY)); // lo_quantity < 25
        delete pair1.second;
        MEASURE_OP(sw2, x, bat3, v2::bat::ops::mirrorHead_AN<v2_tinyint_t>(pair1.first)); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete pair1.first;
        MEASURE_OP_PAIR(sw2, x, pair2, v2::bat::ops::selection_AN<v2_tinyint_t>(selection_type_t::BT, batLDenc, static_cast<restiny_t> (1) * ::A_TINY, static_cast<restiny_t> (3) * ::A_TINY)); // lo_discount between 1 and 3
        delete pair2.second;
        MEASURE_OP_TUPLE(sw2, x, tuple4, v2::bat::ops::col_hashjoin_AN<v2_tinyint_t>(bat3, pair2.first)); // join selection
        delete bat3;
        delete pair2.first;
        delete get<1>(tuple4);
        delete get<2>(tuple4);
        MEASURE_OP(sw2, x, bat5, v2::bat::ops::mirrorHead_AN<v2_tinyint_t>(get<0>(tuple4))); // prepare joined selection with lo_orderdate (contains positions in tail)
        MEASURE_OP_TUPLE(sw2, x, tuple6, v2::bat::ops::col_hashjoin_AN<v2_int_t>(bat5, batLOenc)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        delete get<1>(tuple6);
        delete get<2>(tuple6);

        // 1) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP_PAIR(sw2, x, pair7, v2::bat::ops::selection_AN<v2_shortint_t>(selection_type_t::EQ, batDYenc, static_cast<resshort_t> (1993) * ::A_SHORT)); // d_year = 1993
        delete pair7.second;
        MEASURE_OP(sw2, x, bat8, v2::bat::ops::mirrorHead_AN<v2_shortint_t>(pair7.first)); // prepare joined selection over d_year and d_datekey
        delete pair7.first;
        MEASURE_OP_TUPLE(sw2, x, tuple9, v2::bat::ops::col_hashjoin_AN<v2_int_t>(bat8, batDDenc)); // only those d_datekey where d_year...
        delete get<1>(tuple9);
        delete get<2>(tuple9);
        delete bat8;

        // 3) join lineorder and date
        MEASURE_OP_PAIR(sw2, x, pairA, v2::bat::ops::reverse_AN<v2_int_t>(get<0>(tuple9)));
        delete pairA.second;
        delete get<0>(tuple9);
        MEASURE_OP_TUPLE(sw2, x, tupleB, (v2::bat::ops::col_hashjoin_AN<v2_int_t, v2_int_t>(get<0>(tuple6), pairA.first))); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete get<0>(tuple6);
        delete pairA.first;
        delete get<1>(tupleB);
        delete get<2>(tupleB);
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP_TUPLE(sw2, x, tupleC, v2::bat::ops::mirrorHead_resoid_AN(get<0>(tupleB))); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete get<0>(tupleB);
        delete get<1>(tupleC);
        delete get<2>(tupleC);
        // BatF only contains the 
        MEASURE_OP_TUPLE(sw2, x, tupleD, v2::bat::ops::col_hashjoin_AN<v2_int_t>(get<0>(tupleC), batLEenc));
        delete get<1>(tupleD);
        delete get<2>(tupleD);
        MEASURE_OP_TUPLE(sw2, x, tupleE, v2::bat::ops::col_hashjoin_AN<v2_tinyint_t>(get<0>(tupleC), get<0>(tuple4)));
        delete batC;
        delete get<0>(tuple4);
        delete get<1>(tupleE);
        delete get<2>(tupleE);

        // 4) lazy decode
        MEASURE_OP(sw2, x, auto, batFpair, (v2::bat::ops::checkAndDecode_AN<v2_int_t>(get<0>(tupleD), TypeSelector<v2_int_t>::A_INV, TypeSelector<v2_int_t>::A_UNENC_MAX_U)), batFpair.first->size(), batFpair.first->consumption());
        auto batF = batFpair.first;
        SAVE_TYPE(x - 1, batF);
        delete batFpair.second;
        delete get<0>(tupleD);
        MEASURE_OP(sw2, x, auto, batGpair, (v2::bat::ops::checkAndDecode_AN<v2_tinyint_t>(get<0>(tupleE), TypeSelector<v2_tinyint_t>::A_INV, TypeSelector<v2_tinyint_t>::A_UNENC_MAX_U)), batGpair.first->size(), batGpair.first->consumption());
        auto batG = batGpair.first;
        SAVE_TYPE(x - 1, batG);
        delete batGpair.second;
        delete get<0>(tupleE);
        MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(batF, batG, 0));
        delete batF;
        delete batG;


        totalTimes[i] = sw1.stop();

        cout << "\n(" << setw(2) << i << ")\n\tresult: " << result << "\n\t  time: " << sw1 << " ns.";
        COUT_HEADLINE;
        COUT_RESULT(0, x);
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

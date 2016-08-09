/* 
 * File:   ssbm-q01.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include <cstdlib>
#include <algorithm>
#include <vector>
#include <iostream>
#include <iomanip>
#include <cassert>

#include <boost/filesystem.hpp>

#include <column_storage/ColumnBat.h>
#include <column_storage/TransactionManager.h>
#include <column_operators/operators.h>
#include <column_operators/operatorsAN.tcc>
#include <util/stopwatch.hpp>

using namespace std;

// define
// boost::throw_exception(std::runtime_error("Type name demangling failed"));
namespace boost {

    void throw_exception(std::exception const & e) { // user defined
        throw e;
    }
}

template<typename Head, typename Tail>
void printBat(BatIterator<Head, Tail > *iter, const char* message = nullptr, bool doDelete = true) {
    if (message) {
        cout << message << '\n';
    }
    size_t i = 0;
    while (iter->hasNext()) {
        auto p = iter->next();
        cout << i++ << ": " << p.first << " = " << p.second << '\n';
    }
    cout << flush;
    if (doDelete) {
        delete iter;
    }
}

#if not defined NDEBUG
#define PRINT_BAT(SW, PRINT) \
do {                         \
    SW.stop();               \
    PRINT;                   \
    SW.resume();             \
} while (false)
#else
#define PRINT_BAT(SW, PRINT)
#endif

#define SAVE_TYPE(I, BAT)          \
headTypes[I] = BAT->type_head();   \
tailTypes[I] = BAT->type_tail();   \
hasTwoTypes[I] = true

#define MEASURE_OP(...) VFUNC(MEASURE_OP, __VA_ARGS__)

#define MEASURE_OP7(SW, I, TYPE, VAR, OP, STORE_SIZE_OP, STORE_CONSUMPTION_OP) \
SW.start();                                \
TYPE VAR = OP;                             \
opTimes[I] = SW.stop();                    \
batSizes[I] = STORE_SIZE_OP;               \
batConsumptions[I] = STORE_CONSUMPTION_OP; \
++I

#define MEASURE_OP5(SW, I, TYPE, VAR, OP)                       \
MEASURE_OP7(SW, I, TYPE, VAR, OP, 1, sizeof(TYPE));             \
headTypes[I-1] = boost::typeindex::type_id<TYPE>().type_info(); \
hasTwoTypes[I-1] = false

#define MEASURE_OP4(SW, I, BAT, OP)                                     \
MEASURE_OP7(SW, I, auto, BAT, OP, BAT->size(), BAT->consumption());     \
SAVE_TYPE(I-1, BAT)

StopWatch::rep loadTable(string& baseDir, const char* const columnName) {
    static StopWatch sw;
    TransactionManager* tm = TransactionManager::getInstance();
    TransactionManager::Transaction* t = tm->beginTransaction(true);
    assert(t != nullptr);
    string path = baseDir + "/" + columnName;
    sw.start();
    size_t num = t->load(path.c_str(), columnName);
    sw.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << num << "\n\tTime: " << sw << " ns." << endl;
    tm->endTransaction(t);
    return sw.duration();
}

int main(int argc, char** argv) {
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

    typedef ColumnBat<unsigned, resint_t> resintColType;
    // typedef ColumnBat<unsigned, fxd_t> fxdColType;
    const size_t LEN_TYPES = 13;
    string emptyString;
    size_t x = 0;

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new resintColType("dateAN", "year"));
    MEASURE_OP(sw1, x, batDDcb, new resintColType("dateAN", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new resintColType("lineorderAN", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new resintColType("lineorderAN", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new resintColType("lineorderAN", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new resintColType("lineorderAN", "extendedprice"));

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

    for (size_t i = 0; i < x; ++i) {
        cout << "\n\t  op" << setw(2) << i << "\t" << setw(13) << hrc_duration(opTimes[i]) << "\t" << setw(10) << batSizes[i] << "\t" << setw(10) << batConsumptions[i] << "\t" << setw(LEN_TYPES) << headTypes[i].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[i] ? tailTypes[i].pretty_name() : emptyString);
    }
    cout << '\n' << endl;

    for (size_t i = 0; i < NUM_RUNS; ++i) {
        sw1.start();
        x = 0;

        // 1) select from lineorder
        MEASURE_OP(sw2, x, bat1, v2::bat::ops::selection_lt(batLQenc, (25 * ::A))); // lo_quantity < 25
        PRINT_BAT(sw1, printBat(bat1->begin(), "lo_quantity < 25"));
        MEASURE_OP(sw2, x, bat2, v2::bat::ops::selection_bt(batLDenc, 1 * ::A, 3 * ::A)); // lo_discount between 1 and 3
        PRINT_BAT(sw1, printBat(bat2->begin(), "lo_discount between 1 and 3"));
        MEASURE_OP(sw2, x, bat3, v2::bat::ops::mirror(bat1)); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        delete bat1;
        MEASURE_OP(sw2, x, bat4, v2::bat::ops::col_hashjoin(bat3, bat2)); // join selection
        delete bat3;
        delete bat2;
        MEASURE_OP(sw2, x, bat5, v2::bat::ops::mirror(bat4)); // prepare joined selection with lo_orderdate (contains positions in tail)
        PRINT_BAT(sw1, printBat(bat5->begin(), "lo_discount where lo_quantity < 25 and lo_discount between 1 and 3"));
        MEASURE_OP(sw2, x, bat6, v2::bat::ops::col_hashjoin(bat5, batLOenc)); // only those lo_orderdates where lo_quantity... and lo_discount
        delete bat5;
        PRINT_BAT(sw1, printBat(bat6->begin(), "lo_orderdates where lo_quantity < 25 and lo_discount between 1 and 3"));

        // 1) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(sw2, x, bat7, v2::bat::ops::selection_eq(batDYenc, 1993 * ::A)); // d_year = 1993
        PRINT_BAT(sw1, printBat(bat7->begin(), "d_year = 1993"));
        MEASURE_OP(sw2, x, bat8, v2::bat::ops::mirror(bat7)); // prepare joined selection over d_year and d_datekey
        delete bat7;
        MEASURE_OP(sw2, x, bat9, v2::bat::ops::col_hashjoin(bat8, batDDenc)); // only those d_datekey where d_year...
        delete bat8;
        PRINT_BAT(sw1, printBat(bat9->begin(), "d_datekey where d_year = 1993"));

        // 3) join lineorder and date
        MEASURE_OP(sw2, x, batA, v2::bat::ops::reverse(bat9));
        delete bat9;
        MEASURE_OP(sw2, x, batB, v2::bat::ops::col_hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        delete bat6;
        delete batA;
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP(sw2, x, batC, v2::bat::ops::mirror(batB)); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        delete batB;
        // BatF only contains the 
        MEASURE_OP(sw2, x, batD, v2::bat::ops::col_hashjoin(batC, batLEenc));
        PRINT_BAT(sw1, printBat(batD->begin(), "lo_extprice where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25"));
        MEASURE_OP(sw2, x, batE, v2::bat::ops::col_hashjoin(batC, bat4));
        delete batC;
        delete bat4;
        PRINT_BAT(sw1, printBat(batE->begin(), "lo_discount where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25"));
        MEASURE_OP(sw2, x, unsigned, count1, batD->size());
        MEASURE_OP(sw2, x, unsigned, count2, batE->size());

        // 4) lazy decode
        MEASURE_OP(sw2, x, auto, batFpair, (v2::bat::ops::checkAndDecodeA<unsigned, int_t>(batD)), batFpair.first->size(), batFpair.first->consumption());
        auto batF = batFpair.first;
        SAVE_TYPE(x - 1, batF);
        delete batFpair.second;
        delete batD;
        MEASURE_OP(sw2, x, auto, batGpair, (v2::bat::ops::checkAndDecodeA<unsigned, int_t>(batE)), batGpair.first->size(), batGpair.first->consumption());
        auto batG = batGpair.first;
        SAVE_TYPE(x - 1, batG);
        delete batGpair.second;
        delete batE;
        MEASURE_OP(sw2, x, uint64_t, result, v2::bat::ops::aggregate_mul_sum<uint64_t>(batF, batG, 0));
        delete batF;
        delete batG;


        totalTimes[i] = sw1.stop();

        cout << "(" << setw(2) << i << ")\n\tresult: " << result << "\n\t count: " << count1 << " | " << count2 << "\n\t  time: " << setw(13) << sw1 << " ns.";
        cout << "\n\t  name\t" << setw(13) << "time [ns]\t" << setw(10) << "size [#]\t" << setw(10) << "consum [B]\t" << setw(LEN_TYPES) << "type head\t" << setw(LEN_TYPES) << "type tail";
        for (size_t j = 0; j < x; ++j) {
            cout << "\n\t  op" << setw(2) << j << "\t" << setw(13) << hrc_duration(opTimes[j]) << "\t" << setw(10) << batSizes[j] << "\t" << setw(10) << batConsumptions[j] << "\t" << setw(LEN_TYPES) << headTypes[j].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[j] ? tailTypes[j].pretty_name() : emptyString);
        }
        cout << endl;
    }

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

    return 0;
}

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

#include "column_storage/TempBat.h"
#include "column_storage/ColumnBat.h"
#include "column_storage/ColumnManager.h"
#include "column_storage/TransactionManager.h"
#include "column_operators/operators.h"
#include "column_storage/types.h"
#include "util/stopwatch.hpp"

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
    loadTable(baseDir, "customer");
    loadTable(baseDir, "date");
    loadTable(baseDir, "lineorder");
    loadTable(baseDir, "part");
    loadTable(baseDir, "supplier");
    sw1.stop();
    cout << "Total loading time: " << sw1 << " ns." << endl;

    cout << "\nSSBM Q1.1:\nselect lo_extendedprice\n  from lineorder, date\n  where lo_orderdate = d_datekey\n    and d_year = 1993\n    and lo_discount between 1 and 3\n    and lo_quantity  < 25;" << endl;

    const size_t NUM_OPS = 32;
    nanoseconds::rep opTimes[NUM_OPS];
    size_t batSizes[NUM_OPS];
    size_t batConsumptions[NUM_OPS];
    bool hasTwoTypes[NUM_OPS];
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];

    typedef ColumnBat<unsigned, int_t> intColType;
    // typedef ColumnBat<unsigned, fxd_t> fxdColType;
    const size_t LEN_TYPES = 13;
    string emptyString;
    size_t x = 0;

    /* Measure loading ColumnBats */
    MEASURE_OP(sw1, x, batDYcb, new intColType("date", "year"));
    MEASURE_OP(sw1, x, batDDcb, new intColType("date", "datekey"));
    MEASURE_OP(sw1, x, batLQcb, new intColType("lineorder", "quantity"));
    MEASURE_OP(sw1, x, batLDcb, new intColType("lineorder", "discount"));
    MEASURE_OP(sw1, x, batLOcb, new intColType("lineorder", "orderdate"));
    MEASURE_OP(sw1, x, batLEcb, new intColType("lineorder", "extendedprice"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(sw1, x, batDY, Bat_Operators::copy(batDYcb));
    MEASURE_OP(sw1, x, batDD, Bat_Operators::copy(batDDcb));
    MEASURE_OP(sw1, x, batLQ, Bat_Operators::copy(batLQcb));
    MEASURE_OP(sw1, x, batLD, Bat_Operators::copy(batLDcb));
    MEASURE_OP(sw1, x, batLO, Bat_Operators::copy(batLOcb));
    MEASURE_OP(sw1, x, batLE, Bat_Operators::copy(batLEcb));

    for (size_t i = 0; i < x; ++i) {
        cout << "\n\t  op" << setw(2) << i << "\t" << setw(13) << hrc_duration(opTimes[i]) << "\t" << setw(10) << batSizes[i] << "\t" << setw(10) << batConsumptions[i] << "\t" << setw(LEN_TYPES) << headTypes[i].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[i] ? tailTypes[i].pretty_name() : emptyString);
    }
    cout << '\n' << endl;

    for (size_t i = 0; i < 1; ++i) {
        sw1.start();
        x = 5;

        // 1) select from lineorder
        MEASURE_OP(sw2, x, bat1, Bat_Operators::selection_lt(batLQ, 25)); // lo_quantity < 25
        PRINT_BAT(sw1, printBat(bat1->begin(), "lo_quantity < 25"));
        MEASURE_OP(sw2, x, bat2, Bat_Operators::selection_bt(batLD, 1, 3)); // lo_discount between 1 and 3
        PRINT_BAT(sw1, printBat(bat2->begin(), "lo_discount between 1 and 3"));
        MEASURE_OP(sw2, x, bat3, Bat_Operators::mirror(bat1)); // prepare joined selection (select from lineorder where lo_quantity... and lo_discount)
        MEASURE_OP(sw2, x, bat4, Bat_Operators::col_hashjoin(bat3, bat2)); // join selection
        MEASURE_OP(sw2, x, bat5, Bat_Operators::mirror(bat4)); // prepare joined selection with lo_orderdate (contains positions in tail)
        PRINT_BAT(sw1, printBat(bat5->begin(), "lo_discount where lo_quantity < 25 and lo_discount between 1 and 3"));
        MEASURE_OP(sw2, x, bat6, Bat_Operators::col_hashjoin(bat5, batLO)); // only those lo_orderdates where lo_quantity... and lo_discount
        PRINT_BAT(sw1, printBat(bat6->begin(), "lo_orderdates where lo_quantity < 25 and lo_discount between 1 and 3"));

        // 1) select from date (join inbetween to reduce the number of lines we touch in total)
        MEASURE_OP(sw2, x, bat7, Bat_Operators::selection_eq(batDY, 1993)); // d_year = 1993
        PRINT_BAT(sw1, printBat(bat7->begin(), "d_year = 1993"));
        MEASURE_OP(sw2, x, bat8, Bat_Operators::mirror(bat7)); // prepare joined selection over d_year and d_datekey
        MEASURE_OP(sw2, x, bat9, Bat_Operators::col_hashjoin(bat8, batDD)); // only those d_datekey where d_year...
        PRINT_BAT(sw1, printBat(bat9->begin(), "d_datekey where d_year = 1993"));

        // 3) join lineorder and date
        MEASURE_OP(sw2, x, batA, Bat_Operators::reverse(bat9));
        MEASURE_OP(sw2, x, batB, Bat_Operators::col_hashjoin(bat6, batA)); // only those lineorders where lo_quantity... and lo_discount... and d_year...
        // batE now has in the Head the positions from lineorder and in the Tail the positions from date
        MEASURE_OP(sw2, x, batC, Bat_Operators::mirror(batB)); // only those lineorder-positions where lo_quantity... and lo_discount... and d_year...
        // BatF only contains the 
        MEASURE_OP(sw2, x, batD, Bat_Operators::col_hashjoin(batC, batLE));
        PRINT_BAT(sw1, printBat(batD->begin(), "lo_extprice where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25"));
        MEASURE_OP(sw2, x, unsigned, count1, batD->size());
        MEASURE_OP(sw2, x, batE, Bat_Operators::col_hashjoin(batC, bat4));
        MEASURE_OP(sw2, x, unsigned, count2, batE->size());
        MEASURE_OP(sw2, x, uint64_t, result, Bat_Operators::aggregate_mul_sum<uint64_t>(batD, batE, 0));

        delete bat1;
        delete bat2;
        delete bat3;
        delete bat4;
        delete bat5;
        delete bat6;
        delete bat7;
        delete bat8;
        delete bat9;
        delete batA;
        delete batB;
        delete batC;
        delete batD;
        delete batE;

        sw1.stop();

        cout << "(" << setw(2) << i << ")\n\tresult: " << result << "\n\t count: " << count1 << " | " << count2 << "\n\t  time: " << setw(13) << sw1 << " ns.";
        cout << "\n\t  name\t" << setw(13) << "time [ns]\t" << setw(10) << "size [#]\t" << setw(10) << "consum [B]\t" << setw(LEN_TYPES) << "type head\t" << setw(LEN_TYPES) << "type tail";
        for (size_t j = 5; j < x; ++j) {
            cout << "\n\t  op" << setw(2) << j << "\t" << setw(13) << hrc_duration(opTimes[j]) << "\t" << setw(10) << batSizes[j] << "\t" << setw(10) << batConsumptions[j] << "\t" << setw(LEN_TYPES) << headTypes[j].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[j] ? tailTypes[j].pretty_name() : emptyString);
        }
        cout << endl;
    }

    delete batDY;
    delete batDD;
    delete batLQ;
    delete batLD;
    delete batLO;
    delete batLE;
    delete batDYcb;
    delete batDDcb;
    delete batLQcb;
    delete batLDcb;
    delete batLOcb;
    delete batLEcb;

    return 0;
}

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

#include <ColumnStore.h>
#include <column_storage/ColumnBat.h>
#include <column_storage/TransactionManager.h>
#include <column_operators/operators.h>
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
SW.start();                               \
TYPE VAR = OP;                            \
opTimes[I] = SW.stop();                   \
batSizes[I] = STORE_SIZE_OP;              \
batConsumptions[I] = STORE_CONSUMPTION_OP

#define MEASURE_OP5(SW, I, TYPE, VAR, OP)                     \
MEASURE_OP7(SW, I, TYPE, VAR, OP, 1, sizeof(TYPE));           \
headTypes[I] = boost::typeindex::type_id<TYPE>().type_info(); \
hasTwoTypes[I] = false

#define MEASURE_OP4(SW, I, BAT, OP)                                     \
MEASURE_OP7(SW, I, auto, BAT, OP, BAT->size(), BAT->consumption());     \
SAVE_TYPE(I, BAT)

int main(int argc, char** argv) {
    boost::filesystem::path p(argc == 1 ? argv[0] : argv[1]);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    TransactionManager* tm = TransactionManager::getInstance();
    TransactionManager::Transaction* t;
    StopWatch sw1, sw2;
    string path;
    size_t num;

    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw2.start();
    path = baseDir;
    path += "/lineorder2"; // header file only contains the first column definition
    num = t->load(path.c_str(), "lineorder");
    sw2.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << num << "\n\tTime: " << sw2 << " ns." << endl;
    tm->endTransaction(t);

    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw2.start();
    path = baseDir;
    path += "/lineorder2AN"; // header file only contains the first column definition
    num = t->load(path.c_str(), "lineorderAN");
    sw2.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << num << "\n\tTime: " << sw2 << " ns." << endl;
    tm->endTransaction(t);

    const size_t NUM_OPS = 17;
    nanoseconds::rep opTimes[NUM_OPS];
    size_t batSizes[NUM_OPS];
    size_t batConsumptions[NUM_OPS];
    bool hasTwoTypes[NUM_OPS];
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];

    typedef ColumnBat<unsigned, int_t> intColType;
    typedef ColumnBat<unsigned, resint_t> resintColType;
    const size_t LEN_TYPES = 13;
    string emptyString;

    // orderkey|linenumber|custkey|partkey|suppkey|orderdate|orderpriority|shippriority|quantity|extendedprice|ordertotalprice|discount|revenue|supplycost|tax|commitdate|shipmode
    // INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|STRING|STRING|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|STRING
    // RESINT|RESINT|RESINT|RESINT|RESINT|RESINT|STRING|STRING|RESINT|RESINT|RESINT|RESINT|RESINT|RESINT|RESINT|RESINT|STRING

    size_t x = 0;
    MEASURE_OP(sw1, x, batOKbcOrg, new intColType("lineorder", "orderkey"));
    x++;
    MEASURE_OP(sw1, x, batOKbcEnc, new resintColType("lineorderAN", "orderkey"));
    x++;
    MEASURE_OP(sw1, x, batOKtcEnc, v2::bat::ops::copy(batOKbcEnc));
    x++;
    MEASURE_OP(sw1, x, batOKtcEnc2, v2::bat::ops::copy(batOKtcEnc));
    x++;
    delete batOKtcEnc2;
    for (size_t i = 0; i < x; ++i) {
        cout << "\n\t  op" << setw(2) << i << "\t" << setw(13) << hrc_duration(opTimes[i]) << "\t" << setw(10) << batSizes[i] << "\t" << setw(10) << batConsumptions[i] << "\t" << setw(LEN_TYPES) << headTypes[i].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[i] ? tailTypes[i].pretty_name() : emptyString);
    }
    cout << "\n\n";
    cout << " num |         check |        decode |  check+decode\n";
    cout << "-----+---------------+---------------+--------------" << endl;

    for (size_t i = 0; i < 10; ++i) {
        sw1.start();
        auto result1 = v2::bat::ops::checkA(batOKtcEnc);
        sw1.stop();
        cout << "  " << setw(2) << i << "   " << setw(13) << sw1.duration();

        delete result1;

        sw1.start();
        auto result2 = v2::bat::ops::decodeA<unsigned, int_t>(batOKtcEnc);
        sw1.stop();
        cout << "   " << setw(13) << sw1.duration();

        delete result2;

        sw1.start();
        auto result3 = v2::bat::ops::checkAndDecodeA<unsigned, int_t>(batOKtcEnc);
        sw1.stop();
        cout << "   " << setw(13) << sw1.duration() << '\n';

        delete result3.first;
        delete result3.second;
    }

    return 0;
}

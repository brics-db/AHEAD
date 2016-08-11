/* 
 * File:   ssbm-q01.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include <type_traits>

#include "ssbm.hpp"

StopWatch sw1;
const size_t NUM_OPS = 17;
nanoseconds::rep opTimes[NUM_OPS];
size_t batSizes[NUM_OPS];
size_t batConsumptions[NUM_OPS];
bool hasTwoTypes[NUM_OPS];
boost::typeindex::type_index headTypes[NUM_OPS];
boost::typeindex::type_index tailTypes[NUM_OPS];

const size_t NUM_RUNS = 10;
const size_t LEN_TIMES = 13;
const size_t LEN_SIZES = 12;
const size_t LEN_TYPES = 16;
string emptyString;

template<typename T>
struct TypeSelector;

template<>
struct TypeSelector<tinyint_t> {
    typedef tinyint_t base_t;
    typedef tinyint_col_t col_t;
    typedef restiny_t res_t;
    typedef restiny_col_t res_col_t;

    static const res_t A;
    static const res_t A_INV;
    static const res_t A_UNENC_MAX;
    static const res_t A_UNENC_MAX_U;
};

const TypeSelector<tinyint_t>::res_t TypeSelector<tinyint_t>::A = ::A_TINY;
const TypeSelector<tinyint_t>::res_t TypeSelector<tinyint_t>::A_INV = ::A_TINY_INV;
const TypeSelector<tinyint_t>::res_t TypeSelector<tinyint_t>::A_UNENC_MAX = ::A_TINY_UNENC_MAX;
const TypeSelector<tinyint_t>::res_t TypeSelector<tinyint_t>::A_UNENC_MAX_U = ::A_TINY_UNENC_MAX_U;

template<>
struct TypeSelector<shortint_t> {
    typedef shortint_t base_t;
    typedef shortint_col_t col_t;
    typedef resshort_t res_t;
    typedef resshort_col_t res_col_t;

    static const res_t A;
    static const res_t A_INV;
    static const res_t A_UNENC_MAX;
    static const res_t A_UNENC_MAX_U;
};

const TypeSelector<shortint_t>::res_t TypeSelector<shortint_t>::A = ::A_SHORT;
const TypeSelector<shortint_t>::res_t TypeSelector<shortint_t>::A_INV = ::A_SHORT_INV;
const TypeSelector<shortint_t>::res_t TypeSelector<shortint_t>::A_UNENC_MAX = ::A_SHORT_UNENC_MAX;
const TypeSelector<shortint_t>::res_t TypeSelector<shortint_t>::A_UNENC_MAX_U = ::A_SHORT_UNENC_MAX_U;

template<>
struct TypeSelector<int_t> {
    typedef int_t base_t;
    typedef int_col_t col_t;
    typedef resint_t res_t;
    typedef resint_col_t res_col_t;

    static const res_t A;
    static const res_t A_INV;
    static const res_t A_UNENC_MAX;
    static const res_t A_UNENC_MAX_U;
};

const TypeSelector<int_t>::res_t TypeSelector<int_t>::A = ::A_INT;
const TypeSelector<int_t>::res_t TypeSelector<int_t>::A_INV = ::A_INT_INV;
const TypeSelector<int_t>::res_t TypeSelector<int_t>::A_UNENC_MAX = ::A_INT_UNENC_MAX;
const TypeSelector<int_t>::res_t TypeSelector<int_t>::A_UNENC_MAX_U = ::A_INT_UNENC_MAX_U;

template<typename BaseType>
void runTable(const char* strTable, const char* strTableAN, const char* strColumn) {
    size_t x = 0;
    MEASURE_OP(sw1, x, batBc, new typename TypeSelector<BaseType>::col_t(strTable, strColumn));
    MEASURE_OP(sw1, x, batBcAN, new typename TypeSelector<BaseType>::res_col_t(strTableAN, strColumn));
    MEASURE_OP(sw1, x, batTcAN, v2::bat::ops::copy(batBcAN));
    MEASURE_OP(sw1, x, batTcAN2, v2::bat::ops::copy(batTcAN));

    cout << "\n" << strTable << '.' << strColumn << ":";

    cout << "\n  name\t" << setw(LEN_TIMES) << "time [ns]" << '\t' << setw(LEN_SIZES) << "size [#]" << '\t' << setw(LEN_SIZES) << "consum [B]" << '\t' << setw(LEN_TYPES) << "type head" << '\t' << setw(LEN_TYPES) << "type tail";
    for (size_t i = 0; i < x; ++i) {
        cout << "\n  op" << setw(2) << i << "\t" << setw(LEN_TIMES) << hrc_duration(opTimes[i]) << "\t" << setw(LEN_SIZES) << batSizes[i] << "\t" << setw(LEN_SIZES) << batConsumptions[i] << "\t" << setw(LEN_TYPES) << headTypes[i].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[i] ? tailTypes[i].pretty_name() : emptyString);
    }
    cout << "\n+-----+ ORDERKEY      +---------------+---------------+\n";
    cout << "| num |         check |        decode |  check+decode |\n";
    cout << "+-----+---------------+---------------+---------------+\n";

    for (size_t i = 0; i < NUM_RUNS; ++i) {
        cout << "|  " << setw(2) << i;

        sw1.start();
        auto result1 = v2::bat::ops::checkA(batTcAN, TypeSelector<BaseType>::A_INV, TypeSelector<BaseType>::A_UNENC_MAX_U);
        sw1.stop();
        cout << "   " << setw(LEN_TIMES) << sw1.duration() << flush;

        delete result1;

        sw1.start();
        auto result2 = v2::bat::ops::decodeA<typename TypeSelector<BaseType>::base_t > (batTcAN, TypeSelector<BaseType>::A_INV, TypeSelector<BaseType>::A_UNENC_MAX_U);
        sw1.stop();
        cout << "   " << setw(LEN_TIMES) << sw1.duration() << flush;

        delete result2;

        sw1.start();
        auto result3 = v2::bat::ops::checkAndDecodeA<typename TypeSelector<BaseType>::base_t > (batTcAN, TypeSelector<BaseType>::A_INV, TypeSelector<BaseType>::A_UNENC_MAX_U);
        sw1.stop();
        cout << "   " << setw(LEN_TIMES) << sw1.duration() << " |" << endl;

        delete result3.first;
        delete result3.second;
    }
    cout << "+-----+---------------+---------------+---------------+\n";
    delete batTcAN2;
    delete batTcAN;
    delete batBcAN;
    delete batBc;
}

int main(int argc, char** argv) {
    cout << "lineorder_size\n==============" << endl;

    boost::filesystem::path p(argc == 1 ? argv[0] : argv[1]);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());


    loadTable(baseDir, "lineorder");
    loadTable(baseDir, "lineorderAN");

    // Lineorder:
    // orderkey|linenumber|custkey|partkey|suppkey|orderdate|orderpriority|shippriority|quantity|extendedprice|ordertotalprice|discount|revenue|supplycost|tax|commitdate|shipmode
    // INTEGER|SHORTINT|INTEGER|INTEGER|INTEGER|INTEGER|STRING:15|CHAR|TINYINT|INTEGER|INTEGER|TINYINT|INTEGER|INTEGER|TINYINT|INTEGER|STRING:10
    // RESINT|RESSHORT|RESINT|RESINT|RESINT|RESINT|STRING:15|CHAR|RESTINY|RESINT|RESINT|RESTINY|RESINT|RESINT|RESTINY|RESINT|STRING

    // RESINT
    // ORDERKEY
    runTable<int_t>("lineorder", "lineorderAN", "orderkey");


    // RESSHORT
    // LINENUMBER
    runTable<shortint_t>("lineorder", "lineorderAN", "linenumber");



    // RESTINY
    // QUANTITY
    runTable<tinyint_t>("lineorder", "lineorderAN", "quantity");

    cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB." << endl;

    return 0;
}

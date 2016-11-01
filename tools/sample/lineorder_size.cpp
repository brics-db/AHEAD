/* 
 * File:   ssbm-q01.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include <type_traits>

#include "../ssbm/ssbm.hpp"

StopWatch sw1;
const size_t NUM_OPS = 32;
nanoseconds::rep opTimes[NUM_OPS];
size_t batSizes[NUM_OPS];
size_t batConsumptions[NUM_OPS];
bool hasTwoTypes[NUM_OPS];
boost::typeindex::type_index headTypes[NUM_OPS];
boost::typeindex::type_index tailTypes[NUM_OPS];

const size_t NUM_RUNS = 10;
string emptyString;

template<typename Tail>
void runTable(const char* strTable, const char* strTableAN, const char* strColumn) {
    typedef typename TypeMap<Tail>::v2_base_t v2_base_t;
    typedef typename TypeMap<Tail>::v2_encoded_t v2_encoded_t;

    size_t x = 0;
    MEASURE_OP(sw1, x, batBc, (new ColumnBat<v2_oid_t, v2_base_t>(strTable, strColumn)));
    MEASURE_OP(sw1, x, batBcAN, (new ColumnBat<v2_oid_t, v2_encoded_t>(strTableAN, strColumn)));
    MEASURE_OP(sw1, x, batTcAN, v2::bat::ops::copy(batBcAN));

    cout << "\n#runTable(" << strTable << '.' << strColumn << ")";

    cout << "\nname\t" << setw(LEN_TIMES) << "time [ns]" << '\t' << setw(LEN_SIZES) << "size [#]" << '\t' << setw(LEN_SIZES) << "consum [B]" << '\t' << setw(LEN_TYPES) << "type head" << '\t' << setw(LEN_TYPES) << "type tail";
    for (size_t i = 0; i < x; ++i) {
        cout << "\nop" << setw(2) << i << "\t" << setw(LEN_TIMES) << hrc_duration(opTimes[i]) << "\t" << setw(LEN_SIZES) << batSizes[i] << "\t" << setw(LEN_SIZES) << batConsumptions[i] << "\t" << setw(LEN_TYPES) << headTypes[i].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[i] ? tailTypes[i].pretty_name() : emptyString);
    }
    cout << "\nnum\tcopy-" << TypeMap<v2_encoded_t>::TYPENAME << "\tcheck\tdecode\tcheck+decode\tcopy-" << TypeMap<v2_base_t>::TYPENAME << endl;

    for (size_t i = 0; i < NUM_RUNS; ++i) {
        cout << setw(3) << i << flush;

        sw1.start();
        auto result0 = v2::bat::ops::copy(batTcAN);
        sw1.stop();
        cout << '\t' << setw(LEN_TIMES) << sw1.duration() << flush;
        delete result0;

        sw1.start();
        auto result1 = v2::bat::ops::checkAN(batTcAN);
        sw1.stop();
        cout << '\t' << setw(LEN_TIMES) << sw1.duration() << flush;
        delete result1;

        sw1.start();
        auto result2 = v2::bat::ops::decodeAN(batTcAN);
        sw1.stop();
        cout << '\t' << setw(LEN_TIMES) << sw1.duration() << flush;
        delete result2;

        sw1.start();
        auto tuple3 = v2::bat::ops::checkAndDecodeAN(batTcAN);
        if (get<1>(tuple3))
            delete get<1>(tuple3);
        if (get<2>(tuple3))
            delete get<2>(tuple3);
        sw1.stop();
        cout << '\t' << setw(LEN_TIMES) << sw1.duration() << flush;

        sw1.start();
        auto result4 = v2::bat::ops::copy(get<0>(tuple3));
        sw1.stop();
        cout << '\t' << setw(LEN_TIMES) << sw1.duration() << endl;
        delete result4;
        delete get<0>(tuple3);
    }
    delete batTcAN;
    delete batBcAN;
    delete batBc;
}

template<typename Tail>
void runTable2(const char* strTable, const char* strTableAN, const char* strColumn) {
    typedef typename TypeMap<Tail>::v2_base_t v2_base_t;
    typedef typename TypeMap<Tail>::v2_encoded_t v2_encoded_t;

    const size_t MAX_SCALE = 10;
    Bat<v2_oid_t, v2_encoded_t>* bats[MAX_SCALE];
    auto batBc = new ColumnBat<v2_oid_t, v2_base_t>(strTable, strColumn);
    auto batBcAN = new ColumnBat<v2_oid_t, v2_encoded_t>(strTableAN, strColumn);

    cout << "\n#runTable2(" << strTable << '.' << strColumn << ')';

    cout << "\n#sizes";
    size_t szBats = 0;
    for (size_t i = 0; i < MAX_SCALE; ++i) {
        bats[i] = v2::bat::ops::copy(batBcAN);
        szBats += bats[i]->size();
        cout << '\n' << setw(2) << i << '\t' << szBats << flush;
    }

    cout << "\n#NUM_RUNS=" << NUM_RUNS << " (times are averages over as many runs)\nnum\tcheck\tdecode\tcheck+decode" << flush;

    for (size_t scale = 1; scale <= MAX_SCALE; ++scale) {
        cout << '\n' << setw(3) << scale << flush;

        StopWatch::rep totalTime = 0;
        for (size_t i = 0; i < NUM_RUNS; ++i) {
            sw1.start();
            for (size_t scale2 = 0; scale2 < scale; ++scale2) {
                auto result = v2::bat::ops::checkAN(bats[scale2]);
                delete result;
            }
            sw1.stop();
            totalTime += sw1.duration();
        }
        cout << '\t' << setw(LEN_TIMES) << (totalTime / NUM_RUNS) << flush;

        totalTime = 0;
        for (size_t i = 0; i < NUM_RUNS; ++i) {
            sw1.start();
            for (size_t scale2 = 0; scale2 < scale; ++scale2) {
                auto result = v2::bat::ops::decodeAN(bats[scale2]);
                delete result;
            }
            sw1.stop();
            totalTime += sw1.duration();
        }
        cout << '\t' << setw(LEN_TIMES) << (totalTime / NUM_RUNS) << flush;

        totalTime = 0;
        for (size_t i = 0; i < NUM_RUNS; ++i) {
            sw1.start();
            for (size_t scale2 = 0; scale2 < scale; ++scale2) {
                auto tuple = v2::bat::ops::checkAndDecodeAN(bats[scale2]);
                delete get<0>(tuple);
                if (get<1>(tuple))
                    delete get<1>(tuple);
                if (get<2>(tuple))
                    delete get<2>(tuple);
            }
            sw1.stop();
            totalTime += sw1.duration();
        }
        cout << '\t' << setw(LEN_TIMES) << (totalTime / NUM_RUNS) << flush;
    }
    for (size_t i = 0; i < MAX_SCALE; ++i) {
        delete bats[i];
    }
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
    runTable2<v2_int_t>("lineorder", "lineorderAN", "orderkey");
    runTable<v2_int_t>("lineorder", "lineorderAN", "orderkey");

    // RESSHORT
    // LINENUMBER
    runTable<v2_shortint_t>("lineorder", "lineorderAN", "linenumber");

    // RESTINY
    // QUANTITY
    runTable<v2_tinyint_t>("lineorder", "lineorderAN", "quantity");

    cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB." << endl;

    return 0;
}

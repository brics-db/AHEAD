// Copyright (c) 2016 Till Kolditz
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

/* 
 * File:   ssbm.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10. August 2016, 12:40
 */

#ifndef SSBM_HPP
#define SSBM_HPP


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
#include <util/resilience.hpp>
#include <util/rss.hpp>
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
    for (; iter->hasNext(); ++iter) {
        cout << i++ << ": " << iter->head() << " = " << iter->tail() << '\n';
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

#define MEASURE_OP5(SW, I, TYPE, VAR, OP)                        \
MEASURE_OP7(SW, I, TYPE, VAR, OP, 1, sizeof(TYPE));              \
headTypes[I-1] = boost::typeindex::type_id<TYPE>().type_info();  \
hasTwoTypes[I-1] = false

#define MEASURE_OP4(SW, I, BAT, OP)                                      \
MEASURE_OP7(SW, I, auto, BAT, OP, BAT->size(), BAT->consumption());      \
SAVE_TYPE(I-1, BAT)

#define MEASURE_OP_PAIR(SW, I, PAIR, OP)                                           \
MEASURE_OP7(SW, I, auto, PAIR, OP, PAIR.first->size(), PAIR.first->consumption()); \
SAVE_TYPE(I-1, PAIR.first)

#define MEASURE_OP_TUPLE(SW, I, TUPLE, OP)                                                \
MEASURE_OP7(SW, I, auto, TUPLE, OP, get<0>(TUPLE)->size(), get<0>(TUPLE)->consumption()); \
SAVE_TYPE(I-1, (get<0>(TUPLE)))

#define COUT_HEADLINE \
do { \
    cout << "\n\tname\t" << setw(LEN_TIMES) << "time [ns]" << "\t" << setw(LEN_SIZES) << "size [#]" << "\t" << setw(LEN_SIZES) << "consum [B]" << "\t" << setw(LEN_TYPES) << "type head" << "\t" << setw(LEN_TYPES) << "type tail"; \
} while (0)

#define COUT_RESULT(...) VFUNC(COUT_RESULT, __VA_ARGS__)
#define COUT_RESULT3(START, MAX, OPNAMES) \
do { \
    for (size_t k = START; k < MAX; ++k) { \
        cout << "\n\top" << setw(2) << OPNAMES[k] << "\t" << setw(LEN_TIMES) << hrc_duration(opTimes[k]) << "\t" << setw(LEN_SIZES) << batSizes[k] << "\t" << setw(LEN_SIZES) << batConsumptions[k] << "\t" << setw(LEN_TYPES) << headTypes[k].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[k] ? tailTypes[k].pretty_name() : emptyString); \
    } \
    cout << flush; \
} while (0)
#define COUT_RESULT2(START, MAX) \
do { \
    for (size_t k = START; k < MAX; ++k) { \
        cout << "\n\top" << setw(2) << k << "\t" << setw(LEN_TIMES) << hrc_duration(opTimes[k]) << "\t" << setw(LEN_SIZES) << batSizes[k] << "\t" << setw(LEN_SIZES) << batConsumptions[k] << "\t" << setw(LEN_TYPES) << headTypes[k].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[k] ? tailTypes[k].pretty_name() : emptyString); \
    } \
    cout << flush; \
} while (0)

StopWatch::rep loadTable(string& baseDir, const char* const columnName) {
    StopWatch sw;
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

const size_t LEN_TIMES = 13;
const size_t LEN_TYPES = 16;
const size_t LEN_SIZES = 12;

#endif /* SSBM_HPP */

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
// #include <cassert>

#include <boost/filesystem.hpp>
// #include <boost/algorithm/string/predicate.hpp>
// #include <boost/lexical_cast.hpp>

#include <util/argumentparser.hpp>
#include <util/rss.hpp>
#include <util/stopwatch.hpp>

#include <column_storage/ColumnBat.h>
#include <column_storage/TransactionManager.h>
#include <column_operators/Operators.hpp>

// define
// boost::throw_exception(std::runtime_error("Type name demangling failed"));
namespace boost {

    void
    throw_exception (std::exception const & e) { // user defined
        throw e;
    }
}

template<typename Head, typename Tail>
void
printBat (BATIterator<Head, Tail > *iter, const char* message = nullptr, bool doDelete = true) {
    if (message) {
        std::cout << message << '\n';
    }
    size_t i = 0;
    for (; iter->hasNext(); ++*iter) {
        std::cout << i++ << ": " << iter->head() << " = " << iter->tail() << '\n';
    }
    std::cout << std::flush;
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
MEASURE_OP7(SW, I, auto, TUPLE, OP, std::get<0>(TUPLE)->size(), std::get<0>(TUPLE)->consumption()); \
SAVE_TYPE(I-1, (std::get<0>(TUPLE)))

#define COUT_HEADLINE \
do { \
    std::cout << "\tname\t" << setw(CONFIG.LEN_TIMES) << "time [ns]" << "\t" << setw(CONFIG.LEN_SIZES) << "size [#]" << "\t" << setw(CONFIG.LEN_SIZES) << "consum [B]" << "\t" << setw(CONFIG.LEN_TYPES) << "type head" << "\t" << setw(CONFIG.LEN_TYPES) << "type tail\n"; \
} while (0)

#define COUT_RESULT(...) VFUNC(COUT_RESULT, __VA_ARGS__)
#define COUT_RESULT3(START, MAX, OPNAMES) \
do { \
    for (size_t k = START; k < MAX; ++k) { \
        std::cout << "\top" << setw(2) << OPNAMES[k] << "\t" << setw(CONFIG.LEN_TIMES) << hrc_duration(opTimes[k]) << "\t" << setw(CONFIG.LEN_SIZES) << batSizes[k] << "\t" << setw(CONFIG.LEN_SIZES) << batConsumptions[k] << "\t" << setw(CONFIG.LEN_TYPES) << headTypes[k].pretty_name() << "\t" << setw(CONFIG.LEN_TYPES) << (hasTwoTypes[k] ? tailTypes[k].pretty_name() : emptyString) << '\n'; \
    } \
    std::cout << flush; \
} while (0)
#define COUT_RESULT2(START, MAX) \
do { \
    for (size_t k = START; k < MAX; ++k) { \
        std::cout << "\top" << setw(2) << k << "\t" << setw(CONFIG.LEN_TIMES) << hrc_duration(opTimes[k]) << "\t" << setw(CONFIG.LEN_SIZES) << batSizes[k] << "\t" << setw(CONFIG.LEN_SIZES) << batConsumptions[k] << "\t" << setw(CONFIG.LEN_TYPES) << headTypes[k].pretty_name() << "\t" << setw(CONFIG.LEN_TYPES) << (hasTwoTypes[k] ? tailTypes[k].pretty_name() : emptyString) << '\n'; \
    } \
    std::cout << std::flush; \
} while (0)

#define CLEAR_SELECT_AN(pair)    \
do {                             \
    if (std::get<1>(pair)) {          \
        delete std::get<1>(pair);     \
    }                            \
} while(0)

#define CLEAR_HASHJOIN_AN(tuple) \
do {                             \
    if (std::get<1>(tuple))           \
        delete std::get<1>(tuple);    \
    if (std::get<2>(tuple))           \
        delete std::get<2>(tuple);    \
    if (std::get<3>(tuple))           \
        delete std::get<3>(tuple);    \
    if (std::get<4>(tuple))           \
        delete std::get<4>(tuple);    \
} while (0)

#define CLEAR_CHECKANDDECODE_AN(tuple) \
do {                                   \
    if (std::get<1>(tuple))                 \
        delete std::get<1>(tuple);          \
    if (std::get<2>(tuple))                 \
        delete std::get<2>(tuple);          \
} while (0)

#define CLEAR_GROUPBY_AN(tuple)        \
do {                                   \
    if (std::get<2>(tuple))                 \
        delete std::get<2>(tuple);          \
    if (std::get<3>(tuple))                 \
        delete std::get<3>(tuple);          \
} while (0)

#define CLEAR_GROUPEDSUM_AN(tuple)     \
do {                                   \
    if (std::get<5>(tuple))                 \
        delete std::get<5>(tuple);          \
    if (std::get<6>(tuple))                 \
        delete std::get<6>(tuple);          \
    if (std::get<7>(tuple))                 \
        delete std::get<7>(tuple);          \
    if (std::get<8>(tuple))                 \
        delete std::get<8>(tuple);          \
    if (std::get<9>(tuple))                 \
        delete std::get<9>(tuple);          \
    if (std::get<10>(tuple))                \
        delete std::get<10>(tuple);         \
} while (0)

///////////////////////////////
// CMDLINE ARGUMENT PARSING  //
///////////////////////////////

struct ssbmconf_t {

    typedef typename ArgumentParser::alias_list_t alias_list_t;

    size_t NUM_RUNS;
    size_t LEN_TIMES;
    size_t LEN_TYPES;
    size_t LEN_SIZES;
    string DB_PATH;
    bool VERBOSE;
    bool PRINT_RESULT;

private:
    ArgumentParser parser;

public:

    ssbmconf_t () : NUM_RUNS (0), LEN_TIMES (0), LEN_TYPES (0), LEN_SIZES (0), DB_PATH (), VERBOSE (false), PRINT_RESULT (0), parser ({
        std::forward_as_tuple("numruns", alias_list_t
        {"--numruns", "-n"}, 15),
        std::forward_as_tuple("lentimes", alias_list_t
        {"--lentimes"}, 13),
        std::forward_as_tuple("lentypes", alias_list_t
        {"--lentypes"}, 16),
        std::forward_as_tuple("lensizes", alias_list_t
        {"--lensizes"}, 12)
    },
    {
        std::forward_as_tuple("dbpath", alias_list_t{"--dbpath", "-d"}, ".")
    },
    {

        std::forward_as_tuple("verbose", alias_list_t{"--verbose", "-v"}, false),
        std::forward_as_tuple("printresult", alias_list_t{"--print-result", "-p"}, false)
    }) {
#ifdef DEBUG
        std::cout << "ssbmconf_t()" << std::endl;
#endif
    }

    ssbmconf_t (int argc, char** argv) : ssbmconf_t () {
#ifdef DEBUG
        std::cout << "ssbmconf_t(int argc, char** argv)" << std::endl;
#endif
        init(argc, argv);
    }

    void
    init (int argc, char** argv) {
#ifdef DEBUG
        std::cout << "ssbmconf_t::init(int argc, char** argv)" << std::endl;
#endif
        parser.parse(argc, argv, 1);
        NUM_RUNS = parser.get_uint("numruns");
        LEN_TIMES = parser.get_uint("lentimes");
        LEN_TYPES = parser.get_uint("lentypes");
        LEN_SIZES = parser.get_uint("lensizes");
        DB_PATH = parser.get_str("dbpath");
        VERBOSE = parser.get_bool("verbose");
        PRINT_RESULT = parser.get_bool("printresult");
    }
};

StopWatch::rep
loadTable (string& baseDir, const char* const columnName, const ssbmconf_t & CONFIG) {
    StopWatch sw;
    TransactionManager* tm = TransactionManager::getInstance();
    TransactionManager::Transaction* t = tm->beginTransaction(true);
    assert(t != nullptr);
    string path = baseDir + "/" + columnName;
    sw.start();
    size_t num = t->load(path.c_str(), columnName);
    sw.stop();
    if (CONFIG.VERBOSE) {
        std::cout << "File: " << path << "\n\tNumber of BUNs: " << num << "\n\tTime: " << sw << " ns." << std::endl;
    }
    tm->endTransaction(t);
    return sw.duration();
}

#endif /* SSBM_HPP */

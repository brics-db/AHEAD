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
#include <map>
#include <sstream>

#include <boost/filesystem.hpp>

#include <column_storage/ColumnBat.h>
#include <column_storage/TransactionManager.h>
#include <column_operators/Operators.h>
#include <column_operators/OperatorsAN.tcc>
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
    cout << "\n\tname\t" << setw(CONFIG.LEN_TIMES) << "time [ns]" << "\t" << setw(CONFIG.LEN_SIZES) << "size [#]" << "\t" << setw(CONFIG.LEN_SIZES) << "consum [B]" << "\t" << setw(CONFIG.LEN_TYPES) << "type head" << "\t" << setw(CONFIG.LEN_TYPES) << "type tail"; \
} while (0)

#define COUT_RESULT(...) VFUNC(COUT_RESULT, __VA_ARGS__)
#define COUT_RESULT3(START, MAX, OPNAMES) \
do { \
    for (size_t k = START; k < MAX; ++k) { \
        cout << "\n\top" << setw(2) << OPNAMES[k] << "\t" << setw(CONFIG.LEN_TIMES) << hrc_duration(opTimes[k]) << "\t" << setw(CONFIG.LEN_SIZES) << batSizes[k] << "\t" << setw(CONFIG.LEN_SIZES) << batConsumptions[k] << "\t" << setw(CONFIG.LEN_TYPES) << headTypes[k].pretty_name() << "\t" << setw(CONFIG.LEN_TYPES) << (hasTwoTypes[k] ? tailTypes[k].pretty_name() : emptyString); \
    } \
    cout << flush; \
} while (0)
#define COUT_RESULT2(START, MAX) \
do { \
    for (size_t k = START; k < MAX; ++k) { \
        cout << "\n\top" << setw(2) << k << "\t" << setw(CONFIG.LEN_TIMES) << hrc_duration(opTimes[k]) << "\t" << setw(CONFIG.LEN_SIZES) << batSizes[k] << "\t" << setw(CONFIG.LEN_SIZES) << batConsumptions[k] << "\t" << setw(CONFIG.LEN_TYPES) << headTypes[k].pretty_name() << "\t" << setw(CONFIG.LEN_TYPES) << (hasTwoTypes[k] ? tailTypes[k].pretty_name() : emptyString); \
    } \
    cout << flush; \
} while (0)

#define CLEAR_HASHJOIN_AN(tuple) \
do {                             \
    if (get<1>(tuple))           \
        delete get<1>(tuple);    \
    if (get<2>(tuple))           \
        delete get<2>(tuple);    \
    if (get<3>(tuple))           \
        delete get<3>(tuple);    \
    if (get<4>(tuple))           \
        delete get<4>(tuple);    \
} while (0)

#define CLEAR_CHECKANDDECODE_AN(tuple) \
do {                                   \
    if (get<1>(tuple))                 \
        delete get<1>(tuple);          \
    if (get<2>(tuple))                 \
        delete get<2>(tuple);          \
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

///////////////////////////////
// CMDLINE ARGUMENT PARSING  //
///////////////////////////////
map<string, size_t> cmdIntArgs = {
    {"--numruns", 15},
    {"--lentimes", 13},
    {"--lentypes", 16},
    {"--lensizes", 12}
};

map<string, string> cmdStrArgs = {
    {"--dbpath", "."}
};

enum cmdargtype_t {
    argint, argstr
};

map<string, cmdargtype_t> cmdArgTypes = {
    {"--numruns", argint},
    {"--lentimes", argint},
    {"--lentypes", argint},
    {"--lensizes", argint},
    {"--dbpath", argstr}
};

struct ssbmconf_t {
    size_t NUM_RUNS;
    size_t LEN_TIMES;
    size_t LEN_TYPES;
    size_t LEN_SIZES;
    string DB_PATH;

    ssbmconf_t(size_t numRuns, size_t lenTimes, size_t lenTypes, size_t lenSizes, string& dbPath) : NUM_RUNS(numRuns), LEN_TIMES(lenTimes), LEN_TYPES(lenTypes), LEN_SIZES(lenSizes), DB_PATH(dbPath) {
    }

    ssbmconf_t() : ssbmconf_t(cmdIntArgs["--numruns"], cmdIntArgs["--lentimes"], cmdIntArgs["--lentypes"], cmdIntArgs["--lensizes"], cmdStrArgs["--dbpath"]) {
    }
};

void parseint(const string& name, char* arg) {
    string str(arg);
    size_t idx = string::npos;
    size_t value;
    try {
        value = stoul(str, &idx);
    } catch (invalid_argument& exc) {
        stringstream ss;
        ss << "Value for parameter \"" << name << "\" is not an integer (is \"" << str << "\")!";
        throw runtime_error(ss.str());
    }
    if (idx < str.length()) {
        stringstream ss;
        ss << "Value for parameter \"" << name << "\" is not an integer (is \"" << str << "\")!";
        throw runtime_error(ss.str());
    }
    cmdIntArgs[name] = value;
}

void parsestr(const string& name, char* arg) {
    cmdStrArgs[name] = arg;
}

void parsearg(const string& name, cmdargtype_t argtype, char* arg) {
    switch (argtype) {
        case argint:
            parseint(name, arg);
            break;

        case argstr:
            parsestr(name, arg);
            break;
    }
}

ssbmconf_t initSSBM(int argc, char** argv) {
    if (argc > 1) {
        // for now only long parameters with 2 components ("--name value")
        for (int nArg = 1; nArg < argc; nArg += 2) {
            bool recognized = false;
            for (auto p : cmdArgTypes) {
                if (p.first.compare(argv[nArg]) == 0) {
                    recognized = true;
                    if ((nArg + 1) >= argc) {
                        stringstream ss;
                        ss << "Required value for parameter \"" << argv[nArg] << "\" missing!";
                        throw runtime_error(ss.str());
                    }
                    parsearg(p.first, p.second, argv[nArg + 1]);
                    break;
                }
            }
            if (!recognized) {
                stringstream ss;
                ss << "Parameter \"" << argv[nArg] << "\" is unknown!";
                throw runtime_error(ss.str());
            }
        }
    }
    return ssbmconf_t();
}

#endif /* SSBM_HPP */

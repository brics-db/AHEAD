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
#include <unordered_map>
#include <sstream>

#include <boost/filesystem.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

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

///////////////////////////////
// CMDLINE ARGUMENT PARSING  //
///////////////////////////////

class ArgumentParser {
public:
    typedef std::vector<string> alias_list_t;
    typedef std::vector<std::tuple<string, alias_list_t, size_t>> uint_args_t;
    typedef std::vector<std::tuple<string, alias_list_t, string>> str_args_t;
    typedef std::vector<std::tuple<string, alias_list_t, bool>> bool_args_t;

private:

    enum argtype_t {
        argint, argstr, argbool
    };

    uint_args_t uintArgs;
    str_args_t strArgs;
    bool_args_t boolArgs;
    unordered_map<string, argtype_t> argTypes; // we know what we do

public:

    ArgumentParser() : uintArgs(), strArgs(), boolArgs(), argTypes() {
#ifdef DEBUG
        std::cout << "ArgumentParser()" << std::endl;
#endif
    }

    ArgumentParser(const uint_args_t & uintArgs, const str_args_t & strArgs, const bool_args_t & boolArgs) : uintArgs(uintArgs), strArgs(strArgs), boolArgs(boolArgs), argTypes(uintArgs.size() + strArgs.size() + boolArgs.size()) {
#ifdef DEBUG
        std::cout << "ArgumentParser(const &, const &, const &)" << std::endl;
#endif
        for (auto a : this->uintArgs) {
            for (auto s : get<1>(a)) {
                argTypes[s] = argint;
            }
        }
        for (auto a : this->strArgs) {
            for (auto s : get<1>(a)) {
                argTypes[s] = argstr;
            }
        }
        for (auto a : this->boolArgs) {
            for (auto s : get<1>(a)) {
                argTypes[s] = argbool;
                if (boost::starts_with(s, "--")) {
                    argTypes["--no-" + s.substr(2)] = argbool;
                } else if (boost::starts_with(s, "-")) {
                    argTypes["-no-" + s.substr(1)] = argbool;
                }
            }
        }
    }

    ArgumentParser(const uint_args_t && uintArgs, const str_args_t && strArgs, const bool_args_t && boolArgs) : uintArgs(uintArgs), strArgs(strArgs), boolArgs(boolArgs), argTypes(uintArgs.size() + strArgs.size() + boolArgs.size()) {
#ifdef DEBUG
        std::cout << "ArgumentParser(const &&, const &&, const &&)" << std::endl;
#endif
        for (auto a : this->uintArgs) {
            for (auto s : get<1>(a)) {
                argTypes[s] = argint;
            }
        }
        for (auto a : this->strArgs) {
            for (auto s : get<1>(a)) {
                argTypes[s] = argstr;
            }
        }
        for (auto a : this->boolArgs) {
            for (auto s : get<1>(a)) {
                argTypes[s] = argbool;
                if (boost::starts_with(s, "--")) {
                    argTypes["--no-" + s.substr(2)] = argbool;
                } else if (boost::starts_with(s, "-")) {
                    argTypes["-no-" + s.substr(1)] = argbool;
                }
            }
        }
    }

    virtual ~ArgumentParser() {
    }

    ArgumentParser& operator=(const ArgumentParser & other) {
#ifdef DEBUG
        std::cout << "ArgumentParser& operator=(const ArgumentParser & other)" << std::endl;
#endif
        uintArgs.clear();
        strArgs.clear();
        boolArgs.clear();
        argTypes.clear();
        uintArgs.insert(uintArgs.begin(), other.uintArgs.begin(), other.uintArgs.end());
        strArgs.insert(strArgs.begin(), other.strArgs.begin(), other.strArgs.end());
        boolArgs.insert(boolArgs.begin(), other.boolArgs.begin(), other.boolArgs.end());
        argTypes.insert(other.argTypes.begin(), other.argTypes.end());
        return *this;
    }

private:

    size_t parseint(const string& name, char* arg) {
        if (arg == nullptr) {
            stringstream ss;
            ss << "Required value for parameter \"" << name << "\" missing! (on line " << __LINE__ << ')';
            throw runtime_error(ss.str());
        }
        string str(arg);
        size_t idx = string::npos;
        size_t value;
        try {
            value = stoul(str, &idx);
        } catch (invalid_argument& exc) {
            stringstream ss;
            ss << "Value for parameter \"" << name << "\" is not an integer (is \"" << str << "\")! (on line " << __LINE__ << ')';
            throw runtime_error(ss.str());
        }
        if (idx < str.length()) {
            stringstream ss;
            ss << "Value for parameter \"" << name << "\" is not an integer (is \"" << str << "\")! (on line " << __LINE__ << ')';
            throw runtime_error(ss.str());
        }
        for (auto & tup : uintArgs) {
            for (auto & alias : get<1>(tup)) {
                if (name.compare(alias) == 0) {
                    get<2>(tup) = value;
                    return 1;
                }
            }
        }
        return 1;
    }

    size_t parsestr(const string& name, char* arg) {
        if (arg == nullptr) {
            stringstream ss;
            ss << "Required value for parameter \"" << name << "\" missing! (on line " << __LINE__ << ')';
            throw runtime_error(ss.str());
        }
        for (auto & tup : strArgs) {
            for (auto & alias : get<1>(tup)) {
                if (name.compare(alias) == 0) {
                    get<2>(tup) = arg;
                    return 1;
                }
            }
        }
        return 1;
    }

    size_t parsebool(const string& name, __attribute__((unused)) char* arg) {
        size_t start = 0;
        if (boost::starts_with(name, "no-")) {
            start = 3;
        }
        for (auto & tup : boolArgs) {
            for (auto & alias : get<1>(tup)) {
                if (name.compare(start, string::npos, alias) == 0) {
                    get<2>(tup) = (start == 0); // "no-..." -> false
                    return 0;
                }
            }
        }
        return 0;
    }

public:

    void parse(int argc, char** argv, size_t offset) { // no C++17 (array_view), yet :-(
#ifdef DEBUG
        std::cout << "parse(int argc, char** argv, size_t offset)" << std::endl;
#endif
        if (argc > 1) {
            for (int nArg = offset; nArg < argc; ++nArg) { // always advance at least one step
                bool recognized = false;
                for (auto & p : argTypes) {
                    if (p.first.compare(argv[nArg]) == 0) {
                        recognized = true;
                        char* arg = (nArg + 1) < argc ? argv[nArg + 1] : nullptr;
                        switch (p.second) {
                            case argint:
                                nArg += parseint(p.first, arg);
                                break;

                            case argstr:
                                nArg += parsestr(p.first, arg);
                                break;

                            case argbool:
                                nArg += parsebool(p.first, arg);
                                break;
                        }
                        break;
                    }
                }
                if (!recognized) {
                    stringstream ss;
                    ss << "Parameter \"" << argv[nArg] << "\" is unknown! (on line " << __LINE__ << ')';
                    throw runtime_error(ss.str());
                }
            }
        }
    }

    size_t get_uint(const string & name) {
        for (auto & tup : uintArgs) {
            if (get<0>(tup).compare(name) == 0) {
                return get<2>(tup);
            }
        }
        stringstream ss;
        ss << "UINT parameter \"" << name << "\" is unknown! (on line " << __LINE__ << ')';
        throw runtime_error(ss.str());
    }

    const string & get_str(const string & name) {
        for (auto & tup : strArgs) {
            if (get<0>(tup).compare(name) == 0) {
                return std::get<2>(tup);
            }
        }
        stringstream ss;
        ss << "String parameter \"" << name << "\" is unknown! (on line " << __LINE__ << ')';
        throw runtime_error(ss.str());
    }

    bool get_bool(const string & name) {
        for (auto & tup : boolArgs) {
            if (get<0>(tup).compare(name) == 0) {
                return std::get<2>(tup);
            }
        }
        stringstream ss;
        ss << "Boolean parameter \"" << name << "\" is unknown! (on line " << __LINE__ << ')';
        throw runtime_error(ss.str());
    }
};

struct ssbmconf_t {
    typedef typename ArgumentParser::alias_list_t alias_list_t;

    size_t NUM_RUNS;
    size_t LEN_TIMES;
    size_t LEN_TYPES;
    size_t LEN_SIZES;
    string DB_PATH;
    bool VERBOSE;

private:
    ArgumentParser parser;

public:

    ssbmconf_t() : NUM_RUNS(0), LEN_TIMES(0), LEN_TYPES(0), LEN_SIZES(0), DB_PATH(), VERBOSE(false), parser({
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

        std::forward_as_tuple("verbose", alias_list_t{"--verbose", "-v"}, true)
    }
    ) {
#ifdef DEBUG
        std::cout << "ssbmconf_t()" << std::endl;
#endif
    }

    ssbmconf_t(int argc, char** argv) : ssbmconf_t() {
#ifdef DEBUG
        std::cout << "ssbmconf_t(int argc, char** argv)" << std::endl;
#endif
        init(argc, argv);
    }

    void init(int argc, char** argv) {
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
    }
};

StopWatch::rep loadTable(string& baseDir, const char* const columnName, const ssbmconf_t & CONFIG) {
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

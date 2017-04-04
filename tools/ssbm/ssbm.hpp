// Copyright (c) 2016-2017 Till Kolditz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
#include <fstream>
#include <limits>
#include <stdexcept>

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#pragma GCC diagnostic ignored "-Weffc++"
#endif
#include <cpucounters.h>
#include <utils.h>
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#include <util/argumentparser.hpp>
#include <util/rss.hpp>
#include <util/stopwatch.hpp>

#include <ColumnStore.h>
#include <column_storage/ColumnBat.h>
#include <column_storage/TransactionManager.h>
#include <column_operators/Operators.hpp>

using namespace ahead;

// define
// boost::throw_exception(std::runtime_error("Type name demangling failed"));
namespace boost {

    void throw_exception(std::exception const & e) { // user defined
        throw e;
    }
}

template<typename Head, typename Tail>
void printBat(BAT<Head, Tail> *bat, const char* filename, const char* message = nullptr) {
    std::ofstream fout(filename);
    typedef typename Head::type_t head_t;
    typedef typename Tail::type_t tail_t;
    fout << "size:" << bat->size();
    if (message) {
        fout << " message:\"" << message << "\"\n";
    } else {
        fout << '\n';
    }
    oid_t i = 0;
    constexpr const size_t wOID = numeric_limits<oid_t>::digits10;
    constexpr const bool isHeadLT16 = larger_type<head_t, uint16_t>::isSecondLarger;
    constexpr const size_t wHead = isHeadLT16 ? numeric_limits<uint16_t>::digits10 : numeric_limits<head_t>::digits10;
    constexpr const bool isTailLT16 = larger_type<tail_t, uint16_t>::isSecondLarger;
    constexpr const size_t wTail = isTailLT16 ? numeric_limits<uint16_t>::digits10 : numeric_limits<tail_t>::digits10;
    auto iter = bat->begin();
    fout << std::right;
    fout << std::setw(wOID) << "void";
    fout << " | " << std::setw(wHead) << "head";
    fout << " | " << std::setw(wTail) << "tail";
    fout << '\n';
    for (; iter->hasNext(); ++i, ++*iter) {
        fout << std::setw(wOID) << i;
        if (isHeadLT16) {
            fout << " | " << std::setw(wHead) << static_cast<uint16_t>(iter->head());
        } else {
            fout << " | " << std::setw(wHead) << iter->head();
        }
        if (isTailLT16) {
            fout << " | " << std::setw(wTail) << static_cast<uint16_t>(iter->tail());
        } else {
            fout << " | " << std::setw(wTail) << iter->tail();
        }
        fout << '\n';
    }
    fout << std::flush;
    fout.close();
    delete iter;
}

#define PRINT_BAT(PRINTCMD)                                                    \
;do {                                                                          \
    StopWatch swp;                                                             \
    swp.stop();                                                                \
    PRINTCMD;                                                                  \
    swp.resume();                                                              \
} while (false)

/////////////////////////////
// SSBM_REQUIRED_VARIABLES //
/////////////////////////////
#define RUNTABLE_REQUIRED_VARIABLES(OpsNum)                                    \
const size_t NUM_OPS = OpsNum;                                                 \
size_t I = 0;                                                                  \
StopWatch sw1, sw2;                                                            \
StopWatch::rep opTimes[NUM_OPS] = {0};                                         \
size_t batSizes[NUM_OPS] = {0};                                                \
size_t batConsumptions[NUM_OPS] = {0};                                         \
size_t batConsumptionsProj[NUM_OPS] = {0};                                     \
size_t batRSS[NUM_OPS] = {0};                                                  \
bool hasTwoTypes[NUM_OPS] = {false};                                           \
boost::typeindex::type_index headTypes[NUM_OPS];                               \
boost::typeindex::type_index tailTypes[NUM_OPS];                               \
std::string emptyString;                                                       \
;do {                                                                          \
} while (false)

#define SSBM_REQUIRED_VARIABLES(Headline, OpsNum, ...)                         \
ssbmconf_t CONFIG(argc, argv);                                                 \
std::vector<StopWatch::rep> totalTimes(CONFIG.NUM_RUNS);                       \
RUNTABLE_REQUIRED_VARIABLES(OpsNum);                                           \
cstr_t OP_NAMES[NUM_OPS] = {__VA_ARGS__};                                      \
size_t rssBeforeLoad, rssAfterLoad, rssAfterCopy, rssAfterQueries;             \
std::vector<CoreCounterState> cstate1, cstate2, cstate3;                       \
std::vector<SocketCounterState> sktstate1, sktstate2, sktstate3;               \
SystemCounterState sysstate1, sysstate2, sysstate3;                            \
set_signal_handlers();                                                         \
PCM * m = nullptr;                                                             \
PCM::ErrorCode pcmStatus = PCM::UnknownError;                                  \
;do {                                                                          \
    m = PCM::getInstance();                                                    \
    pcmStatus = m->program();                                                  \
    if (pcmStatus == PCM::PMUBusy) {                                           \
        m->resetPMU();                                                         \
        pcmStatus = m->program();                                              \
    }                                                                          \
    if (pcmStatus != PCM::Success) {                                           \
        std::cerr << "Intel's PCM couldn't start" << '\n';                     \
        std::cerr << "\tError code: " << pcmStatus << '\n';                    \
    }                                                                          \
    if (pcmStatus == PCM::Success) {                                           \
        m->getAllCounterStates(sysstate1, sktstate1, cstate1);                 \
    }                                                                          \
    std::cout << Headline << std::endl;                                        \
    MetaRepositoryManager::init(CONFIG.DB_PATH.c_str());                       \
} while (false)

///////////////
// SSBM_LOAD //
///////////////
#define SSBM_LOAD(...) VFUNC(SSBM_LOAD, __VA_ARGS__)
#define SSBM_LOAD6(tab1, tab2, tab3, tab4, tab5, QueryString)                  \
;do {                                                                          \
    rssBeforeLoad = getPeakRSS(size_enum_t::B);                                \
    sw1.start();                                                               \
    loadTable(CONFIG.DB_PATH, tab1, CONFIG);                                   \
    loadTable(CONFIG.DB_PATH, tab2, CONFIG);                                   \
    loadTable(CONFIG.DB_PATH, tab3, CONFIG);                                   \
    loadTable(CONFIG.DB_PATH, tab4, CONFIG);                                   \
    loadTable(CONFIG.DB_PATH, tab5, CONFIG);                                   \
    sw1.stop();                                                                \
    rssAfterLoad = getPeakRSS(size_enum_t::B);                                 \
    std::cout << "Total loading time: " << sw1 << " ns.\n" << std::endl;       \
    if (CONFIG.VERBOSE) {                                                      \
        std::cout << QueryString << std::endl;                                 \
    }                                                                          \
} while (false)
#define SSBM_LOAD5(tab1, tab2, tab3, tab4, QueryString)                        \
;do {                                                                          \
    rssBeforeLoad = getPeakRSS(size_enum_t::B);                                \
    sw1.start();                                                               \
    loadTable(CONFIG.DB_PATH, tab1, CONFIG);                                   \
    loadTable(CONFIG.DB_PATH, tab2, CONFIG);                                   \
    loadTable(CONFIG.DB_PATH, tab3, CONFIG);                                   \
    loadTable(CONFIG.DB_PATH, tab4, CONFIG);                                   \
    sw1.stop();                                                                \
    rssAfterLoad = getPeakRSS(size_enum_t::B);                                 \
    std::cout << "Total loading time: " << sw1 << " ns.\n" << std::endl;       \
    if (CONFIG.VERBOSE) {                                                      \
        std::cout << QueryString << std::endl;                                 \
    }                                                                          \
} while (false)
#define SSBM_LOAD3(tab1, tab2, QueryString)                                    \
;do {                                                                          \
    rssBeforeLoad = getPeakRSS(size_enum_t::B);                                \
    sw1.start();                                                               \
    loadTable(CONFIG.DB_PATH, tab1, CONFIG);                                   \
    loadTable(CONFIG.DB_PATH, tab2, CONFIG);                                   \
    sw1.stop();                                                                \
    rssAfterLoad = getPeakRSS(size_enum_t::B);                                 \
    std::cout << "Total loading time: " << sw1 << " ns.\n" << std::endl;       \
    if (CONFIG.VERBOSE) {                                                      \
        std::cout << QueryString << std::endl;                                 \
    }                                                                          \
} while (false)

/////////////////////////
// SSBM_BEFORE_QUERIES //
/////////////////////////
#define SSBM_BEFORE_QUERIES                                                    \
;do {                                                                          \
    rssAfterCopy = getPeakRSS(size_enum_t::B);                                 \
    if (CONFIG.VERBOSE) {                                                      \
        std::cout << "\n(Copying)\n";                                          \
        COUT_HEADLINE;                                                         \
        COUT_RESULT(0, I);                                                     \
        std::cout << std::endl;                                                \
    }                                                                          \
    if (pcmStatus == PCM::Success) {                                           \
        m->getAllCounterStates(sysstate2, sktstate2, cstate2);                 \
    }                                                                          \
} while (false)

///////////////////////
// SSBM_BEFORE_QUERY //
///////////////////////
#define SSBM_BEFORE_QUERY                                                      \
;do {                                                                          \
    sw2.start();                                                               \
    I = 0;                                                                     \
} while (false)

//////////////////////
// SSBM_AFTER_QUERY //
//////////////////////
#define SSBM_AFTER_QUERY(index, result)                                        \
;do {                                                                          \
    totalTimes[index] = sw2.stop();                                            \
    std::cout << "(" << std::setw(2) << index << ")\n\tresult: " << result << "\n\t  time: " << sw2 << " ns.\n"; \
    COUT_HEADLINE;                                                             \
    COUT_RESULT(0, I, OP_NAMES);                                               \
} while (false)

////////////////////////
// SSBM_AFTER_QUERIES //
////////////////////////
#define SSBM_AFTER_QUERIES                                                     \
;do {                                                                          \
    rssAfterQueries = getPeakRSS(size_enum_t::B);                              \
    if (CONFIG.VERBOSE) {                                                      \
        std::cout << "Memory statistics (Resident Set size in B):\n\t" << std::setw(15) << "before load: " << std::setw(CONFIG.LEN_SIZES) << rssBeforeLoad << "\n\t" << std::setw(15) << "after load: " << std::setw(CONFIG.LEN_SIZES) << rssAfterLoad << "\n\t" << std::setw(15) << "after copy: " << std::setw(CONFIG.LEN_SIZES) << rssAfterCopy << "\n\t" << std::setw(15) << "after queries: " << std::setw(CONFIG.LEN_SIZES) << rssAfterQueries << "\n"; \
    }                                                                          \
    std::cout << "TotalTimes:\n";                                              \
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {                             \
        std::cout << std::setw(2) << i << '\t' << totalTimes[i] << '\n';       \
    }                                                                          \
    std::cout << "Memory:\n" << rssBeforeLoad << '\n' << rssAfterLoad << '\n' << rssAfterCopy << '\n' << rssAfterQueries << std::endl; \
} while (false)

///////////////
// PCM_PRINT //
///////////////
#define PCM_PRINT(attr, proc, state1, state2, state3)                          \
;do {                                                                          \
    std::cout << std::setw(CONFIG.LEN_PCM) << attr << '\t';                    \
    std::cout << std::setw(CONFIG.LEN_PCM) << proc(state1, state2) << '\t';    \
    std::cout << std::setw(CONFIG.LEN_PCM) << proc(state2, state3) << '\n';    \
} while (false)

///////////////////
// SSBM_FINALIZE //
///////////////////
#define SSBM_FINALIZE                                                                  \
;do {                                                                                  \
    TransactionManager::destroyInstance();                                             \
    if (pcmStatus == PCM::Success) {                                                   \
        m->getAllCounterStates(sysstate3, sktstate3, cstate3);                         \
        std::cout << "PCM:\n";                                                         \
        std::cout << std::setw(CONFIG.LEN_PCM) << "Attribute" << '\t';                 \
        std::cout << std::setw(CONFIG.LEN_PCM) << "init" << '\t';                      \
        std::cout << std::setw(CONFIG.LEN_PCM) << "query" << '\n';                     \
        PCM_PRINT("IPC", getIPC, sysstate1, sysstate2, sysstate3);                     \
        PCM_PRINT("MC Read", getBytesReadFromMC, sysstate1, sysstate2, sysstate3);     \
        PCM_PRINT("MC written", getBytesWrittenToMC, sysstate1, sysstate2, sysstate3); \
        PCM_PRINT("Energy", getConsumedEnergy, sysstate1, sysstate2, sysstate3);       \
        PCM_PRINT("Joules", getConsumedJoules, sysstate1, sysstate2, sysstate3);       \
        PCM_PRINT("L2-Miss Cycle Loss", getCyclesLostDueL2CacheMisses, sysstate1, sysstate2, sysstate3);       \
        PCM_PRINT("L3-Miss Cycle Loss", getCyclesLostDueL3CacheMisses, sysstate1, sysstate2, sysstate3);       \
    }                                                                                  \
} while (false)

///////////////
// SAVE_TYPE //
///////////////
#define SAVE_TYPE(BAT)                                                         \
;do {                                                                          \
    headTypes[I - 1] = BAT->type_head();                                       \
    tailTypes[I - 1] = BAT->type_tail();                                       \
    hasTwoTypes[I - 1] = true;                                                 \
} while (false)

////////////////
// MEASURE_OP //
////////////////
#define MEASURE_OP(...) VFUNC(MEASURE_OP, __VA_ARGS__)

#define MEASURE_OP6(TYPE, VAR, OP, STORE_SIZE_OP, STORE_CONSUMPTION_OP, STORE_PROJECTEDCONSUMPTION_OP) \
sw1.start();                                                                   \
TYPE VAR = OP;                                                                 \
;do {                                                                          \
    opTimes[I] = sw1.stop();                                                   \
    batSizes[I] = STORE_SIZE_OP;                                               \
    batConsumptions[I] = STORE_CONSUMPTION_OP;                                 \
    batConsumptionsProj[I] = STORE_PROJECTEDCONSUMPTION_OP;                    \
    batRSS[I] = getPeakRSS(size_enum_t::B);                                    \
    ++I;                                                                       \
} while (false)

#define MEASURE_OP3(TYPE, VAR, OP)                                             \
MEASURE_OP6(TYPE, VAR, OP, 1, sizeof(TYPE), sizeof(TYPE));                     \
;do {                                                                          \
    headTypes[I-1] = boost::typeindex::type_id<TYPE>().type_info();            \
    hasTwoTypes[I-1] = false;                                                  \
} while (false)

#define MEASURE_OP2(BAT, OP)                                                               \
MEASURE_OP6(auto, BAT, OP, BAT->size(), BAT->consumption(), BAT->consumptionProjected());  \
SAVE_TYPE(BAT)

#define DEBUG_PRINT_OP_NAME(OP)                                                \
;do {                                                                          \
    if (CONFIG.VERBOSE) {                                                      \
        std::cout << "[op" << std::setw(2) << OP_NAMES[I - 1] << "] " << #OP << "\n"; \
    }                                                                          \
} while (false)

#define DEBUG_PRINT_VECBOOL_PAIR(PAIR)                                         \
;do {                                                                          \
    if (CONFIG.VERBOSE) {                                                      \
        std::cout << "\tpair<1>: " << std::get<1>(PAIR)->capacity() << " / " << sizeof(std::vector<bool>::size_type) << " = " << (std::get<1>(PAIR)->capacity() / sizeof(std::vector<bool>::size_type)) << " + " << sizeof(*std::get<1>(PAIR)) << " = " << (sizeof(*std::get<1>(PAIR)) + std::get<1>(PAIR)->capacity() / sizeof(std::vector<bool>::size_type)) << std::endl; \
    }                                                                          \
} while (false)

#define DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, idx) \
do { \
    if (CONFIG.VERBOSE) { \
        std::cout << "\ttuple<" << idx << ">: " << std::get<idx>(TUPLE)->capacity() << " / " << sizeof(std::vector<bool>::size_type) << " = " << (std::get<idx>(TUPLE)->capacity() / sizeof(std::vector<bool>::size_type)) << " + " << sizeof(*std::get<idx>(TUPLE)) << " = " << (sizeof(*std::get<idx>(TUPLE)) + std::get<idx>(TUPLE)->capacity() / sizeof(std::vector<bool>::size_type)) << std::endl; \
    } \
} while (false)

#define MEASURE_OP_PAIR(PAIR, OP)                                              \
MEASURE_OP6(auto, PAIR, OP, PAIR.first->size(), PAIR.first->consumption(),     \
    PAIR.first->consumptionProjected());                                       \
SAVE_TYPE(PAIR.first);                                                         \
;do {                                                                          \
    if (std::get<1>(PAIR)) {                                                   \
        DEBUG_PRINT_OP_NAME(OP);                                               \
        DEBUG_PRINT_VECBOOL_PAIR(PAIR);                                        \
    }                                                                          \
} while (false)

#define MEASURE_OP_TUPLE(TUPLE, OP)                                            \
MEASURE_OP6(auto, TUPLE, OP, std::get<0>(TUPLE)->size(), std::get<0>(TUPLE)->consumption(), std::get<0>(TUPLE)->consumptionProjected());  \
SAVE_TYPE((std::get<0>(TUPLE)))

///////////////////
// COUT_HEADLINE //
///////////////////
#define COUT_HEADLINE                                                          \
;do {                                                                          \
    std::cout << "\tname\t" << std::setw(CONFIG.LEN_TIMES) << "time [ns]" << "\t" << std::setw(CONFIG.LEN_SIZES) << "size [#]" << "\t" << std::setw(CONFIG.LEN_SIZES) << "consum [B]" << "\t" << std::setw(CONFIG.LEN_SIZES) << "proj [B]" << "\t " << std::setw(CONFIG.LEN_SIZES) << " RSS Δ [B]" << "\t" << std::setw(CONFIG.LEN_TYPES) << "type head" << "\t" << std::setw(CONFIG.LEN_TYPES) << "type tail" << "\n"; \
} while (false)

/////////////////
// COUT_RESULT //
/////////////////
#define COUT_RESULT(...) VFUNC(COUT_RESULT, __VA_ARGS__)

#define COUT_RESULT3(START, MAX, OPNAMES)                                      \
;do {                                                                          \
    for (size_t k = START; k < MAX; ++k) {                                     \
        std::cout << "\top" << std::setw(2) << OPNAMES[k] << "\t" << std::setw(CONFIG.LEN_TIMES) << hrc_duration(opTimes[k]) << "\t" << std::setw(CONFIG.LEN_SIZES) << batSizes[k] << "\t" << std::setw(CONFIG.LEN_SIZES) << batConsumptions[k] << "\t" << std::setw(CONFIG.LEN_SIZES) << batConsumptionsProj[k] << "\t" << std::setw(CONFIG.LEN_SIZES) << (k == 0 ? (batRSS[k] - rssAfterCopy) : (batRSS[k] - batRSS[k - 1])) << "\t" << std::setw(CONFIG.LEN_TYPES) << headTypes[k].pretty_name() << "\t" << std::setw(CONFIG.LEN_TYPES) << (hasTwoTypes[k] ? tailTypes[k].pretty_name() : emptyString) << '\n'; \
    }                                                                          \
    std::cout << std::flush;                                                   \
} while (false)

#define COUT_RESULT2(START, MAX)                                               \
;do {                                                                          \
    for (size_t k = START; k < MAX; ++k) {                                     \
        std::cout << "\top" << std::setw(2) << k << "\t" << std::setw(CONFIG.LEN_TIMES) << hrc_duration(opTimes[k]) << "\t" << std::setw(CONFIG.LEN_SIZES) << batSizes[k] << "\t" << std::setw(CONFIG.LEN_SIZES) << batConsumptions[k] << "\t" << std::setw(CONFIG.LEN_SIZES) << batConsumptionsProj[k] << "\t" << std::setw(CONFIG.LEN_SIZES) << (k == 0 ? (batRSS[k] - rssAfterLoad) : (batRSS[k] - batRSS[k - 1])) << "\t" << std::setw(CONFIG.LEN_TYPES) << headTypes[k].pretty_name() << "\t" << std::setw(CONFIG.LEN_TYPES) << (hasTwoTypes[k] ? tailTypes[k].pretty_name() : emptyString) << '\n'; \
    }                                                                          \
    std::cout << std::flush;                                                   \
} while (false)

/////////////////////
// CLEAR_SELECT_AN //
/////////////////////
#define CLEAR_SELECT_AN(PAIR)                                                  \
;do {                                                                          \
    if (std::get<1>(PAIR)) {                                                   \
        DEBUG_PRINT_OP_NAME(<unknown>);                                        \
        DEBUG_PRINT_VECBOOL_PAIR(PAIR);                                        \
        delete std::get<1>(PAIR);                                              \
    }                                                                          \
} while(false)

///////////////////////
// CLEAR_HASHJOIN_AN //
///////////////////////
#define CLEAR_HASHJOIN_AN(TUPLE)                                               \
;do {                                                                          \
    if (std::get<1>(TUPLE) || std::get<2>(TUPLE) || std::get<3>(TUPLE)         \
        || std::get<4>(TUPLE)) {                                               \
        DEBUG_PRINT_OP_NAME(<unknown>);                                        \
    }                                                                          \
    if (std::get<1>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 1);                                   \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 2);                                   \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 3);                                   \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
    if (std::get<4>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 4);                                   \
        delete std::get<4>(TUPLE);                                             \
    }                                                                          \
} while (false)

/////////////////////////////
// CLEAR_CHECKANDDECODE_AN //
/////////////////////////////
#define CLEAR_CHECKANDDECODE_AN(TUPLE)                                         \
;do {                                                                          \
    if (std::get<1>(TUPLE) || std::get<2>(TUPLE)) {                            \
        DEBUG_PRINT_OP_NAME(<unknown>);                                        \
    }                                                                          \
    if (std::get<1>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 1);                                   \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 2);                                   \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
} while (false)

//////////////////////
// CLEAR_GROUPBY_AN //
//////////////////////
#define CLEAR_GROUPBY_AN(TUPLE)                                                \
;do {                                                                          \
    if (std::get<2>(TUPLE) || std::get<3>(TUPLE))) {                           \
        DEBUG_PRINT_OP_NAME(<unknown>);                                        \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 2);                                   \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 3);                                   \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
} while (false)

/////////////////////////
// CLEAR_GROUPEDSUM_AN //
/////////////////////////
#define CLEAR_GROUPEDSUM_AN(TUPLE)                                             \
;do {                                                                          \
    if (std::get<5>(TUPLE) || std::get<6>(TUPLE) || std::get<7>(TUPLE)         \
        || std::get<8>(TUPLE) || std::get<9>(TUPLE) || std::get<10>(TUPLE)) {  \
        DEBUG_PRINT_OP_NAME(<unknown>);                                        \
    }                                                                          \
    if (std::get<5>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 5);                                   \
        delete std::get<5>(TUPLE);                                             \
    }                                                                          \
    if (std::get<6>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 6);                                   \
        delete std::get<6>(TUPLE);                                             \
    }                                                                          \
    if (std::get<7>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 7);                                   \
        delete std::get<7>(TUPLE);                                             \
    }                                                                          \
    if (std::get<8>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 8);                                   \
        delete std::get<8>(TUPLE);                                             \
    }                                                                          \
    if (std::get<9>(TUPLE)) {                                                  \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 9);                                   \
        delete std::get<9>(TUPLE);                                             \
    }                                                                          \
    if (std::get<10>(TUPLE)) {                                                 \
        DEBUG_PRINT_VECBOOL_TUPLE(TUPLE, 10);                                  \
        delete std::get<10>(TUPLE);                                            \
    }                                                                          \
} while (false)

///////////////////////////////
// CMDLINE ARGUMENT PARSING  //
///////////////////////////////

struct ssbmconf_t {

    typedef typename ArgumentParser::alias_list_t alias_list_t;

    size_t NUM_RUNS;
    size_t LEN_TIMES;
    size_t LEN_TYPES;
    size_t LEN_SIZES;
    size_t LEN_PCM;
    std::string DB_PATH;
    bool VERBOSE;
    bool PRINT_RESULT;

private:
    ArgumentParser parser;

public:

    ssbmconf_t()
            : NUM_RUNS(0), LEN_TIMES(0), LEN_TYPES(0), LEN_SIZES(0), LEN_PCM(0), DB_PATH(), VERBOSE(false), PRINT_RESULT(0),
                    parser(
                            {std::forward_as_tuple("numruns", alias_list_t {"--numruns", "-n"}, 15), std::forward_as_tuple("lentimes", alias_list_t {"--lentimes"}, 16), std::forward_as_tuple(
                                    "lentypes", alias_list_t {"--lentypes"}, 14), std::forward_as_tuple("lensizes", alias_list_t {"--lensizes"}, 16), std::forward_as_tuple("lenpcm", alias_list_t {
                                    "--lenpcm"}, 16)}, {std::forward_as_tuple("dbpath", alias_list_t {"--dbpath", "-d"}, ".")}, {

                            std::forward_as_tuple("verbose", alias_list_t {"--verbose", "-v"}, false), std::forward_as_tuple("printresult", alias_list_t {"--print-result", "-p"}, false)}) {
    }

    ssbmconf_t(int argc, char** argv)
            : ssbmconf_t() {
        init(argc, argv);
    }

    void init(int argc, char** argv) {
        parser.parse(argc, argv, 1);
        NUM_RUNS = parser.get_uint("numruns");
        LEN_TIMES = parser.get_uint("lentimes");
        LEN_TYPES = parser.get_uint("lentypes");
        LEN_SIZES = parser.get_uint("lensizes");
        LEN_PCM = parser.get_uint("lenpcm");
        DB_PATH = parser.get_str("dbpath");
        VERBOSE = parser.get_bool("verbose");
        PRINT_RESULT = parser.get_bool("printresult");
    }
};

StopWatch::rep loadTable(std::string& baseDir, const char* const tableName, const ssbmconf_t & CONFIG) {
    StopWatch sw;
    TransactionManager* tm = TransactionManager::getInstance();
    TransactionManager::Transaction* t = tm->beginTransaction(true);
    assert(t != nullptr);
    std::string path = baseDir + "/" + tableName;
    sw.start();
    size_t num = t->load(path.c_str(), tableName);
    sw.stop();
    if (CONFIG.VERBOSE) {
        std::cout << "File: " << path << "\n\tNumber of BUNs: " << num << "\n\tTime: " << sw << " ns." << std::endl;
    }
    tm->endTransaction(t);
    return sw.duration();
}

template<size_t MODULARITY>
struct ModularRedundancyVoter {

    template<typename T>
    static T vote(T (&values)[MODULARITY]) {
        std::stringstream ss;
        ss << "Unsupported ModularRedundancyVoter<" << MODULARITY << '>';
        throw std::runtime_error(ss.str());
    }
};

template<>
struct ModularRedundancyVoter<2> {

    template<typename T>
    static T vote(T (&values)[2]) {
        return values[0] == values[1] ? values[0] : static_cast<uint64_t>(-1);;
    }
};

template<>
struct ModularRedundancyVoter<3> {

    template<typename T>
    static T vote(T (&values)[3]) {
        return values[0] == values[1] ? values[0] : (values[1] == values[2] ? values[1] : static_cast<uint64_t>(-1));;
    }
};

template<typename T, size_t MODULARITY, size_t i>
struct ModularRedundantValueInitializer {

    static
    void init(T (&values)[MODULARITY], T value) {
        values[MODULARITY - i] = value;
        ModularRedundantValueInitializer<T, MODULARITY, i - 1>::init(values, value);
    }
};

template<typename T, size_t MODULARITY>
struct ModularRedundantValueInitializer<T, MODULARITY, 1> {

    static
    void init(T (&values)[MODULARITY], T value) {
        values[MODULARITY - 1] = value;
    }
};

template<typename T, size_t MODULARITY>
class ModularRedundantValue {

    T values[MODULARITY];

public:

    ModularRedundantValue(T value) {
        ModularRedundantValueInitializer<T, MODULARITY, MODULARITY>::init(this->values, value);
    }

    T operator()() {
        return ModularRedundancyVoter<MODULARITY>::vote(values);
    }

    ModularRedundantValue<T, MODULARITY> & operator=(T value) {
        ModularRedundantValueInitializer<T, MODULARITY, MODULARITY>::init(this->values, value);
        return *this;
    }

    T & operator[](const size_t i) {
        if (i >= MODULARITY) {
            std::stringstream ss;
            ss << "[Exception] [ModularRedundantError:operator[]] given index " << i << " is too large! Must be less than " << MODULARITY;
            throw std::runtime_error(ss.str());
        }
        return values[i];
    }
};

#endif /* SSBM_HPP */


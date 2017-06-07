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
#include <util/ModularRedundant.hpp>
#include <util/rss.hpp>
#include <util/stopwatch.hpp>

#include <AHEAD.hpp>

// define
// boost::throw_exception(std::runtime_error("Type name demangling failed"));
namespace boost {

    void throw_exception(std::exception const & e) { // user defined
        throw e;
    }
}

using namespace ahead;
using namespace ahead::bat::ops;
#ifdef FORCE_SSE
using namespace ahead::bat::ops::sse;
#else
using namespace ahead::bat::ops::scalar;
#endif

///////////////////////////////
// CMDLINE ARGUMENT PARSING  //
///////////////////////////////
namespace ssb {

    struct SSB_CONF {

        typedef typename ArgumentParser::alias_list_t alias_list_t;

        size_t NUM_RUNS;
        size_t LEN_TIMES;
        size_t LEN_TYPES;
        size_t LEN_SIZES;
        size_t LEN_PCM;
        std::string DB_PATH;
        bool VERBOSE;
        bool PRINT_RESULT;
        bool CONVERT_TABLE_FILES;

    private:
        ArgumentParser parser;

        constexpr static const char * const ID_NUMRUNS = "numruns";
        constexpr static const char * const ID_LENTIMES = "lentimes";
        constexpr static const char * const ID_LENTYPES = "lentypes";
        constexpr static const char * const ID_LENSIZES = "lensizes";
        constexpr static const char * const ID_LENPCM = "lenpcm";
        constexpr static const char * const ID_DBPATH = "numruns";
        constexpr static const char * const ID_VERBOSE = "verbose";
        constexpr static const char * const ID_PRINTRESULT = "printresult";
        constexpr static const char * const ID_CONVERTTABLEFILES = "converttablefiles";

    public:

        SSB_CONF();
        SSB_CONF(int argc, char** argv);

        void init(int argc, char** argv);
    };

    StopWatch::rep loadTable(const char* const tableName, const SSB_CONF & CONFIG);

    template<typename Head, typename Tail>
    void printBat(StopWatch & sw, BAT<Head, Tail> *bat, const char* filename, const char* message = nullptr) {
        sw.stop();
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
        constexpr const size_t wOID = std::numeric_limits<oid_t>::digits10;
        constexpr const bool isHeadLT16 = ahead::larger_type<head_t, uint16_t>::isSecondLarger;
        constexpr const size_t wHead = isHeadLT16 ? std::numeric_limits<uint16_t>::digits10 : std::numeric_limits<head_t>::digits10;
        constexpr const bool isTailLT16 = ahead::larger_type<tail_t, uint16_t>::isSecondLarger;
        constexpr const size_t wTail = isTailLT16 ? std::numeric_limits<uint16_t>::digits10 : std::numeric_limits<tail_t>::digits10;
        auto iter = bat->begin();
        fout << std::right;
        fout << std::setw(wOID) << "void";
        fout << " | " << std::setw(wHead) << "head";
        fout << " | " << std::setw(wTail) << "tail";
        fout << '\n';
        for (; iter->hasNext(); ++i, ++*iter) {
            fout << std::setw(wOID) << i;
            fout << " | " << std::setw(wHead) << iter->head();
            fout << " | " << std::setw(wTail) << iter->tail();
            fout << '\n';
        }
        fout << std::flush;
        fout.close();
        delete iter;
        sw.resume();
    }

#define PRINTBAT_TEMPLATES(v2_head_t) \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_tinyint_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_shortint_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_int_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_bigint_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_str_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_restinyint_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_resshortint_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_resint_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_resbigint_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_resstr_t> *bat, const char* filename, const char* message);

    extern SSB_CONF ssb_config;
    extern ::PCM * m;
    extern ::PCM::ErrorCode pcmStatus;
    extern size_t rssBeforeLoad, rssAfterLoad, rssAfterCopy, rssAfterQueries;
    extern std::vector<CoreCounterState> cstate1, cstate2, cstate3;
    extern std::vector<SocketCounterState> sktstate1, sktstate2, sktstate3;
    extern SystemCounterState sysstate1, sysstate2, sysstate3;
    extern std::string emptyString;
    extern std::vector<StopWatch::rep> totalTimes;
    extern size_t I;
    extern StopWatch sw1, sw2;

    extern std::vector<cstr_t> opNames;
    extern std::vector<StopWatch::rep> opTimes;
    extern std::vector<size_t> batSizes;
    extern std::vector<size_t> batConsumptions;
    extern std::vector<size_t> batConsumptionsProj;
    extern std::vector<size_t> batRSS;
    extern std::vector<bool> hasTwoTypes;
    extern std::vector<boost::typeindex::type_index> headTypes;
    extern std::vector<boost::typeindex::type_index> tailTypes;

    void init(int argc, char ** argv, const char * strHeadline, size_t numOps);
    void init_pcm();
    void clear_stats();
    void after_create_columnbats();
    void before_queries();
    void after_queries();
    void before_query();
    void after_query(size_t index, size_t result);
    void before_op();
    void after_op();
    void finalize();

    void print_headline();
    void print_result();
    void print_result(std::vector<cstr_t> const & opNames);

}

/////////////////////////////
// SSBM_REQUIRED_VARIABLES //
/////////////////////////////
#define SSBM_REQUIRED_VARIABLES(Headline, OpsNum, ...) \
ssb::opNames = {__VA_ARGS__}; \
ssb::init(argc, argv, Headline, OpsNum)

///////////////
// SSBM_LOAD //
///////////////
#define SSBM_LOAD_PRINTQUERYSTRING(...) VFUNC(SSBM_LOAD_PRINTQUERYSTRING, __VA_ARGS__)
#define SSBM_LOAD_PRINTQUERYSTRING6(tab1, tab2, tab3, tab4, tab5, QueryString) \
std::cout << QueryString << std::endl;
#define SSBM_LOAD_PRINTQUERYSTRING5(tab1, tab2, tab3, tab4, QueryString) \
std::cout << QueryString << std::endl;
#define SSBM_LOAD_PRINTQUERYSTRING4(tab1, tab2, tab3,  QueryString) \
std::cout << QueryString << std::endl;
#define SSBM_LOAD_PRINTQUERYSTRING3(tab1, tab2, QueryString) \
std::cout << QueryString << std::endl;
#define SSBM_LOAD_PRINTQUERYSTRING2(tab1, QueryString) \
std::cout << QueryString << std::endl;
#define SSBM_LOAD(...)                                                         \
;do {                                                                          \
    ssb::rssBeforeLoad = getPeakRSS(size_enum_t::B);                           \
    ssb::sw1.start();                                                          \
    VFUNC(SSBM_LOAD, __VA_ARGS__)                                              \
    ssb::sw1.stop();                                                           \
    ssb::rssAfterLoad = getPeakRSS(size_enum_t::B);                            \
    std::cout << "Total loading time: " << ssb::sw1 << " ns.\n" << std::endl;       \
    if (ssb::ssb_config.VERBOSE) {                                             \
        SSBM_LOAD_PRINTQUERYSTRING(__VA_ARGS__);                               \
    }                                                                          \
} while (false)
#define SSBM_LOAD6(tab1, tab2, tab3, tab4, tab5, QueryString)                  \
loadTable(tab1, ssb::ssb_config);                                                       \
loadTable(tab2, ssb::ssb_config);                                                       \
loadTable(tab3, ssb::ssb_config);                                                       \
loadTable(tab4, ssb::ssb_config);                                                       \
loadTable(tab5, ssb::ssb_config);
#define SSBM_LOAD5(tab1, tab2, tab3, tab4, QueryString)                        \
loadTable(tab1, ssb::ssb_config);                                                       \
loadTable(tab2, ssb::ssb_config);                                                       \
loadTable(tab3, ssb::ssb_config);                                                       \
loadTable(tab4, ssb::ssb_config);
#define SSBM_LOAD3(tab1, tab2, QueryString)                                    \
loadTable(tab1, ssb::ssb_config);                                                       \
loadTable(tab2, ssb::ssb_config);
#define SSBM_LOAD2(tab1, QueryString)                                          \
loadTable(tab1, ssb::ssb_config);

///////////////
// SAVE_TYPE //
///////////////
#define SAVE_TYPE(BAT)                                                         \
;do {                                                                          \
	ssb::headTypes.push_back(BAT->type_head());                                \
	ssb::tailTypes.push_back(BAT->type_tail());                                \
	ssb::hasTwoTypes.push_back(true);                                          \
} while (false)

////////////////
// MEASURE_OP //
////////////////
#define MEASURE_OP(...) VFUNC(MEASURE_OP, __VA_ARGS__)

#define MEASURE_OP4(VAR, IDX, OP, TYPE) \
MEASURE_OP6(VAR, IDX, OP, VAR IDX ->size(), VAR IDX ->consumption(), VAR IDX ->consumptionProjected()); \
do { \
    SAVE_TYPE(TYPE); \
} while (false)

#define MEASURE_OP6(TYPE, VAR, OP, STORE_SIZE_OP, STORE_CONSUMPTION_OP, STORE_PROJECTEDCONSUMPTION_OP) \
ssb::before_op();                                                              \
TYPE VAR = OP;                                                                 \
ssb::after_op();                                                               \
;do {                                                                          \
    ssb::batSizes.push_back(STORE_SIZE_OP);                                    \
    ssb::batConsumptions.push_back(STORE_CONSUMPTION_OP);                      \
    ssb::batConsumptionsProj.push_back(STORE_PROJECTEDCONSUMPTION_OP);         \
} while (false)

#define MEASURE_OP3(TYPE, VAR, OP)                                             \
MEASURE_OP6(TYPE, VAR, OP, 1, sizeof(TYPE), sizeof(TYPE));                     \
;do {                                                                          \
	ssb::headTypes.push_back(boost::typeindex::type_id<TYPE>().type_info());   \
	ssb::hasTwoTypes.push_back(false);                                         \
} while (false)

#define MEASURE_OP2(BAT, OP)                                                               \
MEASURE_OP6(auto, BAT, OP, BAT->size(), BAT->consumption(), BAT->consumptionProjected());  \
SAVE_TYPE(BAT)

#define MEASURE_OP_PAIR(PAIR, OP)                                              \
MEASURE_OP6(auto, PAIR, OP, PAIR.first->size(), PAIR.first->consumption(),     \
    PAIR.first->consumptionProjected());                                       \
SAVE_TYPE(PAIR.first);

#define MEASURE_OP_TUPLE(TUPLE, OP)                                            \
MEASURE_OP6(auto, TUPLE, OP, std::get<0>(TUPLE)->size(), std::get<0>(TUPLE)->consumption(), std::get<0>(TUPLE)->consumptionProjected());  \
SAVE_TYPE((std::get<0>(TUPLE)))

/////////////////////
// CLEAR_SELECT_AN //
/////////////////////
#define CLEAR_SELECT_AN(PAIR)                                                  \
;do {                                                                          \
    if (std::get<1>(PAIR)) {                                                   \
        delete std::get<1>(PAIR);                                              \
    }                                                                          \
} while(false)

///////////////////
// CLEAR_JOIN_AN //
///////////////////
#define CLEAR_JOIN_AN(TUPLE)                                                   \
;do {                                                                          \
    if (std::get<1>(TUPLE)) {                                                  \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
    if (std::get<4>(TUPLE)) {                                                  \
        delete std::get<4>(TUPLE);                                             \
    }                                                                          \
} while (false)

#define CLEAR_FETCHJOIN_AN(TUPLE)                                              \
;do {                                                                          \
    if (std::get<1>(TUPLE)) {                                                  \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
} while (false)

/////////////////////////////
// CLEAR_CHECKANDDECODE_AN //
/////////////////////////////
#define CLEAR_CHECKANDDECODE_AN(TUPLE)                                         \
;do {                                                                          \
    if (std::get<1>(TUPLE)) {                                                  \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
} while (false)

//////////////////////
// CLEAR_GROUPBY_AN //
//////////////////////
#define CLEAR_GROUPBY_AN(TUPLE)                                                \
;do {                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
} while (false)

/////////////////////////
// CLEAR_GROUPEDSUM_AN //
/////////////////////////
#define CLEAR_GROUPEDSUM_AN(TUPLE)                                             \
;do {                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
} while (false)

#endif /* SSBM_HPP */


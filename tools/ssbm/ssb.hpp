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

#include <AHEAD.hpp>

// define
// boost::throw_exception(std::runtime_error("Type name demangling failed"));
namespace boost {

    void throw_exception(
            const std::exception & e) { // user defined
        throw e;
    }
}

using namespace ahead;
using namespace ahead::bat::ops;
#ifdef FORCE_SSE
using namespace ahead::bat::ops::simd::sse;
#elif defined FORCE_AVX2
using namespace ahead::bat::ops::simd::avx2;
#elif defined FORCE_AVX512
using namespace ahead::bat::ops::simd::avx512;
#else
using namespace ahead::bat::ops::scalar;
#endif

///////////////////////////////
// CMDLINE ARGUMENT PARSING  //
///////////////////////////////
namespace ssb {

    enum architecture_t {
        Scalar,
        SSE42,
        AVX2,
        AVX512
    };

#if defined(FORCE_AVX512)
    const architecture_t DEFAULT_ARCHITECTURE = AVX512;
#elif defined(FORCE_AVX2)
    const architecture_t DEFAULT_ARCHITECTURE = AVX2;
#elif defined(FORCE_SSE)
    const architecture_t DEFAULT_ARCHITECTURE = SSE42;
#else
    const architecture_t DEFAULT_ARCHITECTURE = Scalar;
#endif

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

        static const constexpr char * const ID_NUMRUNS = "numruns";
        static const constexpr char * const ID_LENTIMES = "lentimes";
        static const constexpr char * const ID_LENTYPES = "lentypes";
        static const constexpr char * const ID_LENSIZES = "lensizes";
        static const constexpr char * const ID_LENPCM = "lenpcm";
        static const constexpr char * const ID_DBPATH = "numruns";
        static const constexpr char * const ID_VERBOSE = "verbose";
        static const constexpr char * const ID_PRINTRESULT = "printresult";
        static const constexpr char * const ID_CONVERTTABLEFILES = "converttablefiles";

    public:

        SSB_CONF();
        SSB_CONF(
                int argc,
                char** argv);

        void init(
                int argc,
                char** argv);
    };

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
    extern StopWatch swOperator, swTotalTime;
    extern std::vector<StopWatch::rep> opTimes;
    extern std::vector<size_t> batSizes;
    extern std::vector<size_t> batConsumptions;
    extern std::vector<size_t> batConsumptionsProj;
    extern std::vector<size_t> batRSS;
    extern std::vector<bool> hasTwoTypes;
    extern std::vector<boost::typeindex::type_index> headTypes;
    extern std::vector<boost::typeindex::type_index> tailTypes;

    void init(
            int argc,
            char ** argv,
            const char * strHeadline,
            architecture_t arch = DEFAULT_ARCHITECTURE);
    void init_pcm();
    void loadTable(
            const char* tableName,
            const SSB_CONF & CONFIG = ssb::ssb_config);
    void loadTable(
            const std::string & tableName,
            const SSB_CONF & CONFIG = ssb::ssb_config);
    void loadTables(
            std::vector<std::string> && tableNames);
    void loadTables(
            std::vector<std::string> && tableNames,
            std::string && query);
    void clear_stats();
    void before_load();
    void after_load();
    void after_create_columnbats();
    void before_queries();
    void after_queries();
    void before_query();
    void after_query(
            size_t index,
            size_t result);
    void after_query(
            size_t index,
            std::exception & ex);
    void before_op();
    void after_op();
    void lock_for_stats();
    void unlock_for_stats();
    void finalize();

    void print_headline();
    void print_result();

    template<typename T>
    struct PrintBatHelper {
        typedef typename T::type_t print_type_t;
    };

    template<>
    struct PrintBatHelper<v2_tinyint_t> {
        typedef int print_type_t;
    };

    template<typename Head, typename Tail>
    void printBat(
            StopWatch & sw,
            BAT<Head, Tail> *bat,
            const char* filename,
            const char* message = nullptr) {
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
        const constexpr size_t wOID = std::numeric_limits<oid_t>::digits10;
        const constexpr bool isHeadLT16 = ahead::larger_type<head_t, uint16_t>::isSecondLarger;
        const constexpr size_t wHead = isHeadLT16 ? std::numeric_limits<uint16_t>::digits10 : std::numeric_limits<head_t>::digits10;
        const constexpr bool isTailLT16 = ahead::larger_type<tail_t, uint16_t>::isSecondLarger;
        const constexpr size_t wTail = isTailLT16 ? std::numeric_limits<uint16_t>::digits10 : std::numeric_limits<tail_t>::digits10;
        auto iter = bat->begin();
        fout << std::right;
        fout << std::setw(wOID) << "void";
        fout << " | " << std::setw(wHead) << "head";
        fout << " | " << std::setw(wTail) << "tail";
        fout << '\n';
        fout << std::setw(wOID) << "oid_t";
        fout << " | " << std::setw(wHead) << bat->type_head().pretty_name();
        fout << " | " << std::setw(wTail) << bat->type_tail().pretty_name();
        fout << '\n';
        for (; iter->hasNext(); ++i, ++*iter) {
            fout << std::setw(wOID) << i;
            fout << " | " << std::setw(wHead) << static_cast<typename PrintBatHelper<Head>::print_type_t>(iter->head());
            fout << " | " << std::setw(wTail) << static_cast<typename PrintBatHelper<Tail>::print_type_t>(iter->tail());
            fout << '\n';
        }
        fout << std::flush;
        fout.close();
        delete iter;
        sw.resume();
    }

#define PRINTBAT_TEMPLATES(v2_head_t) \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_void_t> *bat, const char* filename, const char* message); \
extern template void printBat(StopWatch & sw, BAT<v2_head_t, v2_oid_t> *bat, const char* filename, const char* message); \
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

    PRINTBAT_TEMPLATES(v2_void_t)
    PRINTBAT_TEMPLATES(v2_oid_t)
    PRINTBAT_TEMPLATES(v2_tinyint_t)
    PRINTBAT_TEMPLATES(v2_shortint_t)
    PRINTBAT_TEMPLATES(v2_int_t)
    PRINTBAT_TEMPLATES(v2_bigint_t)
    PRINTBAT_TEMPLATES(v2_str_t)
    PRINTBAT_TEMPLATES(v2_restinyint_t)
    PRINTBAT_TEMPLATES(v2_resshortint_t)
    PRINTBAT_TEMPLATES(v2_resint_t)
    PRINTBAT_TEMPLATES(v2_resbigint_t)
    PRINTBAT_TEMPLATES(v2_resstr_t)

#undef PRINTBAT_TEMPLATES

}

#endif /* SSBM_HPP */


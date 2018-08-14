// Copyright (c) 2017 Till Kolditz
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
 * File:   ssb.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 10-04-2017 11:03
 */

#include <iostream>
#include <iomanip>
#include <execinfo.h>
#include <signal.h>
#include <exception>
#include <sstream>

#include <mutex>

#include "ssb.hpp"
#include <column_storage/TransactionManager.h>

namespace ssb {

    void handler(
            int sig) {
        const constexpr int BACKTRACE_SIZE = 64;
        void *array[BACKTRACE_SIZE];
        std::size_t size;

        // get void*'s for all entries on the stack
        size = backtrace(array, BACKTRACE_SIZE);

        std::exception_ptr pEx = std::current_exception();

        // print out all the frames to stderr
        std::cerr << "\nError: signal " << sig << ':';
        switch (sig) {
            case SIGSEGV:
                std::cerr << " SIGSEGV";
                break;
            case SIGTERM:
                std::cerr << " SIGTERM";
                try {
                    std::rethrow_exception(pEx);
                } catch (std::exception & ex) {
                    std::cerr << ex.what() << ":\n";
                } catch (...) {
                    std::cerr << "Unknown exception type caught!";
                }
                break;
            default:
                std::cerr << " OTHER";
        }
        std::cerr << '\n';
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        std::cerr << std::flush;
        exit(1);
    }

    const constexpr char * const SSB_CONF::ID_NUMRUNS;
    const constexpr char * const SSB_CONF::ID_LENTIMES;
    const constexpr char * const SSB_CONF::ID_LENTYPES;
    const constexpr char * const SSB_CONF::ID_LENSIZES;
    const constexpr char * const SSB_CONF::ID_LENPCM;
    const constexpr char * const SSB_CONF::ID_VERBOSE;
    const constexpr char * const SSB_CONF::ID_PRINTRESULT;

    const constexpr size_t SSB_CONF::DEF_NUMRUNS;
    const constexpr size_t SSB_CONF::DEF_NUMTHREADS;
    const constexpr size_t SSB_CONF::DEF_LENTIMES;
    const constexpr size_t SSB_CONF::DEF_LENTYPES;
    const constexpr size_t SSB_CONF::DEF_LENSIZES;
    const constexpr size_t SSB_CONF::DEF_LENPCM;
    const constexpr bool SSB_CONF::DEF_VERBOSE;
    const constexpr bool SSB_CONF::DEF_PRINTRESULT;

    SSB_CONF::SSB_CONF()
            : NUM_RUNS(0),
              NUM_THREADS(0),
              LEN_TIMES(0),
              LEN_TYPES(0),
              LEN_SIZES(0),
              LEN_PCM(0),
              VERBOSE(false),
              PRINT_RESULT(false),
              parser( {std::forward_as_tuple(ID_NUMRUNS, alias_list_t {"--numruns", "-n"}, DEF_NUMRUNS), std::forward_as_tuple(ID_NUMTHREADS, alias_list_t {"--numthreads", "-t"}, DEF_NUMTHREADS),
                      std::forward_as_tuple(ID_LENTIMES, alias_list_t {"--lentimes"}, DEF_LENTIMES), std::forward_as_tuple(ID_LENTYPES, alias_list_t {"--lentypes"}, DEF_LENTYPES),
                      std::forward_as_tuple(ID_LENSIZES, alias_list_t {"--lensizes"}, DEF_LENSIZES), std::forward_as_tuple(ID_LENPCM, alias_list_t {"--lenpcm"}, DEF_LENPCM)}, {}, {
                      std::forward_as_tuple(ID_VERBOSE, alias_list_t {"--verbose", "-v"}, DEF_VERBOSE), std::forward_as_tuple(ID_PRINTRESULT, alias_list_t {"--print-result", "-p"}, DEF_PRINTRESULT)}) {
        signal(SIGSEGV, handler);
        signal(SIGTERM, handler);
    }

    SSB_CONF::SSB_CONF(
            int argc,
            const char * const * argv)
            : SSB_CONF() {
        init(argc, argv);
    }

    void SSB_CONF::init(
            int argc,
            const char * const * argv) {
        parser.parse(argc, argv, 1);
        NUM_RUNS = parser.get_uint(ID_NUMRUNS);
        NUM_THREADS = parser.get_uint(ID_NUMTHREADS);
        LEN_TIMES = parser.get_uint(ID_LENTIMES);
        LEN_TYPES = parser.get_uint(ID_LENTYPES);
        LEN_SIZES = parser.get_uint(ID_LENSIZES);
        LEN_PCM = parser.get_uint(ID_LENPCM);
        VERBOSE = parser.get_bool(ID_VERBOSE);
        PRINT_RESULT = parser.get_bool(ID_PRINTRESULT);
    }

    SSB_CONF ssb_config;
#ifdef AHEAD_PCM
    PCM * m = nullptr;
    PCM::ErrorCode pcmStatus = PCM::UnknownError;
#endif
    std::mutex mutex_stats;
    std::size_t rssBeforeLoad, rssAfterLoad, rssAfterCopy, rssAfterQueries;
#ifdef AHEAD_PCM
    std::vector<CoreCounterState> cstate1, cstate2, cstate3;
    std::vector<SocketCounterState> sktstate1, sktstate2, sktstate3;
    SystemCounterState sysstate1, sysstate2, sysstate3;
#endif
    std::string emptyString;
    std::vector<StopWatch::rep> totalTimes;
    ahead::StopWatch swOperatorTime, swTotalTime;

    std::vector<ahead::StopWatch::rep> opTimes;
    std::vector<std::size_t> batSizes;
    std::vector<std::size_t> batConsumptions;
    std::vector<std::size_t> batConsumptionsProj;
    std::vector<std::size_t> batRSS;
#ifdef AHEAD_PCM
    std::vector<SystemCounterState> sysstatesBeforeOp, sysstatesAfterOp;
#endif
    std::vector<bool> hasTwoTypes;
    std::vector<boost::typeindex::type_index> headTypes;
    std::vector<boost::typeindex::type_index> tailTypes;

    int run(
            int argc,
            char ** argv,
            std::function<int(
                    int,
                    char **)> runnable) {
        try {
            return runnable(argc, argv);
        } catch (std::exception & e) {
            std::cerr << "[ERROR] " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[ERROR] Unanticipated exception caught!" << std::endl;
        }
        return 1;
    }

    void init(
            int argc,
            const char * const * argv,
            const char * strHeadline,
            architecture_t arch) {
#ifdef AHEAD_PCM
        set_signal_handlers();
#endif

        rssBeforeLoad = 0;
        rssAfterLoad = 0;
        rssAfterCopy = 0;
        rssAfterQueries = 0;

        const constexpr std::size_t numOpsDefault = 64;
        ssb::opTimes.reserve(numOpsDefault);
        ssb::batSizes.reserve(numOpsDefault);
        ssb::batConsumptions.reserve(numOpsDefault);
        ssb::batConsumptionsProj.reserve(numOpsDefault);
        ssb::batRSS.reserve(numOpsDefault);
#ifdef AHEAD_PCM
        ssb::sysstatesBeforeOp.reserve(numOpsDefault);
        ssb::sysstatesAfterOp.reserve(numOpsDefault);
#endif
        ssb::hasTwoTypes.reserve(numOpsDefault);
        ssb::headTypes.reserve(numOpsDefault);
        ssb::tailTypes.reserve(numOpsDefault);

        ssb::ssb_config.init(argc, argv);
        ssb::totalTimes.reserve(ssb::ssb_config.NUM_RUNS);
        std::stringstream ss;
        ss << strHeadline << ' ';
        switch (arch) {
            case AVX512:
                ss << "AVX-512";
                break;
            case AVX2:
                ss << "AVX-2";
                break;
            case SSE42:
                ss << "SSE-4.2";
                break;
            case Scalar:
                ss << "Scalar";
                break;
            default:
                ss << "Unknown";
        }
        auto strHeadline2 = ss.str();
        std::cout << strHeadline2 << '\n';
        auto fillChar = std::cout.fill('=');
        std::cout << std::setw(strHeadline2.size()) << "=" << std::setfill(fillChar) << '\n';
        auto instance = ahead::AHEAD::createInstance(argc, argv);
        std::cout << "Database path: \"" << instance->getConfig().getDBPath() << "\"" << std::endl;
#ifdef AHEAD_PCM
        ssb::init_pcm();
#endif
    }

#ifdef AHEAD_PCM
    void init_pcm() {
        ssb::m = PCM::getInstance();
        ssb::pcmStatus = ssb::m->program();
        if (ssb::pcmStatus == PCM::PMUBusy) {
            ssb::m->resetPMU();
            ssb::pcmStatus = ssb::m->program();
        }
        if (ssb::pcmStatus != PCM::Success) {
            std::cerr << "Intel's PCM couldn't start" << '\n';
            std::cerr << "\tError code: " << pcmStatus << '\n';
        }
        if (ssb::pcmStatus == PCM::Success) {
            ssb::m->getAllCounterStates(ssb::sysstate1, ssb::sktstate1, ssb::cstate1);
        }
    }
#endif

    void loadTable(
            const char* tableName,
            const SSB_CONF & config) {
        ahead::StopWatch sw;
        sw.start();
        std::size_t numBUNs = AHEAD::getInstance()->loadTable(tableName);
        sw.stop();
        if (config.VERBOSE) {
            std::cout << "Table: " << tableName << "\n\tNumber of BUNs: " << numBUNs << "\n\tTime: " << sw.duration() << " ns." << std::endl;
        }
    }

    void loadTable(
            const std::string & tableName,
            const SSB_CONF & config) {
        loadTable(tableName.c_str(), config);
    }

    void loadTables(
            std::vector<std::string> && tableNames) {
        if (ssb::ssb_config.VERBOSE) {
            std::cout << "[VERBOSE] ssb::loadTables(@" << __LINE__ << " Loading Tables:";
            for (const std::string & tab : tableNames) {
                std::cout << " '" << tab << "'";
            }
            std::cout << std::endl;
        }
        ahead::StopWatch sw;
        sw.start();
        ssb::before_load();
        for (const std::string & tab : tableNames) {
            ssb::loadTable(tab);
        }
        ssb::after_load();
        sw.stop();
        std::cout << "Total loading time: " << sw.duration() << " ns.\n" << std::endl;
    }

    void loadTables(
            std::vector<std::string> && tableNames,
            std::string && query) {
        loadTables(std::forward<std::vector<std::string>>(tableNames));
        if (ssb::ssb_config.VERBOSE && query.size() != 0) {
            std::cout << query << std::endl;
        }
    }

    void clear_stats() {
        ssb::opTimes.clear();
        ssb::batSizes.clear();
        ssb::batConsumptions.clear();
        ssb::batConsumptionsProj.clear();
        ssb::batRSS.clear();
        ssb::hasTwoTypes.clear();
        ssb::headTypes.clear();
        ssb::tailTypes.clear();
#ifdef AHEAD_PCM
        ssb::sysstatesBeforeOp.clear();
        ssb::sysstatesAfterOp.clear();
#endif
    }

    void before_load() {
        ssb::rssBeforeLoad = getPeakRSS(size_enum_t::B);
    }

    void after_load() {
        ssb::rssAfterLoad = getPeakRSS(size_enum_t::B);
    }

    void after_create_columnbats() {
        if (ssb::ssb_config.VERBOSE) {
            std::cout << "\n(Create ColumnBATs)\n";
            ssb::print_headline();
            ssb::print_result();
            std::cout << std::endl;
        }
        ssb::clear_stats();
    }

    void before_queries() {
        if (ssb::ssb_config.VERBOSE) {
            std::cout << "\n(Copying)\n";
            ssb::print_headline();
            ssb::print_result();
            std::cout << std::endl;
        }
#ifdef AHEAD_PCM
        if (ssb::pcmStatus == PCM::Success) {
            ssb::m->getAllCounterStates(ssb::sysstate2, ssb::sktstate2, ssb::cstate2);
        }
#endif
        ssb::rssAfterCopy = getPeakRSS(size_enum_t::B);
    }

    void after_queries() {
        ssb::rssAfterQueries = getPeakRSS(size_enum_t::B);
        if (ssb::ssb_config.VERBOSE) {
            std::cout << "Memory statistics (Resident Set size in B):\n\t";
            std::cout << std::setw(15) << "before load: " << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::rssBeforeLoad << "\n\t";
            std::cout << std::setw(15) << "after load: " << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::rssAfterLoad << "\n\t";
            std::cout << std::setw(15) << "after copy: " << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::rssAfterCopy << "\n\t";
            std::cout << std::setw(15) << "after queries: " << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::rssAfterQueries << "\n";
        }
        std::cout << "TotalTimes:\n";
        for (std::size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
            std::cout << std::setw(2) << i << '\t' << ssb::totalTimes[i] << '\n';
        }
        std::cout << "Memory:\n" << ssb::rssBeforeLoad << '\n' << ssb::rssAfterLoad << '\n' << ssb::rssAfterCopy << '\n' << ssb::rssAfterQueries << std::endl;

#ifdef AHEAD_PCM
#define PCM_PRINT(attr, proc, state1, state2, state3)                          \
        std::cout << std::setw(ssb::ssb_config.LEN_PCM) << attr << '\t';                    \
        std::cout << std::setw(ssb::ssb_config.LEN_PCM) << proc(state1, state2) << '\t';    \
        std::cout << std::setw(ssb::ssb_config.LEN_PCM) << proc(state2, state3) << '\n';

        if (ssb::pcmStatus == PCM::Success) {
            ssb::m->getAllCounterStates(ssb::sysstate3, ssb::sktstate3, ssb::cstate3);
            std::cout << "PCM:\n";
            std::cout << std::setw(ssb::ssb_config.LEN_PCM) << "Attribute" << '\t';
            std::cout << std::setw(ssb::ssb_config.LEN_PCM) << "init" << '\t';
            std::cout << std::setw(ssb::ssb_config.LEN_PCM) << "query" << '\n';
            PCM_PRINT("IPC", getIPC, ssb::sysstate1, ssb::sysstate2, ssb::sysstate3);
            PCM_PRINT("MC Read", getBytesReadFromMC, ssb::sysstate1, ssb::sysstate2, ssb::sysstate3);
            PCM_PRINT("MC written", getBytesWrittenToMC, ssb::sysstate1, ssb::sysstate2, ssb::sysstate3);
            PCM_PRINT("Energy", getConsumedEnergy, ssb::sysstate1, ssb::sysstate2, ssb::sysstate3);
            PCM_PRINT("Joules", getConsumedJoules, ssb::sysstate1, ssb::sysstate2, ssb::sysstate3);
            PCM_PRINT("L2-Miss Cycle Loss", getCyclesLostDueL2CacheMisses, ssb::sysstate1, ssb::sysstate2, ssb::sysstate3);
            PCM_PRINT("L3-Miss Cycle Loss", getCyclesLostDueL3CacheMisses, ssb::sysstate1, ssb::sysstate2, ssb::sysstate3);
        }
#undef PCM_PRINT
#endif
    }

    void before_query() {
        ssb::clear_stats();
        ssb::swTotalTime.start();
    }

    void after_query(
            std::size_t index,
            std::size_t result) {
        ssb::totalTimes[index] = ssb::swTotalTime.stop();
        std::cout << "(" << std::setw(2) << index << ")\n\tresult: " << result << "\n\t  time: " << ssb::swTotalTime.duration() << " ns.\n";
        ssb::print_headline();
        ssb::print_result();
    }

    void after_query(
            std::size_t index,
            std::exception & ex) {
        ssb::totalTimes[index] = ssb::swTotalTime.stop();
        std::cout << "(" << std::setw(2) << index << ")\n\tresult: " << ex.what() << "\n\t  time: " << ssb::swTotalTime.duration() << " ns.\n";
    }

    void before_op() {
#ifdef AHEAD_PCM
        if (ssb::pcmStatus == PCM::Success) {
            ssb::sysstatesBeforeOp.push_back(ssb::m->getSystemCounterState());
        }
#endif
        ssb::swOperatorTime.start();
    }

    void after_op() {
        ssb::opTimes.push_back(ssb::swOperatorTime.stop());
#ifdef AHEAD_PCM
        if (ssb::pcmStatus == PCM::Success) {
            ssb::sysstatesAfterOp.push_back(ssb::m->getSystemCounterState());
        }
#endif
        ssb::batRSS.push_back(getPeakRSS(size_enum_t::B));
    }

    void lock_for_stats() {
        mutex_stats.lock();
    }

    void unlock_for_stats() {
        mutex_stats.unlock();
    }

    void finalize() {
        AHEAD::destroyInstance();
    }

    void print_headline() {
        std::cout << "\tname";
        std::cout << "\t" << std::setw(ssb::ssb_config.LEN_TIMES) << "time [ns]";
        std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "size [#]";
        std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "consum [B]";
        std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "proj [B]";
        std::cout << "\t " << std::setw(ssb::ssb_config.LEN_SIZES) << " RSS Δ [B]";
        std::cout << "\t" << std::setw(ssb::ssb_config.LEN_TYPES) << "type head";
        std::cout << "\t" << std::setw(ssb::ssb_config.LEN_TYPES) << "type tail";
#ifdef AHEAD_PCM
        if (ssb::pcmStatus == PCM::Success) {
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "Inst. Retired";
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "IPC";
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "Cycles";
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "Cycles lost L2";
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "Cycles lost L3";
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "L2 Hit Ratio";
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "L3 Hit Ratio";
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "MemCtl Read [B]";
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "MemCtl Write [B]";
        }
#endif
        std::cout << "\n";
    }

    void print_result() {
        for (std::size_t k = 0; k < ssb::opTimes.size(); ++k) {
            std::cout << "\top" << std::setw(2) << (k + 1);
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_TIMES) << ssb::opTimes[k];
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::batSizes[k];
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::batConsumptions[k];
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::batConsumptionsProj[k];
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << (k == 0 ? (ssb::batRSS[k] - ssb::rssAfterCopy) : (ssb::batRSS[k] - ssb::batRSS[k - 1]));
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_TYPES) << ssb::headTypes[k].pretty_name();
            std::cout << "\t" << std::setw(ssb::ssb_config.LEN_TYPES) << (ssb::hasTwoTypes[k] ? ssb::tailTypes[k].pretty_name() : ssb::emptyString);
#ifdef AHEAD_PCM
            if (ssb::pcmStatus == PCM::Success) {
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getInstructionsRetired(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getIPC(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getCycles(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getCyclesLostDueL2CacheMisses(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getCyclesLostDueL3CacheMisses(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getL2CacheHitRatio(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getL3CacheHitRatio(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getBytesReadFromMC(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
                std::cout << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << getBytesWrittenToMC(sysstatesBeforeOp[k], sysstatesAfterOp[k]);
            }
#endif
            std::cout << '\n';
        }
        std::cout << std::flush;
    }

#define PRINTBAT_TEMPLATES(v2_head_t) \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_void_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_oid_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_tinyint_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_shortint_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_int_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_bigint_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_str_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_restinyint_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_resshortint_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_resint_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_resbigint_t> *bat, const char* filename, const char* message); \
template void printBat(StopWatch & sw, BAT<v2_head_t, v2_resstr_t> *bat, const char* filename, const char* message);

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

}

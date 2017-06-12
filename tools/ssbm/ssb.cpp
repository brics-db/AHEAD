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
#include <execinfo.h>
#include <signal.h>

#include "ssb.hpp"

namespace ssb {

    void handler(
            int sig) {
        constexpr const int BACKTRACE_SIZE = 64;
        void *array[BACKTRACE_SIZE];
        size_t size;

        // get void*'s for all entries on the stack
        size = backtrace(array, BACKTRACE_SIZE);

        // print out all the frames to stderr
        std::cerr << "Error: signal " << sig << ":\\n";
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        exit(1);
    }

    constexpr const char * const SSB_CONF::ID_NUMRUNS;
    constexpr const char * const SSB_CONF::ID_LENTIMES;
    constexpr const char * const SSB_CONF::ID_LENTYPES;
    constexpr const char * const SSB_CONF::ID_LENSIZES;
    constexpr const char * const SSB_CONF::ID_LENPCM;
    constexpr const char * const SSB_CONF::ID_DBPATH;
    constexpr const char * const SSB_CONF::ID_VERBOSE;
    constexpr const char * const SSB_CONF::ID_PRINTRESULT;
    constexpr const char * const SSB_CONF::ID_CONVERTTABLEFILES;

    SSB_CONF::SSB_CONF()
            : NUM_RUNS(0),
              LEN_TIMES(0),
              LEN_TYPES(0),
              LEN_SIZES(0),
              LEN_PCM(0),
              DB_PATH(),
              VERBOSE(false),
              PRINT_RESULT(0),
              CONVERT_TABLE_FILES(true),
              parser(
                      {std::forward_as_tuple(ID_NUMRUNS, alias_list_t {"--numruns", "-n"}, 15), std::forward_as_tuple(ID_LENTIMES, alias_list_t {"--lentimes"}, 16), std::forward_as_tuple(ID_LENTYPES,
                              alias_list_t {"--lentypes"}, 20), std::forward_as_tuple(ID_LENSIZES, alias_list_t {"--lensizes"}, 16), std::forward_as_tuple(ID_LENPCM, alias_list_t {"--lenpcm"}, 16)}, {
                              std::forward_as_tuple(ID_DBPATH, alias_list_t {"--dbpath", "-d"}, ".")}, {std::forward_as_tuple(ID_VERBOSE, alias_list_t {"--verbose", "-v"}, false),
                              std::forward_as_tuple(ID_PRINTRESULT, alias_list_t {"--print-result", "-p"}, false), std::forward_as_tuple(ID_CONVERTTABLEFILES, alias_list_t {"--convert-table-files",
                                      "-c"}, true)}) {
    }

    SSB_CONF::SSB_CONF(
            int argc,
            char** argv)
            : SSB_CONF() {
        init(argc, argv);
    }

    void SSB_CONF::init(
            int argc,
            char** argv) {
        signal(SIGSEGV, handler);
        parser.parse(argc, argv, 1);
        NUM_RUNS = parser.get_uint(ID_NUMRUNS);
        LEN_TIMES = parser.get_uint(ID_LENTIMES);
        LEN_TYPES = parser.get_uint(ID_LENTYPES);
        LEN_SIZES = parser.get_uint(ID_LENSIZES);
        LEN_PCM = parser.get_uint(ID_LENPCM);
        DB_PATH = parser.get_str(ID_DBPATH);
        VERBOSE = parser.get_bool(ID_VERBOSE);
        PRINT_RESULT = parser.get_bool(ID_PRINTRESULT);
        CONVERT_TABLE_FILES = parser.get_bool(ID_CONVERTTABLEFILES);
    }

    StopWatch::rep loadTable(
            const char* const tableName,
            const SSB_CONF & CONFIG) {
        StopWatch sw;
        sw.start();
        size_t numBUNs = AHEAD::getInstance()->loadTable(tableName);
        sw.stop();
        if (CONFIG.VERBOSE) {
            std::cout << "Table: " << tableName << "\n\tNumber of BUNs: " << numBUNs << "\n\tTime: " << sw << " ns." << std::endl;
        }
        return sw.duration();
    }

    SSB_CONF ssb_config;
    PCM * m = nullptr;
    PCM::ErrorCode pcmStatus = PCM::UnknownError;
    size_t rssBeforeLoad, rssAfterLoad, rssAfterCopy, rssAfterQueries;
    std::vector<CoreCounterState> cstate1, cstate2, cstate3;
    std::vector<SocketCounterState> sktstate1, sktstate2, sktstate3;
    SystemCounterState sysstate1, sysstate2, sysstate3;
    std::string emptyString;
    std::vector<StopWatch::rep> totalTimes;
    StopWatch sw1, sw2;

    std::vector<StopWatch::rep> opTimes;
    std::vector<size_t> batSizes;
    std::vector<size_t> batConsumptions;
    std::vector<size_t> batConsumptionsProj;
    std::vector<size_t> batRSS;
    std::vector<bool> hasTwoTypes;
    std::vector<boost::typeindex::type_index> headTypes;
    std::vector<boost::typeindex::type_index> tailTypes;

    void init(
            int argc,
            char ** argv,
            const char * strHeadline) {
        set_signal_handlers();

        constexpr const size_t numOpsDefault = 64;
        ssb::opTimes.reserve(numOpsDefault);
        ssb::batSizes.reserve(numOpsDefault);
        ssb::batConsumptions.reserve(numOpsDefault);
        ssb::batConsumptionsProj.reserve(numOpsDefault);
        ssb::batRSS.reserve(numOpsDefault);
        ssb::hasTwoTypes.reserve(numOpsDefault);
        ssb::headTypes.reserve(numOpsDefault);
        ssb::tailTypes.reserve(numOpsDefault);

        ssb::ssb_config.init(argc, argv);
        ssb::totalTimes.reserve(ssb::ssb_config.NUM_RUNS);
        ssb::init_pcm();
        std::cout << strHeadline << std::endl;
        ahead::AHEAD::createInstance(ssb::ssb_config.DB_PATH.c_str());
    }

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

    void clear_stats() {
        ssb::opTimes.clear();
        ssb::batSizes.clear();
        ssb::batConsumptions.clear();
        ssb::batConsumptionsProj.clear();
        ssb::batRSS.clear();
        ssb::hasTwoTypes.clear();
        ssb::headTypes.clear();
        ssb::tailTypes.clear();
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
        ssb::rssAfterCopy = getPeakRSS(size_enum_t::B);
        if (ssb::ssb_config.VERBOSE) {
            std::cout << "\n(Copying)\n";
            ssb::print_headline();
            ssb::print_result();
            std::cout << std::endl;
        }
        if (ssb::pcmStatus == PCM::Success) {
            ssb::m->getAllCounterStates(ssb::sysstate2, ssb::sktstate2, ssb::cstate2);
        }
    }

    void after_queries() {
        ssb::rssAfterQueries = getPeakRSS(size_enum_t::B);
        if (ssb::ssb_config.VERBOSE) {
            std::cout << "Memory statistics (Resident Set size in B):\n\t" << std::setw(15) << "before load: " << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::rssBeforeLoad << "\n\t" << std::setw(15)
                    << "after load: " << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::rssAfterLoad << "\n\t" << std::setw(15) << "after copy: " << std::setw(ssb::ssb_config.LEN_SIZES)
                    << ssb::rssAfterCopy << "\n\t" << std::setw(15) << "after queries: " << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::rssAfterQueries << "\n";
        }
        std::cout << "TotalTimes:\n";
        for (size_t i = 0; i < ssb::ssb_config.NUM_RUNS; ++i) {
            std::cout << std::setw(2) << i << '\t' << ssb::totalTimes[i] << '\n';
        }
        std::cout << "Memory:\n" << ssb::rssBeforeLoad << '\n' << ssb::rssAfterLoad << '\n' << ssb::rssAfterCopy << '\n' << ssb::rssAfterQueries << std::endl;
    }

    void before_query() {
        ssb::clear_stats();
        ssb::sw2.start();
    }

    void after_query(
            size_t index,
            size_t result) {
        ssb::totalTimes[index] = ssb::sw2.stop();
        std::cout << "(" << std::setw(2) << index << ")\n\tresult: " << result << "\n\t  time: " << ssb::sw2 << " ns.\n";
        ssb::print_headline();
        ssb::print_result();
    }

    void before_op() {
        ssb::sw1.start();
    }

    void after_op() {
        ssb::opTimes.push_back(ssb::sw1.stop());
        ssb::batRSS.push_back(getPeakRSS(size_enum_t::B));
    }

    ///////////////
    // PCM_PRINT //
    ///////////////
#define PCM_PRINT(attr, proc, state1, state2, state3)                          \
        std::cout << std::setw(ssb::ssb_config.LEN_PCM) << attr << '\t';                    \
        std::cout << std::setw(ssb::ssb_config.LEN_PCM) << proc(state1, state2) << '\t';    \
        std::cout << std::setw(ssb::ssb_config.LEN_PCM) << proc(state2, state3) << '\n';

    void finalize() {
        ahead::TransactionManager::destroyInstance();
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
    }

#undef PCM_PRINT

    void print_headline() {
        std::cout << "\tname\t" << std::setw(ssb::ssb_config.LEN_TIMES) << "time [ns]" << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "size [#]" << "\t" << std::setw(ssb::ssb_config.LEN_SIZES)
                << "consum [B]" << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << "proj [B]" << "\t " << std::setw(ssb::ssb_config.LEN_SIZES) << " RSS Δ [B]" << "\t"
                << std::setw(ssb::ssb_config.LEN_TYPES) << "type head" << "\t" << std::setw(ssb::ssb_config.LEN_TYPES) << "type tail" << "\n";
    }

    void print_result() {
        for (size_t k = 0; k < ssb::opTimes.size(); ++k) {
            std::cout << "\top" << std::setw(2) << k << "\t" << std::setw(ssb::ssb_config.LEN_TIMES) << hrc_duration(ssb::opTimes[k]) << "\t" << std::setw(ssb::ssb_config.LEN_SIZES)
                    << ssb::batSizes[k] << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::batConsumptions[k] << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << ssb::batConsumptionsProj[k]
                    << "\t" << std::setw(ssb::ssb_config.LEN_SIZES) << (k == 0 ? (ssb::batRSS[k] - ssb::rssAfterCopy) : (ssb::batRSS[k] - ssb::batRSS[k - 1])) << "\t"
                    << std::setw(ssb::ssb_config.LEN_TYPES) << ssb::headTypes[k].pretty_name() << "\t" << std::setw(ssb::ssb_config.LEN_TYPES)
                    << (ssb::hasTwoTypes[k] ? ssb::tailTypes[k].pretty_name() : ssb::emptyString) << '\n';
        }
        std::cout << std::flush;
    }

}

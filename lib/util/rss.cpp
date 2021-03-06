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
 * File:   rss.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 5. August 2016, 17:23
 */

#include <util/rss.hpp>

namespace ahead {

    /*
     * Author:  David Robert Nadeau
     * Site:    http://NadeauSoftware.com/
     * License: Creative Commons Attribution 3.0 Unported License
     *          http://creativecommons.org/licenses/by/3.0/deed.en_US
     */

#if defined(_WIN32)
#include <windows.h>
#include <psapi.h>

#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <unistd.h>
#include <sys/resource.h>

#if defined(__APPLE__) && defined(__MACH__)
#include <mach/mach.h>

#elif (defined(_AIX) || defined(__TOS__AIX__)) || (defined(__sun__) || defined(__sun) || defined(sun) && (defined(__SVR4) || defined(__svr4__)))
#include <fcntl.h>
#include <procfs.h>

#elif defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
#include <stdio.h>

#endif

#else
#error "Cannot define getPeakRSS( ) or getCurrentRSS( ) for an unknown OS."
#endif

    /**
     * Returns the peak (maximum so far) resident set size (physical
     * memory use) measured in bytes, or zero if the value cannot be
     * determined on this OS.
     */
    size_t getPeakRSS(
            size_enum_t size_enum) {
        size_t result = 0ull;
#if defined(_WIN32)
        /* Windows -------------------------------------------------- */
        PROCESS_MEMORY_COUNTERS info;
        GetProcessMemoryInfo(GetCurrentProcess(), &info, sizeof (info));
        result = (size_t)info.PeakWorkingSetSize;

#elif (defined(_AIX) || defined(__TOS__AIX__)) || (defined(__sun__) || defined(__sun) || defined(sun) && (defined(__SVR4) || defined(__svr4__)))
        /* AIX and Solaris ------------------------------------------ */
        struct psinfo psinfo;
        int fd = -1;
        if ((fd = open("/proc/self/psinfo", O_RDONLY)) == -1)
        return (size_t)0L; /* Can't open? */
        if (read(fd, &psinfo, sizeof (psinfo)) != sizeof (psinfo)) {
            close(fd);
            return (size_t)0L; /* Can't read? */
        }
        close(fd);
        result = (size_t)(psinfo.pr_rssize * 1024L);

#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
        /* BSD, Linux, and OSX -------------------------------------- */
        struct rusage rusage;
        getrusage(RUSAGE_SELF, &rusage);
#if defined(__APPLE__) && defined(__MACH__)
        result = (size_t)rusage.ru_maxrss;
#else
        result = (size_t) (rusage.ru_maxrss * 1024L);
#endif

#else
        /* Unknown OS ----------------------------------------------- */
        /* Unsupported. */
#endif

        switch (size_enum) {
            case size_enum_t::B:
                return result;
            case size_enum_t::KB:
                return result / 1024ull;
            case size_enum_t::MB:
                return result / (1024ull * 1024ull);
            case size_enum_t::GB:
                return result / (1024ull * 1024ull * 1024ull);
            case size_enum_t::TB:
                return result / (1024ull * 1024ull * 1024ull * 1024ull);
            default:
                return result;
        }
    }

    /**
     * Returns the current resident set size (physical memory use) measured
     * in bytes, or zero if the value cannot be determined on this OS.
     */
    size_t getCurrentRSS(
            size_enum_t size_enum) {
        size_t result = 0ull;
#if defined(_WIN32)
        /* Windows -------------------------------------------------- */
        PROCESS_MEMORY_COUNTERS info;
        GetProcessMemoryInfo(GetCurrentProcess(), &info, sizeof (info));
        result = (size_t)info.WorkingSetSize;

#elif defined(__APPLE__) && defined(__MACH__)
        /* OSX ------------------------------------------------------ */
        struct mach_task_basic_info info;
        mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
        if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                        (task_info_t) & info, &infoCount) != KERN_SUCCESS)
        return (size_t)0L; /* Can't access? */
        result = (size_t)info.resident_size;

#elif defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
        /* Linux ---------------------------------------------------- */
        long rss = 0L;
        FILE* fp = NULL;
        if ((fp = fopen("/proc/self/statm", "r")) == NULL)
            return (size_t) 0L; /* Can't open? */
        if (fscanf(fp, "%*s%ld", &rss) != 1) {
            fclose(fp);
            return (size_t) 0L; /* Can't read? */
        }
        fclose(fp);
        result = (size_t) rss * (size_t) sysconf(_SC_PAGESIZE);

#else
        /* AIX, BSD, Solaris, and Unknown OS ------------------------ */
        /* Unsupported. */
#endif

        switch (size_enum) {
            case size_enum_t::B:
                return result;
            case size_enum_t::KB:
                return result / 1024ull;
            case size_enum_t::MB:
                return result / (1024ull * 1024ull);
            case size_enum_t::GB:
                return result / (1024ull * 1024ull * 1024ull);
            case size_enum_t::TB:
                return result / (1024ull * 1024ull * 1024ull * 1024ull);
            default:
                return result;
        }
    }

}

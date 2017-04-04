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

#include <util/stopwatch.hpp>

namespace ahead {

    const bool OUTPUT_INSERT_DOT = true;

    StopWatch::StopWatch()
            : startNS(), stopNS(), totalNS(std::chrono::duration_cast<std::chrono::nanoseconds>(stopNS - startNS).count()) {
    }

    StopWatch::StopWatch(time_point startNS, time_point stopNS, rep totalNS)
            : startNS(startNS), stopNS(stopNS), totalNS(totalNS) {
    }

    void StopWatch::start() {
        totalNS = 0;
        startNS = std::chrono::high_resolution_clock::now();
    }

    void StopWatch::resume() {
        startNS = std::chrono::high_resolution_clock::now();
    }

    StopWatch::rep StopWatch::stop() {
        stopNS = std::chrono::high_resolution_clock::now();
        totalNS += std::chrono::duration_cast<std::chrono::nanoseconds>(stopNS - startNS).count();
        return duration();
    }

    StopWatch::rep StopWatch::duration() {
        return totalNS;
    }

    hrc_duration::hrc_duration(StopWatch::rep dura)
            : dura(dura) {
    }

    std::ostream& operator<<(std::ostream& stream, hrc_duration hrcd) {
        std::chrono::high_resolution_clock::rep dura = hrcd.dura;
        std::stringstream ss;
        if (OUTPUT_INSERT_DOT) {
            size_t max = 1000;
            while (dura / max > 0) {
                max *= 1000;
            }
            max /= 1000;
            ss << std::setfill('0') << (dura / max);
            while (max > 1) {
                dura %= max;
                max /= 1000;
                ss << '.' << std::setw(3) << (dura / max);
            }
            ss << std::flush;
            stream << ss.str();
        } else {
            stream << dura;
        }
        return stream;
    }

    std::ostream& operator<<(std::ostream& stream, StopWatch sw) {
        return stream << hrc_duration(sw.duration());
    }

}

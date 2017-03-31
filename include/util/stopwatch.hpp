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

#ifndef STOPWATCH_HPP__
#define STOPWATCH_HPP__

#include <iostream>
#include <iomanip>
#include <chrono>

namespace v2 {

    class StopWatch {

    public:
        typedef typename std::chrono::high_resolution_clock::time_point time_point;
        typedef typename std::chrono::high_resolution_clock::rep rep;

    private:
        time_point startNS, stopNS;
        rep totalNS;

        StopWatch(time_point startNS, time_point stopNS, rep totalNS);

    public:
        StopWatch();

        void start();
        void resume();
        rep stop();
        rep duration();

        friend StopWatch operator+(StopWatch lhs, const StopWatch & rhs) {
            return StopWatch(lhs.startNS, time_point(std::chrono::high_resolution_clock::duration(lhs.stopNS.time_since_epoch().count() + rhs.totalNS)), lhs.totalNS + rhs.totalNS);
        }
    };

    typedef struct hrc_duration {

        StopWatch::rep dura;

        hrc_duration(StopWatch::rep dura);
    } hrc_duration;

    std::ostream& operator<<(std::ostream& stream, hrc_duration hrcd);
    std::ostream& operator<<(std::ostream& stream, StopWatch sw);

}

#endif // STOPWATCH_HPP__

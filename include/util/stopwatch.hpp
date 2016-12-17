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

#ifndef STOPWATCH_HPP__
#define STOPWATCH_HPP__

#include <iostream>
#include <iomanip>
#include <chrono>

using namespace std;
using namespace std::chrono;

class StopWatch {

public:
    typedef typename high_resolution_clock::time_point time_point;
    typedef typename high_resolution_clock::rep rep;

private:
    time_point startNS, stopNS;
    rep totalNS;

    StopWatch (time_point startNS, time_point stopNS, rep totalNS);

public:
    StopWatch ();

    void start ();
    void resume ();
    rep stop ();
    rep duration ();

    friend StopWatch operator+ (StopWatch lhs, const StopWatch & rhs) {
        return StopWatch(lhs.startNS, time_point(high_resolution_clock::duration(lhs.stopNS.time_since_epoch().count() + rhs.totalNS)), lhs.totalNS + rhs.totalNS);
    }
};

typedef struct hrc_duration {

    StopWatch::rep dura;

    hrc_duration (StopWatch::rep dura);
} hrc_duration;

ostream& operator<< (ostream& stream, hrc_duration hrcd);
ostream& operator<< (ostream& stream, StopWatch sw);


#endif // STOPWATCH_HPP__

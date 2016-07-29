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

#include "util/stopwatch.hpp"

using namespace std;
using namespace std::chrono;

const bool OUTPUT_INSERT_DOT = true;

StopWatch::StopWatch() {
}

void StopWatch::start() {
    startNS = high_resolution_clock::now();
}

high_resolution_clock::rep StopWatch::stop() {
    stopNS = high_resolution_clock::now();
    return duration();
}

high_resolution_clock::rep StopWatch::duration() {
    return duration_cast<nanoseconds>(stopNS - startNS).count();
}

hrc_duration::hrc_duration(high_resolution_clock::rep dura)
: dura(dura) {
}

ostream& operator<<(ostream& stream, hrc_duration hrcd) {
    high_resolution_clock::rep dura = hrcd.dura;
    stringstream ss;
    if (OUTPUT_INSERT_DOT) {
        size_t max = 1000;
        while (dura / max > 0) {
            max *= 1000;
        }
        max /= 1000;
        ss << setfill('0') << (dura / max);
        while (max > 1) {
            dura %= max;
            max /= 1000;
            ss << '.' << setw(3) << (dura / max);
        }
        ss << flush;
        stream << ss.str();
    } else {
        stream << dura;
    }
    return stream;
}

ostream& operator<<(ostream& stream, StopWatch& sw) {
    return stream << hrc_duration(sw.duration());
}

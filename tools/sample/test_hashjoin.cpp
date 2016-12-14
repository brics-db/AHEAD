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

/* 
 * File:   test_hashjoin.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 13. August 2016, 22:58
 */

#include "../ssbm/ssbm.hpp"

/*
 * 
 */
int
main (int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    StopWatch sw1;
    StopWatch::rep totalTime = 0;

    if (CONFIG.VERBOSE) {
        std::cout << "test_hashjoin\n=============" << std::endl;
    }

    boost::filesystem::path p(CONFIG.DB_PATH);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    sw1.start();
    loadTable(baseDir, "date", CONFIG);
    loadTable(baseDir, "lineorder", CONFIG);
    sw1.stop();

    if (CONFIG.VERBOSE) {
        std::cout << "Total loading time: " << sw1 << " ns." << std::endl;
    }

    auto cbDateDatekey = new int_colbat_t("date", "datekey");
    auto cbLineorderOrderdate = new int_colbat_t("lineorder", "orderdate");

    auto tbDateDatekey = cbDateDatekey->reverse();
    auto tbLineorderOrderdate = v2::bat::ops::copy(cbLineorderOrderdate);

    std::cout << tbDateDatekey->size() << '\t' << tbLineorderOrderdate->size() << std::endl;

    delete cbDateDatekey;
    delete cbLineorderOrderdate;

    if (CONFIG.VERBOSE) {
        std::cout << "<DEPRECATED> !!! Measuring " << CONFIG.NUM_RUNS << " runs. Times are in [ns]. Joining Lineorder.orderdate with Date.datekey:" << std::endl;
    }

    totalTime = 0;
    if (CONFIG.VERBOSE) {
        std::cout << "col_hashjoin_old (only new implementation available any longer!):" << std::endl;
    }
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        auto result = v2::bat::ops::hashjoin(tbLineorderOrderdate, tbDateDatekey, hash_side_t::right);
        totalTime += sw1.stop();
        if (CONFIG.VERBOSE) {
            std::cout << (i + 1) << '\t' << sw1.duration() << '\t' << result->size() << std::endl;
        }
        delete result;
    }

    std::cout << "average\t" << (totalTime / CONFIG.NUM_RUNS) << std::endl;

    totalTime = 0;
    if (CONFIG.VERBOSE) {
        std::cout << "col_hashjoin_new:" << std::endl;
    }
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        auto result = v2::bat::ops::hashjoin(tbLineorderOrderdate, tbDateDatekey, hash_side_t::right);
        totalTime += sw1.stop();
        if (CONFIG.VERBOSE) {
            std::cout << (i + 1) << '\t' << sw1.duration() << '\t' << result->size() << std::endl;
        }
        delete result;
    }
    std::cout << "average\t" << (totalTime / CONFIG.NUM_RUNS) << std::endl;

    delete tbDateDatekey;
    delete tbLineorderOrderdate;

    return 0;
}


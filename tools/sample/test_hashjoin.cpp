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
 * File:   test_hashjoin.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 13. August 2016, 22:58
 */

#include "../ssbm/ssbm.hpp"

/*
 * 
 */
int main(int argc, char** argv) {
    ssbmconf_t CONFIG(argc, argv);
    StopWatch sw1;
    StopWatch::rep totalTime = 0;

    if (CONFIG.VERBOSE) {
        std::cout << "test_hashjoin\n=============" << std::endl;
    }

    MetaRepositoryManager::init(CONFIG.DB_PATH.c_str());

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
    auto tbLineorderOrderdate = ahead::bat::ops::copy(cbLineorderOrderdate);

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
        auto result = ahead::bat::ops::hashjoin(tbLineorderOrderdate, tbDateDatekey, hash_side_t::right);
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
        auto result = ahead::bat::ops::hashjoin(tbLineorderOrderdate, tbDateDatekey, hash_side_t::right);
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


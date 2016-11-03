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
 * File:   test_group.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 3. November 2016, 16:40
 */

#include "../ssbm/ssbm.hpp"

/**
 * Group-By the tail
 * Returns 2 BAT's:
 * 1) Mapping (V)OID -> GroupID
 * 2) GroupID -> Value
 */
template<typename Head, typename Tail>
pair<Bat<Head, v2_oid_t>*, Bat<v2_void_t, Tail>*> group(Bat<Head, Tail>* bat) {
    auto mapHeadtoGID = new TempBat<Head, v2_oid_t>();
    auto mapGIDtoTail = new TempBat<v2_void_t, Tail>();
    oid_t nextGID = 0;
    auto iter = bat->begin();
    for (; iter->hasNext(); ++*iter) {
        typename Tail::type_t curTail = iter->tail();
        // search this tail in our mapping
        // idx is the void value of mapGIDtoTail, which starts at zero
        size_t curGID = 0;
        bool found = false;
        for (auto itertail = mapGIDtoTail->tail.container->begin(); itertail != mapGIDtoTail->tail.container->end(); ++itertail) {
            if (curTail == *itertail) {
                found = true;
                break;
            }
            ++curGID;
        }
        if (found) {
        } else {
            // new group!
            mapGIDtoTail->append(curTail);
            mapHeadtoGID->append(make_pair(iter->head(), curTail));
            ++nextGID;
        }
    }
    delete iter;
    return make_pair(mapHeadtoGID, mapGIDtoTail);
}

/**
 * 
 * @param bat1 The bat over which to sum up
 * @param bat2 The first grouping BAT
 * @param bat3 The second grouping BAT
 * @return Three BATs: 1) sum over double groups. 2) Mapping sumID -> group1. 3) Mapping sumID -> group2.
 */
template<typename V2Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename Head3, typename Tail3>
tuple<Bat<v2_void_t, V2Result>*, Bat<v2_void_t, v2_oid_t>*, Bat<v2_void_t, v2_oid_t>*> groupedSum(Bat<Head1, Tail1>* bat1, Bat<Head2, Tail2>* bat2, Bat<Head3, Tail3>* bat3) {
}

/*
 * Testing with scale factor 1, the following is the expected result (from MonetDB):
 * +------+--------+
 * | L1   | d_year |
 * +======+========+
 * |  366 |   1992 |
 * |  365 |   1993 |
 * |  365 |   1994 |
 * |  365 |   1995 |
 * |  366 |   1996 |
 * |  365 |   1997 |
 * |  364 |   1998 |
 * +------+--------+
 */

/**
 * main function
 * 
 * @param argc
 * @param argv
 * @return 
 */
int main(int argc, char** argv) {
    ssbmconf_t CONFIG = initSSBM(argc, argv);
    StopWatch sw1;
    StopWatch::rep totalTime = 0;
    StopWatch::rep *allTimes = new StopWatch::rep[CONFIG.NUM_RUNS];

    cout << "test_hashjoin\n=============" << endl;

    boost::filesystem::path p(CONFIG.DB_PATH);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    sw1.start();
    loadTable(baseDir, "date");
    sw1.stop();
    cout << "Total loading time: " << sw1 << " ns." << endl;

    auto cbYear = new shortint_colbat_t("date", "year");

    totalTime = 0;
    cout << "group date.d_year:" << endl;
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        auto result = group(cbYear);
        allTimes[i] = sw1.stop();
        totalTime += allTimes[i];
        cout << (i + 1) << '\t' << sw1.duration() << '\t' << result.first->size() << '\t' << result.second->size() << endl;
        cout << "Result:\n";
        auto iter = result.second->begin();
        for (; iter->hasNext(); ++*iter) {
            cout << setw(15) << iter->head() << " | " << setw(15) << iter->tail() << '\n';
        }
        cout << endl;
        delete iter;
        delete result.first;
        delete result.second;
    }
    cout << "Times:\n";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        cout << (i + 1) << ":\t" << allTimes[i] << '\n';
    }
    cout << "average\t" << (totalTime / CONFIG.NUM_RUNS) << endl;

    delete allTimes;
    delete cbYear;

    return 0;

}

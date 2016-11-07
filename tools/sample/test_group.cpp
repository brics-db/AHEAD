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
    loadTable(baseDir, "lineorder");
    loadTable(baseDir, "part");
    sw1.stop();
    cout << "Total loading time: " << sw1 << " ns." << endl;

    auto cbD_Y = new shortint_colbat_t("date", "year");
    auto tbD_Y = v2::bat::ops::copy(cbD_Y);

    totalTime = 0;
    cout << "group date.d_year:" << endl;
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        auto result = v2::bat::ops::group(tbD_Y);
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

    cout << "select sum(lo_revenue), d_year, p_brand\n";
    cout << "  from lineorder, date, part\n";
    cout << "  where lo_orderdate = d_datekey\n";
    cout << "    and lo_partkey = p_partkey\n";
    cout << "  group by d_year, p_brand" << endl;

    sw1.start();
    auto cbLO_OD = new int_colbat_t("lineorder", "orderdate");
    auto cbLO_R = new int_colbat_t("lineorder", "revenue");
    auto cbLO_PK = new int_colbat_t("lineorder", "partkey");
    auto cbD_DK = new int_colbat_t("date", "datekey");
    auto cbP_PK = new int_colbat_t("part", "partkey");
    auto cbP_B = new str_colbat_t("part", "brand");
    auto tbLO_OD = v2::bat::ops::copy(cbLO_OD);
    auto tbLO_R = v2::bat::ops::copy(cbLO_R);
    auto tbLO_PK = v2::bat::ops::copy(cbLO_PK);
    auto tbD_DK = v2::bat::ops::copy(cbD_DK);
    auto tbP_PK = v2::bat::ops::copy(cbP_PK);
    auto tbP_B = v2::bat::ops::copy(cbP_B);
    sw1.stop();
    cout << "Copying the 6 columns took " << sw1 << " ns." << endl;
    totalTime = 0;
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        auto bat1 = tbD_DK->reverse();
        auto bat2 = v2::bat::ops::hashjoin(tbLO_OD, bat1);
        auto bat3 = tbP_PK->reverse();
        auto bat4 = v2::bat::ops::hashjoin(tbLO_PK, bat3);
        auto bat5 = v2::bat::ops::hashjoin(bat2, tbD_Y);
        auto bat6 = v2::bat::ops::hashjoin(bat4, tbP_B);
        auto tuple = v2::bat::ops::groupedSum<v2_bigint_t>(tbLO_R, bat5, bat6);
        allTimes[i] = sw1.stop();
        totalTime += allTimes[i];
        size_t numSums = get<0>(tuple)->size();
        cout << numSums << endl;
        if (i == 0) {
            auto iter1 = get<0>(tuple)->begin();
            auto iter2 = get<1>(tuple)->begin();
            auto iter3 = get<2>(tuple)->begin();
            auto iter4 = get<3>(tuple)->begin();
            auto iter5 = get<4>(tuple)->begin();
            cout << '\n';
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter4) {
                iter3->position(0);
                iter5->position(0);
                cout << setw(15) << iter1->tail() << " | ";
                iter3->position(iter2->tail());
                cout << iter3->tail() << " | ";
                iter5->position(iter4->tail());
                cout << iter5->tail() << '\n';
            }
            delete iter1;
            delete iter2;
            delete iter3;
            delete iter4;
            delete iter5;
        }
        delete bat1;
        delete bat2;
        delete bat3;
        delete bat4;
        delete bat5;
        delete bat6;
        delete get<0>(tuple);
        delete get<1>(tuple);
        delete get<2>(tuple);
        delete get<3>(tuple);
        delete get<4>(tuple);
    }
    cout << "Times:\n";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        cout << (i + 1) << ":\t" << allTimes[i] << '\n';
    }
    cout << "average\t" << (totalTime / CONFIG.NUM_RUNS) << endl;

    delete allTimes;
    delete cbD_Y;
    delete tbD_Y;
    delete cbLO_OD;
    delete tbLO_OD;
    delete cbLO_R;
    delete tbLO_R;
    delete cbLO_PK;
    delete tbLO_PK;
    delete cbD_DK;
    delete tbD_DK;
    delete cbP_PK;
    delete tbP_PK;
    delete cbP_B;
    delete tbP_B;

    return 0;

}

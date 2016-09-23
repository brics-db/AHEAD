/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   test_hashjoin.cpp
 * Author: tk4
 *
 * Created on 13. August 2016, 22:58
 */

#include "../ssbm/ssbm.hpp"

/*
 * 
 */
int main(int argc, char** argv) {
    cout << "test_hashjoin\n=============" << endl;

    boost::filesystem::path p(argc == 1 ? argv[0] : argv[1]);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    StopWatch sw1;

    sw1.start();
    loadTable(baseDir, "date");
    loadTable(baseDir, "lineorder");
    sw1.stop();
    cout << "Total loading time: " << sw1 << " ns." << endl;

    auto cbDateDatekey = new int_colbat_t("date", "datekey");
    auto cbLineorderOrderdate = new int_colbat_t("lineorder", "orderdate");

    auto tbDateDatekey = cbDateDatekey->reverse();
    auto tbLineorderOrderdate = v2::bat::ops::copy(cbLineorderOrderdate);

    cout << tbDateDatekey->size() << '\t' << tbLineorderOrderdate->size() << endl;

    delete cbDateDatekey;
    delete cbLineorderOrderdate;

    const size_t NUM_RUNS = 10;
    StopWatch::rep totalTime = 0;

    cout << "<DEPRECATED> !!! Measuring " << NUM_RUNS << " runs. Times are in [ns]. Joining Lineorder.orderdate with Date.datekey:" << endl;

    totalTime = 0;
    cout << "col_hashjoin_old (only new implementation available any longer!):" << endl;
    for (size_t i = 0; i < NUM_RUNS; ++i) {
        sw1.start();
        auto result = v2::bat::ops::hashjoin(tbLineorderOrderdate, tbDateDatekey, join_side_t::right);
        totalTime += sw1.stop();
        cout << (i + 1) << '\t' << sw1.duration() << '\t' << result->size() << endl;
        delete result;
    }
    cout << "average\t" << (totalTime / NUM_RUNS) << endl;

    totalTime = 0;
    cout << "col_hashjoin_new:" << endl;
    for (size_t i = 0; i < NUM_RUNS; ++i) {
        sw1.start();
        auto result = v2::bat::ops::hashjoin(tbLineorderOrderdate, tbDateDatekey, join_side_t::right);
        totalTime += sw1.stop();
        cout << (i + 1) << '\t' << sw1.duration() << '\t' << result->size() << endl;
        delete result;
    }
    cout << "average\t" << (totalTime / NUM_RUNS) << endl;

    delete tbDateDatekey;
    delete tbLineorderOrderdate;

    return 0;
}


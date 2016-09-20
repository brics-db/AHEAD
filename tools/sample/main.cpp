#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>

#include <boost/filesystem.hpp>

#include <column_storage/ColumnBat.h>
#include <column_storage/TransactionManager.h>
#include <column_operators/operators.h>
#include <column_operators/operatorsAN.tcc>
#include <util/rss.hpp>
#include <util/stopwatch.hpp>

using namespace std;

int main(int argc, char ** argv) {
    boost::filesystem::path p(argc == 1 ? argv[0] : argv[1]);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    // create a transaction manager
    TransactionManager* tm = TransactionManager::getInstance();
    TransactionManager::Transaction* t;

    StopWatch sw;

    // import the data	
    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw.start();
    string path("/home/tk4/git/columnstore/database/customer");
    size_t numCust = t->load(path.c_str(), "customer");
    sw.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << numCust << "\n\tTime: " << sw << " ns." << endl;
    tm->endTransaction(t);

    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw.start();
    path = "/home/tk4/git/columnstore/database/customer2";
    size_t numCust2 = t->load(path.c_str(), "customer2");
    sw.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << numCust2 << "\n\tTime: " << sw << " ns." << endl;
    tm->endTransaction(t);

    /*
    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw.start();
    path = "/home/tk4/git/columnstore/database/sdate";
    size_t numDate = t->load(path.c_str(), "sdate");
    sw.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << numDate << "\n\tTime: " << sw << " ns." << endl;
    tm->endTransaction(t);
     */

    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw.start();
    path = "/home/tk4/git/columnstore/database/lineorder";
    size_t numLineOrder = t->load(path.c_str(), "lineorder");
    sw.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << numLineOrder << "\n\tTime: " << sw << " ns." << endl;
    tm->endTransaction(t);

    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw.start();
    path = "/home/tk4/git/columnstore/database/lineorder2";
    size_t numLineOrder2 = t->load(path.c_str(), "lineorder2");
    sw.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << numLineOrder2 << "\n\tTime: " << sw << " ns." << endl;
    tm->endTransaction(t);

    // Test Query
    auto batCustKey = new int_colbat_t("customer", "custkey");
    sw.start();
    auto bat0 = v2::bat::ops::selection_lt(batCustKey, static_cast<int_t> (12345));
    sw.stop();
    cout << "[customer] Selection over " << numCust << " tuples took " << sw << " ns" << endl;

    /*
    auto batLineOrderKey = new ColumnBat<int_t>("lineorder", "orderkey");
    sw.start();
    __attribute__((unused)) auto bat1 = v2::bat::ops::selection_lt(batLineOrderKey, static_cast<int_t> (123450));
    sw.stop();
    cout << "[lineorder] Selection over " << numLineOrder << " tuples took " << sw << " ns" << endl;
     */

    sw.start();
    auto batA = v2::bat::ops::encode_AN(batCustKey);
    sw.stop();
    cout << "[customer] Converted " << numCust << " tuples from int_t to resint_t took " << sw << " ns" << endl;

    const size_t max = 10;
    cout << "Customer::custkey (int_t) top " << max << ":\n";
    auto iter = bat0->begin();
    for (size_t i = 0; iter->hasNext() && i < max; ++i, ++iter) {
        cout << iter->tail() << '\n';
    }
    cout << endl;
    delete iter;

    cout << "Customer::custkey (resint_t) top " << max << ":\n";
    auto iter2 = batA->begin();
    for (size_t i = 0; iter2->hasNext() && i < max; ++i, ++iter2) {
        cout << iter2->tail() << '\n';
    }
    cout << endl;
    delete iter2;

    cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB." << endl;

    TransactionManager::destroyInstance();

    return 0;
}


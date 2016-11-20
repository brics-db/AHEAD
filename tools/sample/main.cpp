#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>

#include <boost/filesystem.hpp>

#include <column_storage/ColumnBat.h>
#include <column_storage/TransactionManager.h>
#include <column_operators/Operators.h>
#include <column_operators/OperatorsAN.tcc>
#include <util/rss.hpp>
#include <util/stopwatch.hpp>

#include "../ssbm/ssbm.hpp"

using namespace std;

int main(int argc, char ** argv) {
    ssbmconf_t CONFIG(argc, argv);

    boost::filesystem::path p(CONFIG.DB_PATH);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    StopWatch sw;

    // import the data
    loadTable(baseDir, "customer", CONFIG);
    loadTable(baseDir, "date", CONFIG);
    loadTable(baseDir, "lineorder", CONFIG);

    // Test Query
    int_colbat_t batCustKey("customer", "custkey");
    sw.start();
    auto bat0 = v2::bat::ops::select<less>(&batCustKey, static_cast<int_t> (12345));
    sw.stop();
    cout << "[customer] Selection over " << batCustKey.size() << " tuples took " << sw << " ns" << endl;

    int_colbat_t batLineOrderKey("lineorder", "orderkey");
    sw.start();
    auto bat1 = v2::bat::ops::select<std::less>(&batLineOrderKey, static_cast<int_t> (123450));
    sw.stop();
    cout << "[lineorder] Selection over " << batLineOrderKey.size() << " tuples took " << sw << " ns" << endl;

    sw.start();
    auto batA = v2::bat::ops::encodeAN(&batCustKey);
    sw.stop();
    cout << "[customer] Converted " << batCustKey.size() << " tuples from int_t to resint_t took " << sw << " ns" << endl;

    const size_t max = 10;
    cout << "Customer::custkey (int_t) top " << max << ":\n";
    auto iter = bat0->begin();
    for (size_t i = 0; iter->hasNext() && i < max; ++i, ++iter) {
        cout << iter->tail() << '\n';
    }
    cout << endl;
    delete iter;

    delete bat0;
    delete bat1;

    cout << "Customer::custkey (resint_t) top " << max << ":\n";
    auto iter2 = batA->begin();
    for (size_t i = 0; iter2->hasNext() && i < max; ++i, ++iter2) {
        cout << iter2->tail() << '\n';
    }
    cout << endl;
    delete iter2;

    delete batA;

    cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB." << endl;

    TransactionManager::destroyInstance();

    return 0;
}


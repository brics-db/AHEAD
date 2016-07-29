#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>

#include <boost/filesystem.hpp>

#include "column_storage/TempBat.h"
#include "column_storage/ColumnBat.h"
#include "column_storage/ColumnManager.h"
#include "column_storage/TransactionManager.h"
#include "column_operators/operators.h"
#include "column_storage/types.h"
#include "query_executor/executor.h"
#include "query_executor/codeHandler.h"
#include "util/stopwatch.hpp"

#include <unistd.h>

using namespace std;
using namespace llvm;
using namespace __gnu_cxx;

int main(int argc, char ** argv) {
    boost::filesystem::path p(argc == 1 ? argv[0] : argv[1]);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    // create a column manager
    ColumnManager *cm = ColumnManager::getInstance();

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
    path = "/home/tk4/git/columnstore/database/sdate";
    size_t numDate = t->load(path.c_str(), "sdate");
    sw.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << numDate << "\n\tTime: " << sw << " ns." << endl;
    tm->endTransaction(t);

    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw.start();
    path = "/home/tk4/git/columnstore/database/lineorder";
    size_t numLineOrder = t->load(path.c_str(), "lineorder");
    sw.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << numLineOrder << "\n\tTime: " << sw << " ns." << endl;
    tm->endTransaction(t);

    // Test Query
    auto batCustKey = new ColumnBat<unsigned, int_t>("customer", "custkey");
    auto z = new Bat_Operators();
    sw.start();
    __attribute__((unused)) auto bat0 = z->selection_lt(batCustKey, static_cast<int_t> (12345));
    sw.stop();
    cout << "[customer] Selection over " << numCust << " tuples took " << sw << " ns" << endl;

    auto batLineOrderKey = new ColumnBat<unsigned, int_t>("lineorder", "orderkey");
    sw.start();
    __attribute__((unused)) auto bat1 = z->selection_lt(batLineOrderKey, static_cast<int_t> (123450));
    sw.stop();
    cout << "[lineorder] Selection over " << numLineOrder << " tuples took " << sw << " ns" << endl;

    /*
    cout << "Customer::custkey (int_t):\n";
    for (auto iter = bat0->begin(); iter->hasNext();) {
        auto data = iter->next();
        cout << data.second << '\n';
    }
    cout << endl;
     */

    return 0;

}


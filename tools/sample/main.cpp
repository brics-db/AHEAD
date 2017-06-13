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

#include "../ssbm/ssb.hpp"

int main(
        int argc,
        char ** argv) {
    SSB_CONF CONFIG(argc, argv);

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
    auto bat0 = ahead::bat::ops::select < less > (&batCustKey, static_cast<int_t>(12345));
    sw.stop();
    cout << "[customer] Selection over " << batCustKey.size() << " tuples took " << sw << " ns" << endl;

    int_colbat_t batLineOrderKey("lineorder", "orderkey");
    sw.start();
    auto bat1 = ahead::bat::ops::select < std::less > (&batLineOrderKey, static_cast<int_t>(123450));
    sw.stop();
    cout << "[lineorder] Selection over " << batLineOrderKey.size() << " tuples took " << sw << " ns" << endl;

    sw.start();
    auto batA = ahead::bat::ops::encodeAN(&batCustKey);
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


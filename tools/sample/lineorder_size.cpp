/* 
 * File:   ssbm-q01.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. August 2016, 12:20
 */

#include "ssbm.hpp"

int main(int argc, char** argv) {
    cout << "lineorder_size\n==============" << endl;

    boost::filesystem::path p(argc == 1 ? argv[0] : argv[1]);
    if (boost::filesystem::is_regular(p)) {
        p.remove_filename();
    }
    string baseDir = p.remove_trailing_separator().generic_string();
    MetaRepositoryManager::init(baseDir.c_str());

    TransactionManager* tm = TransactionManager::getInstance();
    TransactionManager::Transaction* t;
    StopWatch sw1, sw2;
    string path;
    size_t num;

    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw2.start();
    path = baseDir;
    path += "/lineorder2"; // header file only contains the first column definition
    num = t->load(path.c_str(), "lineorder");
    sw2.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << num << "\n\tTime: " << sw2 << " ns." << endl;
    tm->endTransaction(t);

    t = tm->beginTransaction(true);
    assert(t != nullptr);
    sw2.start();
    path = baseDir;
    path += "/lineorder2AN"; // header file only contains the first column definition
    num = t->load(path.c_str(), "lineorderAN");
    sw2.stop();
    cout << "File: " << path << "\n\tNumber of BUNs: " << num << "\n\tTime: " << sw2 << " ns." << endl;
    tm->endTransaction(t);

    const size_t NUM_OPS = 17;
    nanoseconds::rep opTimes[NUM_OPS];
    size_t batSizes[NUM_OPS];
    size_t batConsumptions[NUM_OPS];
    bool hasTwoTypes[NUM_OPS];
    boost::typeindex::type_index headTypes[NUM_OPS];
    boost::typeindex::type_index tailTypes[NUM_OPS];

    const size_t LEN_TYPES = 13;
    string emptyString;

    // orderkey|linenumber|custkey|partkey|suppkey|orderdate|orderpriority|shippriority|quantity|extendedprice|ordertotalprice|discount|revenue|supplycost|tax|commitdate|shipmode
    // INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|STRING|STRING|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|INTEGER|STRING
    // RESINT|RESINT|RESINT|RESINT|RESINT|RESINT|STRING|STRING|RESINT|RESINT|RESINT|RESINT|RESINT|RESINT|RESINT|RESINT|STRING

    size_t x = 0;
    MEASURE_OP(sw1, x, batOKbcOrg, new int_col_t("lineorder", "orderkey"));
    x++;
    MEASURE_OP(sw1, x, batOKbcEnc, new resint_col_t("lineorderAN", "orderkey"));
    x++;
    MEASURE_OP(sw1, x, batOKtcEnc, v2::bat::ops::copy(batOKbcEnc));
    x++;
    MEASURE_OP(sw1, x, batOKtcEnc2, v2::bat::ops::copy(batOKtcEnc));
    x++;
    delete batOKtcEnc2;
    cout << "\n\t  name\t" << setw(13) << "time [ns]\t" << setw(10) << "size [#]\t" << setw(10) << "consum [B]\t" << setw(LEN_TYPES) << "type head\t" << setw(LEN_TYPES) << "type tail";
    for (size_t i = 0; i < x; ++i) {
        cout << "\n\t  op" << setw(2) << i << "\t" << setw(13) << hrc_duration(opTimes[i]) << "\t" << setw(10) << batSizes[i] << "\t" << setw(10) << batConsumptions[i] << "\t" << setw(LEN_TYPES) << headTypes[i].pretty_name() << "\t" << setw(LEN_TYPES) << (hasTwoTypes[i] ? tailTypes[i].pretty_name() : emptyString);
    }
    cout << "\npeak RSS: " << getPeakRSS(size_enum_t::MB) << " MB.\n" << endl;
    cout << " num |         check |        decode |  check+decode\n";
    cout << "-----+---------------+---------------+--------------" << endl;

    for (size_t i = 0; i < 1; ++i) {
        sw1.start();
        auto result1 = v2::bat::ops::checkA(batOKtcEnc, ::A_INT_INV, ::A_INT_UNENC_MAX_U);
        sw1.stop();
        cout << "  " << setw(2) << i << "   " << setw(13) << sw1.duration();

        delete result1;

        sw1.start();
        auto result2 = v2::bat::ops::decodeA<int_t>(batOKtcEnc, ::A_INT_INV, ::A_INT_UNENC_MAX_U);
        sw1.stop();
        cout << "   " << setw(13) << sw1.duration();

        delete result2;

        sw1.start();
        auto result3 = v2::bat::ops::checkAndDecodeA<int_t>(batOKtcEnc, ::A_INT_INV, ::A_INT_UNENC_MAX_U);
        sw1.stop();
        cout << "   " << setw(13) << sw1.duration() << '\n';

        delete result3.first;
        delete result3.second;
    }

    delete batOKbcOrg;
    delete batOKbcEnc;
    delete batOKtcEnc;

    return 0;
}

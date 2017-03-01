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
    ssbmconf_t CONFIG(argc, argv);
    StopWatch sw1;
    StopWatch::rep totalTime = 0;
    StopWatch::rep *allTimes = new StopWatch::rep[CONFIG.NUM_RUNS];

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
    loadTable(baseDir, "part", CONFIG);
    sw1.stop();

    if (CONFIG.VERBOSE) {
        std::cout << "Total loading time: " << sw1 << " ns." << std::endl;
    }

    auto cbD_Y = new shortint_colbat_t("date", "year");
    auto tbD_Y = v2::bat::ops::copy(cbD_Y);

    totalTime = 0;
    std::cout << "group date.d_year:" << std::endl;
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        auto result = v2::bat::ops::groupby(tbD_Y);
        allTimes[i] = sw1.stop();
        totalTime += allTimes[i];
        if (i == 0) {
            if (CONFIG.VERBOSE) {
                std::cout << (i + 1) << '\t' << sw1.duration() << '\t' << result.first->size() << '\t' << result.second->size() << std::endl;
            }
            std::cout << "Result:\n";
            auto iter = result.second->begin();
            for (; iter->hasNext(); ++*iter) {
                std::cout << setw(15) << iter->head() << " | " << setw(15) << iter->tail() << '\n';
            }
            std::cout << std::endl;
            delete iter;
        }
        delete result.first;
        delete result.second;
    }
    std::cout << "Times:\n";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << (i + 1) << ":\t" << allTimes[i] << '\n';
    }
    std::cout << "average\t" << (totalTime / CONFIG.NUM_RUNS) << std::endl;

    auto cbP_B = new str_colbat_t("part", "brand");
    auto tbP_B = v2::bat::ops::copy(cbP_B);

    totalTime = 0;
    std::cout << "group part.p_brand:" << std::endl;
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        sw1.start();
        auto result = v2::bat::ops::groupby(tbP_B);
        allTimes[i] = sw1.stop();
        totalTime += allTimes[i];
        if (i == 0) {
            if (CONFIG.VERBOSE) {
                std::cout << (i + 1) << '\t' << sw1.duration() << '\t' << result.first->size() << '\t' << result.second->size() << std::endl;
            }
            if (CONFIG.NUM_RUNS == 10000) {
                std::cout << "Result:\n";
                auto iter = result.second->begin();
                for (; iter->hasNext(); ++*iter) {
                    std::cout << setw(15) << iter->head() << " | " << setw(15) << iter->tail() << '\n';
                }
                std::cout << std::endl;
                delete iter;
            }
        }
        delete result.first;
        delete result.second;
    }
    std::cout << "Times:\n";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << (i + 1) << ":\t" << allTimes[i] << '\n';
    }
    std::cout << "average\t" << (totalTime / CONFIG.NUM_RUNS) << std::endl;

    // select sum(lo_revenue), d_year, p_brand from lineorder, date, part where lo_orderdate = d_datekey and lo_partkey = p_partkey group by d_year, p_brand
    std::cout << "select sum(lo_revenue), d_year, p_brand\n";
    std::cout << "  from lineorder, date, part\n";
    std::cout << "  where lo_orderdate = d_datekey\n";
    std::cout << "    and lo_partkey = p_partkey\n";
    std::cout << "  group by d_year, p_brand" << std::endl;

    auto cbLO_OD = new int_colbat_t("lineorder", "orderdate");
    auto cbLO_R = new int_colbat_t("lineorder", "revenue");
    auto cbLO_PK = new int_colbat_t("lineorder", "partkey");
    auto cbD_DK = new int_colbat_t("date", "datekey");
    auto cbP_PK = new int_colbat_t("part", "partkey");
    auto tbLO_OD = v2::bat::ops::copy(cbLO_OD);
    auto tbLO_R = v2::bat::ops::copy(cbLO_R);
    auto tbLO_PK = v2::bat::ops::copy(cbLO_PK);
    auto tbD_DK = v2::bat::ops::copy(cbD_DK);
    auto tbP_PK = v2::bat::ops::copy(cbP_PK);

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
        std::cout << get<0>(tuple)->size() << " " << get<1>(tuple)->size() << " " << get<2>(tuple)->size() << " " << get<3>(tuple)->size() << " " << get<4>(tuple)->size() << std::endl;
        if (i == 0) {
            auto iter1 = get<0>(tuple)->begin();
            auto iter2 = get<1>(tuple)->begin();
            auto iter3 = get<2>(tuple)->begin();
            auto iter4 = get<3>(tuple)->begin();
            auto iter5 = get<4>(tuple)->begin();
            std::cout << '\n';
            for (; iter1->hasNext(); ++*iter1, ++*iter2, ++*iter4) {
                std::cout << std::setw(7) << iter1->head() << ':' << std::setw(10) << iter1->tail() << " | ";
                std::cout << std::setw(7) << iter2->head() << ':' << std::setw(10) << iter2->tail() << " | ";
                iter3->position(iter2->tail());
                std::cout << std::setw(7) << iter3->head() << ':' << std::setw(10) << iter3->tail() << " | ";
                std::cout << std::setw(7) << iter4->head() << ':' << std::setw(10) << iter4->tail() << " | ";
                iter5->position(iter4->tail());
                std::cout << std::setw(7) << iter5->head() << ':' << std::setw(10) << iter5->tail() << '\n';
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
    std::cout << "Times:\n";
    for (size_t i = 0; i < CONFIG.NUM_RUNS; ++i) {
        std::cout << (i + 1) << ":\t" << allTimes[i] << '\n';
    }
    std::cout << "average\t" << (totalTime / CONFIG.NUM_RUNS) << std::endl;

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

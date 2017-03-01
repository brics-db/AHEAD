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
 * File:   ssbm-convert.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 1. March 2017, 00:14
 */

#include "ssbm.hpp"

int main(int argc, char** argv) {
    SSBM_REQUIRED_VARIABLES("SSBM Data Converter\n==================", 24, "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K", "L", "M", "N", "O", "P");

    SSBM_LOAD("lineorder", "part", "supplier", "customer", "date", "Convert all table data into column files");

    /* Measure loading ColumnBats */
    MEASURE_OP(cbDate_Year, new shortint_colbat_t("date", "year"));
    MEASURE_OP(cbDate_YearMonthNum, new int_colbat_t("date", "yearmonthnum"));
    MEASURE_OP(cbDate_DateKey, new int_colbat_t("date", "datekey"));
    MEASURE_OP(cbDate_WeekNumInYear, new tinyint_colbat_t("date", "weeknuminyear"));

    MEASURE_OP(cbLineorder_Quantity, new tinyint_colbat_t("lineorder", "quantity"));
    MEASURE_OP(cbLineorder_Discount, new tinyint_colbat_t("lineorder", "discount"));
    MEASURE_OP(cbLineorder_OrderDate, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(cbLineorder_ExtendedPrice, new int_colbat_t("lineorder", "extendedprice"));
    MEASURE_OP(cbLineorder_PartKey, new int_colbat_t("lineorder", "partkey"));
    MEASURE_OP(cbLineorder_SuppKey, new int_colbat_t("lineorder", "suppkey"));
    MEASURE_OP(cbLineorder_Revenue, new int_colbat_t("lineorder", "revenue"));

    MEASURE_OP(cbPart_PartKey, new int_colbat_t("part", "partkey"));
    MEASURE_OP(cbPart_Category, new str_colbat_t("part", "category"));
    MEASURE_OP(cbPart_Brand, new str_colbat_t("part", "brand"));

    MEASURE_OP(cbSupplier_SuppKey, new int_colbat_t("supplier", "suppkey"));
    MEASURE_OP(cbSupplier_Region, new str_colbat_t("supplier", "region"));

    /* Measure converting (copying) ColumnBats to TempBats */
    MEASURE_OP(tbDate_Year, v2::bat::ops::copy(cbDate_Year));
    MEASURE_OP(tbDate_YearMonthNum, v2::bat::ops::copy(cbDate_YearMonthNum));
    MEASURE_OP(tbDate_DateKey, v2::bat::ops::copy(cbDate_DateKey));
    MEASURE_OP(tbDate_WeekNumInYear, v2::bat::ops::copy(cbDate_WeekNumInYear));
    MEASURE_OP(tbLineorder_Quantity, v2::bat::ops::copy(cbLineorder_Quantity));
    MEASURE_OP(tbLineorder_Discount, v2::bat::ops::copy(cbLineorder_Discount));
    MEASURE_OP(tbLineorder_OrderDate, v2::bat::ops::copy(cbLineorder_OrderDate));
    MEASURE_OP(tbLineorder_ExtendedPrice, v2::bat::ops::copy(cbLineorder_ExtendedPrice));
    MEASURE_OP(tbLineorder_PartKey, v2::bat::ops::copy(cbLineorder_PartKey));
    MEASURE_OP(tbLineorder_SuppKey, v2::bat::ops::copy(cbLineorder_SuppKey));
    MEASURE_OP(tbLineorder_Revenue, v2::bat::ops::copy(cbLineorder_Revenue));
    MEASURE_OP(tbPart_PartKey, v2::bat::ops::copy(cbPart_PartKey));
    MEASURE_OP(tbPart_Category, v2::bat::ops::copy(cbPart_Category));
    MEASURE_OP(tbPart_Brand, v2::bat::ops::copy(cbPart_Brand));
    MEASURE_OP(tbSupplier_SuppKey, v2::bat::ops::copy(cbSupplier_SuppKey));
    MEASURE_OP(tbSupplier_Region, v2::bat::ops::copy(cbSupplier_Region));

    delete cbDate_Year;
    delete cbDate_YearMonthNum;
    delete cbDate_DateKey;
    delete cbDate_WeekNumInYear;
    delete cbLineorder_Quantity;
    delete cbLineorder_Discount;
    delete cbLineorder_OrderDate;
    delete cbLineorder_ExtendedPrice;
    delete cbLineorder_PartKey;
    delete cbLineorder_SuppKey;
    delete cbLineorder_Revenue;
    delete cbPart_PartKey;
    delete cbPart_Category;
    delete cbPart_Brand;
    delete cbSupplier_SuppKey;
    delete cbSupplier_Region;

    SSBM_BEFORE_QUERIES;

    SSBM_BEFORE_QUERY;

    SSBM_AFTER_QUERY(0, 0);

    SSBM_AFTER_QUERIES;

    SSBM_FINALIZE;

    return 0;
}

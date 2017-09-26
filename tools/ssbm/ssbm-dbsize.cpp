// Copyright 2017 Till Kolditz
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
 * ssbm-dbsize.cpp
 *
 *  Created on: 26.09.2017
 *      Author: Till Kolditz - Till.Kolditz@gmail.com
 */

#include "ssb.hpp"
#include "macros.hpp"

int main(
        int argc,
        char** argv) {
    ssb::init(argc, argv, "SSBM Database Size Printer");

    ssb::load_tables(std::vector<std::string>( {"customer", "date", "lineorder", "part", "supplier", "customerAN", "dateAN", "lineorderAN", "partAN", "supplierAN"}));

    ///////////////
    // Unencoded //
    ///////////////

    // custkey|name|address|city|nation|region|phone|mktsegment
    // INTEGER|STRING:25|STRING:25|STRING:10|STRING:15|STRING:12|STRING:15|STRING:10
    MEASURE_OP(batC1, new int_colbat_t("customer", "custkey"));
    MEASURE_OP(batC2, new str_colbat_t("customer", "name"));
    MEASURE_OP(batC3, new str_colbat_t("customer", "address"));
    MEASURE_OP(batC4, new str_colbat_t("customer", "city"));
    MEASURE_OP(batC5, new str_colbat_t("customer", "nation"));
    MEASURE_OP(batC6, new str_colbat_t("customer", "region"));
    MEASURE_OP(batC7, new str_colbat_t("customer", "phone"));
    MEASURE_OP(batC8, new str_colbat_t("customer", "mktsegment"));

    // datekey|date|dayofweek|month|year|yearmonthnum|yearmonth|daynuminweek|daynuminmonth|daynuminyear|monthnuminyear|weeknuminyear|sellingseason|lastdayinweekfl|lastdayinmonthfl|holidayfl|weekdayfl
    // INTEGER|STRING:18|STRING:9|STRING:9|SHORTINT|INTEGER|STRING:7|TINYINT|TINYINT|SHORTINT|TINYINT|TINYINT|STRING:12|CHAR|CHAR|CHAR|CHAR
    MEASURE_OP(batD1, new int_colbat_t("date", "datekey"));
    MEASURE_OP(batD2, new str_colbat_t("date", "date"));
    MEASURE_OP(batD3, new str_colbat_t("date", "dayofweek"));
    MEASURE_OP(batD4, new str_colbat_t("date", "month"));
    MEASURE_OP(batD5, new shortint_colbat_t("date", "year"));
    MEASURE_OP(batD6, new int_colbat_t("date", "yearmonthnum"));
    MEASURE_OP(batD7, new str_colbat_t("date", "yearmonth"));
    MEASURE_OP(batD8, new tinyint_colbat_t("date", "daynuminweek"));
    MEASURE_OP(batD9, new tinyint_colbat_t("date", "daynuminmonth"));
    MEASURE_OP(batDA, new shortint_colbat_t("date", "daynuminyear"));
    MEASURE_OP(batDB, new tinyint_colbat_t("date", "monthnuminyear"));
    MEASURE_OP(batDC, new tinyint_colbat_t("date", "weeknuminyear"));
    MEASURE_OP(batDD, new str_colbat_t("date", "sellingseason"));
    MEASURE_OP(batDE, new char_colbat_t("date", "lastdayinweekfl"));
    MEASURE_OP(batDF, new char_colbat_t("date", "lastdayinmonthfl"));
    MEASURE_OP(batDG, new char_colbat_t("date", "holidayfl"));
    MEASURE_OP(batDH, new char_colbat_t("date", "weekdayfl"));

    // orderkey|linenumber|custkey|partkey|suppkey|orderdate|orderpriority|shippriority|quantity|extendedprice|ordertotalprice|discount|revenue|supplycost|tax|commitdate|shipmode
    // INTEGER|TINYINT|INTEGER|INTEGER|INTEGER|INTEGER|STRING:15|CHAR|TINYINT|INTEGER|INTEGER|TINYINT|INTEGER|INTEGER|TINYINT|INTEGER|STRING:10
    MEASURE_OP(batL1, new int_colbat_t("lineorder", "orderkey"));
    MEASURE_OP(batL2, new tinyint_colbat_t("lineorder", "linenumber"));
    MEASURE_OP(batL3, new int_colbat_t("lineorder", "custkey"));
    MEASURE_OP(batL4, new int_colbat_t("lineorder", "partkey"));
    MEASURE_OP(batL5, new int_colbat_t("lineorder", "suppkey"));
    MEASURE_OP(batL6, new int_colbat_t("lineorder", "orderdate"));
    MEASURE_OP(batL7, new str_colbat_t("lineorder", "orderpriority"));
    MEASURE_OP(batL8, new char_colbat_t("lineorder", "shippriority"));
    MEASURE_OP(batL9, new tinyint_colbat_t("lineorder", "quantity"));
    MEASURE_OP(batLA, new int_colbat_t("lineorder", "extendedprice"));
    MEASURE_OP(batLB, new int_colbat_t("lineorder", "ordertotalprice"));
    MEASURE_OP(batLC, new tinyint_colbat_t("lineorder", "discount"));
    MEASURE_OP(batLD, new int_colbat_t("lineorder", "revenue"));
    MEASURE_OP(batLE, new int_colbat_t("lineorder", "supplycost"));
    MEASURE_OP(batLF, new tinyint_colbat_t("lineorder", "tax"));
    MEASURE_OP(batLG, new int_colbat_t("lineorder", "commitdate"));
    MEASURE_OP(batLH, new str_colbat_t("lineorder", "shipmode"));

    // partkey|name|mfgr|category|brand|color|type|size|container
    // INTEGER|STRING:22|STRING:6|STRING:7|STRING:9|STRING:11|STRING:25|TINYINT|STRING:10
    MEASURE_OP(batP1, new int_colbat_t("part", "partkey"));
    MEASURE_OP(batP2, new str_colbat_t("part", "name"));
    MEASURE_OP(batP3, new str_colbat_t("part", "mfgr"));
    MEASURE_OP(batP4, new str_colbat_t("part", "category"));
    MEASURE_OP(batP5, new str_colbat_t("part", "brand"));
    MEASURE_OP(batP6, new str_colbat_t("part", "color"));
    MEASURE_OP(batP7, new str_colbat_t("part", "type"));
    MEASURE_OP(batP8, new tinyint_colbat_t("part", "size"));
    MEASURE_OP(batP9, new str_colbat_t("part", "container"));

    // suppkey|name|address|city|nation|region|phone
    // INTEGER|STRING:25|STRING:25|STRING:10|STRING:15|STRING:12|STRING:15
    MEASURE_OP(batS1, new int_colbat_t("supplier", "suppkey"));
    MEASURE_OP(batS2, new str_colbat_t("supplier", "name"));
    MEASURE_OP(batS3, new str_colbat_t("supplier", "address"));
    MEASURE_OP(batS4, new str_colbat_t("supplier", "city"));
    MEASURE_OP(batS5, new str_colbat_t("supplier", "nation"));
    MEASURE_OP(batS6, new str_colbat_t("supplier", "region"));
    MEASURE_OP(batS7, new str_colbat_t("supplier", "phone"));

    ////////////////
    // AN-Encoded //
    ////////////////

    // custkey|name|address|city|nation|region|phone|mktsegment
    // RESINT|STRING:25|STRING:25|STRING:10|STRING:15|STRING:12|STRING:15|STRING:10
    MEASURE_OP(batCA1, new resint_colbat_t("customerAN", "custkey"));
    MEASURE_OP(batCA2, new str_colbat_t("customerAN", "name"));
    MEASURE_OP(batCA3, new str_colbat_t("customerAN", "address"));
    MEASURE_OP(batCA4, new str_colbat_t("customerAN", "city"));
    MEASURE_OP(batCA5, new str_colbat_t("customerAN", "nation"));
    MEASURE_OP(batCA6, new str_colbat_t("customerAN", "region"));
    MEASURE_OP(batCA7, new str_colbat_t("customerAN", "phone"));
    MEASURE_OP(batCA8, new str_colbat_t("customerAN", "mktsegment"));

    // datekey|date|dayofweek|month|year|yearmonthnum|yearmonth|daynuminweek|daynuminmonth|daynuminyear|monthnuminyear|weeknuminyear|sellingseason|lastdayinweekfl|lastdayinmonthfl|holidayfl|weekdayfl
    // RESINT|STRING:18|STRING:9|STRING:9|RESSHORT|RESINT|STRING:7|RESTINY|RESTINY|RESSHORT|RESTINY|RESTINY|STRING:12|CHAR|CHAR|CHAR|CHAR
    MEASURE_OP(batDA1, new resint_colbat_t("dateAN", "datekey"));
    MEASURE_OP(batDA2, new str_colbat_t("dateAN", "date"));
    MEASURE_OP(batDA3, new str_colbat_t("dateAN", "dayofweek"));
    MEASURE_OP(batDA4, new str_colbat_t("dateAN", "month"));
    MEASURE_OP(batDA5, new resshort_colbat_t("dateAN", "year"));
    MEASURE_OP(batDA6, new resint_colbat_t("dateAN", "yearmonthnum"));
    MEASURE_OP(batDA7, new str_colbat_t("dateAN", "yearmonth"));
    MEASURE_OP(batDA8, new restiny_colbat_t("dateAN", "daynuminweek"));
    MEASURE_OP(batDA9, new restiny_colbat_t("dateAN", "daynuminmonth"));
    MEASURE_OP(batDAA, new shortint_colbat_t("dateAN", "daynuminyear"));
    MEASURE_OP(batDAB, new restiny_colbat_t("dateAN", "monthnuminyear"));
    MEASURE_OP(batDAC, new restiny_colbat_t("dateAN", "weeknuminyear"));
    MEASURE_OP(batDAD, new str_colbat_t("dateAN", "sellingseason"));
    MEASURE_OP(batDAE, new char_colbat_t("dateAN", "lastdayinweekfl"));
    MEASURE_OP(batDAF, new char_colbat_t("dateAN", "lastdayinmonthfl"));
    MEASURE_OP(batDAG, new char_colbat_t("dateAN", "holidayfl"));
    MEASURE_OP(batDAH, new char_colbat_t("dateAN", "weekdayfl"));

    // orderkey|linenumber|custkey|partkey|suppkey|orderdate|orderpriority|shippriority|quantity|extendedprice|ordertotalprice|discount|revenue|supplycost|tax|commitdate|shipmode
    // INTEGER|TINYINT|INTEGER|INTEGER|INTEGER|INTEGER|STRING:15|CHAR|TINYINT|INTEGER|INTEGER|TINYINT|INTEGER|INTEGER|TINYINT|INTEGER|STRING:10
    MEASURE_OP(batLA1, new resint_colbat_t("lineorderAN", "orderkey"));
    MEASURE_OP(batLA2, new restiny_colbat_t("lineorderAN", "linenumber"));
    MEASURE_OP(batLA3, new resint_colbat_t("lineorderAN", "custkey"));
    MEASURE_OP(batLA4, new resint_colbat_t("lineorderAN", "partkey"));
    MEASURE_OP(batLA5, new resint_colbat_t("lineorderAN", "suppkey"));
    MEASURE_OP(batLA6, new resint_colbat_t("lineorderAN", "orderdate"));
    MEASURE_OP(batLA7, new str_colbat_t("lineorderAN", "orderpriority"));
    MEASURE_OP(batLA8, new char_colbat_t("lineorderAN", "shippriority"));
    MEASURE_OP(batLA9, new restiny_colbat_t("lineorderAN", "quantity"));
    MEASURE_OP(batLAA, new resint_colbat_t("lineorderAN", "extendedprice"));
    MEASURE_OP(batLAB, new resint_colbat_t("lineorderAN", "ordertotalprice"));
    MEASURE_OP(batLAC, new restiny_colbat_t("lineorderAN", "discount"));
    MEASURE_OP(batLAD, new resint_colbat_t("lineorderAN", "revenue"));
    MEASURE_OP(batLAE, new resint_colbat_t("lineorderAN", "supplycost"));
    MEASURE_OP(batLAF, new restiny_colbat_t("lineorderAN", "tax"));
    MEASURE_OP(batLAG, new resint_colbat_t("lineorderAN", "commitdate"));
    MEASURE_OP(batLAH, new str_colbat_t("lineorderAN", "shipmode"));

    // partkey|name|mfgr|category|brand|color|type|size|container
    // RESINT|STRING:22|STRING:6|STRING:7|STRING:9|STRING:11|STRING:25|RESTINY|STRING:10
    MEASURE_OP(batPA1, new resint_colbat_t("partAN", "partkey"));
    MEASURE_OP(batPA2, new str_colbat_t("partAN", "name"));
    MEASURE_OP(batPA3, new str_colbat_t("partAN", "mfgr"));
    MEASURE_OP(batPA4, new str_colbat_t("partAN", "category"));
    MEASURE_OP(batPA5, new str_colbat_t("partAN", "brand"));
    MEASURE_OP(batPA6, new str_colbat_t("partAN", "color"));
    MEASURE_OP(batPA7, new str_colbat_t("partAN", "type"));
    MEASURE_OP(batPA8, new restiny_colbat_t("partAN", "size"));
    MEASURE_OP(batPA9, new str_colbat_t("partAN", "container"));

    // suppkey|name|address|city|nation|region|phone
    // RESINT|STRING:25|STRING:25|STRING:10|STRING:15|STRING:12|STRING:15
    MEASURE_OP(batSA1, new resint_colbat_t("supplierAN", "suppkey"));
    MEASURE_OP(batSA2, new str_colbat_t("supplierAN", "name"));
    MEASURE_OP(batSA3, new str_colbat_t("supplierAN", "address"));
    MEASURE_OP(batSA4, new str_colbat_t("supplierAN", "city"));
    MEASURE_OP(batSA5, new str_colbat_t("supplierAN", "nation"));
    MEASURE_OP(batSA6, new str_colbat_t("supplierAN", "region"));
    MEASURE_OP(batSA7, new str_colbat_t("supplierAN", "phone"));

    ssb::after_create_columnbats();
}

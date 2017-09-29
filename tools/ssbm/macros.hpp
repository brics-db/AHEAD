// Copyright (c) 2017 Till Kolditz
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
 * File:   macros.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 07-06-2017 16:56
 */
#ifndef TOOLS_SSBM_MACROS_HPP_
#define TOOLS_SSBM_MACROS_HPP_

#include <tuple>

#ifdef _OPENMP
#define BEFORE_SAVE_STATS ssb::lock_for_stats();
#define AFTER_SAVE_STATS ssb::unlock_for_stats();
#else
#define BEFORE_SAVE_STATS
#define AFTER_SAVE_STATS
#endif

///////////////
// SAVE_TYPE //
///////////////
#define SAVE_TYPE(BAT)                                                         \
do {                                                                           \
    ssb::headTypes.push_back(BAT->type_head());                                \
    ssb::tailTypes.push_back(BAT->type_tail());                                \
    ssb::hasTwoTypes.push_back(true);                                          \
} while (false)

////////////////
// MEASURE_OP //
////////////////
#define MEASURE_OP(...) VFUNC(MEASURE_OP, __VA_ARGS__)

#define MEASURE_OP6(TYPE, VAR, OP, STORE_SIZE_OP, STORE_CONSUMPTION_OP, STORE_PROJECTEDCONSUMPTION_OP) \
ssb::before_op();                                                              \
TYPE VAR = OP;                                                                 \
ssb::after_op();                                                               \
do {                                                                           \
    BEFORE_SAVE_STATS                                                          \
    ssb::batSizes.push_back((STORE_SIZE_OP));                                  \
    ssb::batConsumptions.push_back((STORE_CONSUMPTION_OP));                    \
    ssb::batConsumptionsProj.push_back((STORE_PROJECTEDCONSUMPTION_OP));       \
} while (false)

#define MEASURE_OP4(VAR, IDX, OP, TYPE)                                        \
MEASURE_OP6(VAR, IDX, OP, (TYPE ->size()), (TYPE ->consumption()),             \
    (TYPE ->consumptionProjected()));                                          \
do {                                                                           \
    SAVE_TYPE(TYPE);                                                           \
    AFTER_SAVE_STATS                                                           \
} while (false)

#define MEASURE_OP3(TYPE, VAR, OP)                                             \
MEASURE_OP6(TYPE, VAR, OP, 1, sizeof(TYPE), sizeof(TYPE));                     \
do {                                                                           \
    ssb::headTypes.push_back(boost::typeindex::type_id<TYPE>().type_info());   \
    ssb::hasTwoTypes.push_back(false);                                         \
    AFTER_SAVE_STATS                                                           \
} while (false)

#define MEASURE_OP2(BAT, OP)                                                   \
MEASURE_OP6(auto, BAT, OP, BAT->size(), BAT->consumption(),                    \
        BAT->consumptionProjected());                                          \
do {                                                                           \
    SAVE_TYPE(BAT);                                                            \
    AFTER_SAVE_STATS                                                           \
} while (false)

#define MEASURE_OP_PAIR(PAIR, OP)                                              \
MEASURE_OP6(auto, PAIR, OP, PAIR.first->size(), PAIR.first->consumption(),     \
    PAIR.first->consumptionProjected());                                       \
do {                                                                           \
    SAVE_TYPE(PAIR.first);                                                     \
    AFTER_SAVE_STATS                                                           \
} while (false)

#define MEASURE_OP_TUPLE(TUPLE, OP)                                            \
MEASURE_OP6(auto, TUPLE, OP, (std::get<0>(TUPLE)->size()),                     \
        (std::get<0>(TUPLE)->consumption()),                                   \
        (std::get<0>(TUPLE)->consumptionProjected()));                         \
do {                                                                           \
    SAVE_TYPE((std::get<0>(TUPLE)));                                           \
    AFTER_SAVE_STATS                                                           \
} while (false)

/////////////////////
// CLEAR_SELECT_AN //
/////////////////////
#define CLEAR_SELECT_AN(PAIR)                                                  \
do {                                                                           \
    if (std::get<1>(PAIR)) {                                                   \
        delete std::get<1>(PAIR);                                              \
    }                                                                          \
} while(false)

///////////////////
// CLEAR_JOIN_AN //
///////////////////
#define CLEAR_JOIN_AN(TUPLE)                                                   \
do {                                                                           \
    if (std::get<1>(TUPLE)) {                                                  \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
    if (std::get<4>(TUPLE)) {                                                  \
        delete std::get<4>(TUPLE);                                             \
    }                                                                          \
} while (false)

#define CLEAR_FETCHJOIN_AN(TUPLE)                                              \
do {                                                                           \
    if (std::get<1>(TUPLE)) {                                                  \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
} while (false)

/////////////////////////////
// CLEAR_CHECKANDDECODE_AN //
/////////////////////////////
#define CLEAR_CHECKANDDECODE_AN(TUPLE)                                         \
do {                                                                           \
    if (std::get<1>(TUPLE)) {                                                  \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
} while (false)

//////////////////////
// CLEAR_GROUPBY_AN //
//////////////////////
#define CLEAR_GROUPBY_UNARY_AN(TUPLE)                                          \
do {                                                                           \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
} while (false)
#define CLEAR_GROUPBY_BINARY_AN(TUPLE)                                         \
do {                                                                           \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
    if (std::get<4>(TUPLE)) {                                                  \
        delete std::get<4>(TUPLE);                                             \
    }                                                                          \
} while (false)

/////////////////////////
// CLEAR_GROUPEDSUM_AN //
/////////////////////////
#define CLEAR_GROUPEDSUM_AN(TUPLE)                                             \
do {                                                                           \
    if (std::get<1>(TUPLE)) {                                                  \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
} while (false)

/////////////////////////
// CLEAR_GROUPEDSUM_AN //
/////////////////////////
#define CLEAR_ARITHMETIC_AN(TUPLE)                                             \
do {                                                                           \
    if (std::get<1>(TUPLE)) {                                                  \
        delete std::get<1>(TUPLE);                                             \
    }                                                                          \
    if (std::get<2>(TUPLE)) {                                                  \
        delete std::get<2>(TUPLE);                                             \
    }                                                                          \
    if (std::get<3>(TUPLE)) {                                                  \
        delete std::get<3>(TUPLE);                                             \
    }                                                                          \
    if (std::get<4>(TUPLE)) {                                                  \
        delete std::get<4>(TUPLE);                                             \
    }                                                                          \
} while (false)

#endif /* TOOLS_SSBM_MACROS_HPP_ */

// Copyright (c) 2016 Till Kolditz
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
 * File:   rss.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 5. August 2016, 17:22
 */

#ifndef RSS_HPP
#define RSS_HPP

#include <cstddef>
#include <cstdint>
#include <cinttypes>

typedef enum size_e {

    B = 0, KB, MB, GB, TB
} size_enum_t;

/**
 * Returns the peak (maximum so far) resident set size (physical
 * memory use) measured in bytes, or zero if the value cannot be
 * determined on this OS.
 */
size_t getPeakRSS (size_enum_t size_enum = size_enum_t::B);

/**
 * Returns the current resident set size (physical memory use) measured
 * in bytes, or zero if the value cannot be determined on this OS.
 */
size_t getCurrentRSS (size_enum_t size_enum = size_enum_t::B);

#endif /* RSS_HPP */

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
 * File:   cpuid.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 09-08-2017 13:48
 */
#ifndef INCLUDE_UTIL_CPUID_HPP_
#define INCLUDE_UTIL_CPUID_HPP_

namespace ahead {
    namespace cpuid {

//  Misc.
        extern const bool HW_MMX;
        extern const bool HW_x64;
        extern const bool HW_ABM; // Advanced Bit Manipulation
        extern const bool HW_RDRAND;
        extern const bool HW_BMI1;
        extern const bool HW_BMI2;
        extern const bool HW_ADX;
        extern const bool HW_PREFETCHWT1;

//  SIMD: 128-bit
        extern const bool HW_SSE;
        extern const bool HW_SSE2;
        extern const bool HW_SSE3;
        extern const bool HW_SSSE3;
        extern const bool HW_SSE41;
        extern const bool HW_SSE42;
        extern const bool HW_SSE4a;
        extern const bool HW_AES;
        extern const bool HW_SHA;

//  SIMD: 256-bit
        extern const bool HW_AVX;
        extern const bool HW_XOP;
        extern const bool HW_FMA3;
        extern const bool HW_FMA4;
        extern const bool HW_AVX2;

//  SIMD: 512-bit
        extern const bool HW_AVX512F; //  AVX512 Foundation
        extern const bool HW_AVX512CD; //  AVX512 Conflict Detection
        extern const bool HW_AVX512PF; //  AVX512 Prefetch
        extern const bool HW_AVX512ER; //  AVX512 Exponential + Reciprocal
        extern const bool HW_AVX512VL; //  AVX512 Vector Length Extensions
        extern const bool HW_AVX512BW; //  AVX512 Byte + Word
        extern const bool HW_AVX512DQ; //  AVX512 Doubleword + Quadword
        extern const bool HW_AVX512IFMA; //  AVX512 Integer 52-bit Fused Multiply-Add
        extern const bool HW_AVX512VBMI; //  AVX512 Vector Byte Manipulation Instructions

        void checkCPUID();

    }
}

#endif /* INCLUDE_UTIL_CPUID_HPP_ */

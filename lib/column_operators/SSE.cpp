/*
 * Copyright 2017 Till Kolditz <till.kolditz@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* 
 * File:   SSE.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 25. Februar 2017, 23:48
 */

#include <column_operators/SSE.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

#define MASK_128_8_2(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, \
                0x00, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14

#define MASK_128_8_4(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_2(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_2(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x01)

#define MASK_128_8_8(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_4(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_4(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x02)

#define MASK_128_8_16(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_8(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_8(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x03)

#define MASK_128_8_32(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_16(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_16(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x04)

#define MASK_128_8_64(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_32(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_32(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x05)

#define MASK_128_8_128(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_64(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_64(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x06)

#define MASK_128_8_256(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_128(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_128(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x07)

#define MASK_128_8_512(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_256(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_256(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x08)

#define MASK_128_8_1024(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_512(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_512(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0A)

#define MASK_128_8_2048(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_1024(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_1024(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0B)

#define MASK_128_8_4096(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_2048(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_2048(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0C)

#define MASK_128_8_8192(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_4096(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_4096(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0D)

#define MASK_128_8_16384(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_8192(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_8192(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0E)

#define MASK_128_8_32768(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_16384(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_16384(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0F)

            const signed char v2::bat::ops::v2_mm128<uint8_t>::SHUFFLE_EPI8_TABLE8[32768 * 16] = {MASK_128_8_32768(static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF))};

            const __m128i * const v2::bat::ops::v2_mm128<uint8_t>::SHUFFLE_EPI8_TABLE = reinterpret_cast<const __m128i*>(v2::bat::ops::v2_mm128<uint8_t>::SHUFFLE_EPI8_TABLE8);

#undef MASK_128_8_2
#undef MASK_128_8_4
#undef MASK_128_8_8
#undef MASK_128_8_16
#undef MASK_128_8_32
#undef MASK_128_8_64
#undef MASK_128_8_128
#undef MASK_128_8_256
#undef MASK_128_8_512
#undef MASK_128_8_1024
#undef MASK_128_8_2048
#undef MASK_128_8_4096
#undef MASK_128_8_8192
#undef MASK_128_8_16384
#undef MASK_128_8_32768


#define MASK_128_16_2(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, \
                0x00, 0x01, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13

#define MASK_128_16_4(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_2(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_2(A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x03, 0x02)

#define MASK_128_16_8(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_4(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_4(A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x05, 0x04)

#define MASK_128_16_16(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_8(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_8(A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x07, 0x06)

#define MASK_128_16_32(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_16(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_16(A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x09, 0x8)

#define MASK_128_16_64(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_32(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_32(A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0B, 0x0A)

#define MASK_128_16_128(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_64(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_64(A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0D, 0x0C)

#define MASK_128_16_256(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_128(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_128(A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0F, 0x0E)

            const signed char v2::bat::ops::v2_mm128<uint16_t>::SHUFFLE_EPI16_TABLE8[256 * 16] = {MASK_128_16_256(static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF))};
			const __m128i * const v2::bat::ops::v2_mm128<uint16_t>::SHUFFLE_EPI16_TABLE = reinterpret_cast<const __m128i*>(v2::bat::ops::v2_mm128<uint16_t>::SHUFFLE_EPI16_TABLE8);

#undef MASK_128_16_2
#undef MASK_128_16_4
#undef MASK_128_16_8
#undef MASK_128_16_16
#undef MASK_128_16_32
#undef MASK_128_16_64
#undef MASK_128_16_128
#undef MASK_128_16_256

#define MASK_128_32_2(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, \
                0x00, 0x01, 0x02, 0x03, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11

#define MASK_128_32_4(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_32_2(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_32_2(A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x07, 0x06, 0x05, 0x04)

#define MASK_128_32_8(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_32_4(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_32_4(A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0B, 0x0A, 0x09, 0x08)

#define MASK_128_32_16(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_32_8(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_32_8(A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0F, 0x0E, 0x0D, 0x0C)
			
            const signed char v2::bat::ops::v2_mm128<uint32_t>::SHUFFLE_EPI32_TABLE8[16 * 16] = {MASK_128_32_16(static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF))};
			const __m128i * const v2::bat::ops::v2_mm128<uint32_t>::SHUFFLE_EPI32_TABLE = reinterpret_cast<const __m128i*>(v2::bat::ops::v2_mm128<uint32_t>::SHUFFLE_EPI32_TABLE8);

#undef MASK_128_32_2
#undef MASK_128_32_4
#undef MASK_128_32_8
#undef MASK_128_32_16

#define MASK_128_64_2(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, \
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, A0, A1, A2, A3, A4, A5, A6, A7

#define MASK_128_64_4(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_64_2(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_64_2(A7, A6, A5, A4, A3, A2, A1, A0, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09, 0x08)

            const signed char v2::bat::ops::v2_mm128<uint64_t>::SHUFFLE_EPI64_TABLE8[4 * 16] = {MASK_128_64_4(static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF))};
			const __m128i * const v2::bat::ops::v2_mm128<uint64_t>::SHUFFLE_EPI64_TABLE = reinterpret_cast<const __m128i*>(v2::bat::ops::v2_mm128<uint64_t>::SHUFFLE_EPI64_TABLE8);

#undef MASK_128_64_2
#undef MASK_128_64_4
        }
    }
}


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

#include <iostream>

#define MASK_64L_8_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                A0, A1, A2, A3, A4, A5, A6, A7, \
                0x00, A0, A1, A2, A3, A4, A5, A6

#define MASK_64L_8_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64L_8_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64L_8_2(A6, A5, A4, A3, A2, A1, A0, 0x01)

#define MASK_64L_8_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64L_8_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64L_8_4(A6, A5, A4, A3, A2, A1, A0, 0x02)

#define MASK_64L_8_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64L_8_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64L_8_8(A6, A5, A4, A3, A2, A1, A0, 0x03)

#define MASK_64L_8_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64L_8_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64L_8_16(A6, A5, A4, A3, A2, A1, A0, 0x04)

#define MASK_64L_8_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64L_8_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64L_8_32(A6, A5, A4, A3, A2, A1, A0, 0x05)

#define MASK_64L_8_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64L_8_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64L_8_64(A6, A5, A4, A3, A2, A1, A0, 0x06)

#define MASK_64L_8_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64L_8_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64L_8_128(A6, A5, A4, A3, A2, A1, A0, 0x07)

#define MASK_64H_8_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                A0, A1, A2, A3, A4, A5, A6, A7, \
                0x08, A0, A1, A2, A3, A4, A5, A6

#define MASK_64H_8_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64H_8_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64H_8_2(A6, A5, A4, A3, A2, A1, A0, 0x09)

#define MASK_64H_8_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64H_8_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64H_8_4(A6, A5, A4, A3, A2, A1, A0, 0x0A)

#define MASK_64H_8_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64H_8_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64H_8_8(A6, A5, A4, A3, A2, A1, A0, 0x0B)

#define MASK_64H_8_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64H_8_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64H_8_16(A6, A5, A4, A3, A2, A1, A0, 0x0C)

#define MASK_64H_8_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64H_8_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64H_8_32(A6, A5, A4, A3, A2, A1, A0, 0x0D)

#define MASK_64H_8_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64H_8_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64H_8_64(A6, A5, A4, A3, A2, A1, A0, 0x0E)

#define MASK_64H_8_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_64H_8_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_64H_8_128(A6, A5, A4, A3, A2, A1, A0, 0x0F)

#define MASK_64_08L MASK_64L_8_256(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
#define MASK_64_08H MASK_64H_8_256(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

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
                MASK_128_8_512(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x09)

#define MASK_128_8_2048(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_1024(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_1024(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0A)

#define MASK_128_8_4096(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_2048(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_2048(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0B)

#define MASK_128_8_8192(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_4096(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_4096(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0C)

#define MASK_128_8_16384(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_8192(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_8192(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0D)

#define MASK_128_8_32768(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_16384(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_16384(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0E)

#define MASK_128_8_65536(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_8_32768(A15, A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_8_32768(A14, A13, A12, A11, A10, A9, A8, A7, A6, A5, A4, A3, A2, A1, A0, 0x0F)

//#define MASK_128_8 MASK_128_8_65536(static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF), static_cast<int8_t>(0xFF))
#define MASK_128_08 MASK_128_8_65536(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

#define MASK_128_16_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                A0, A1, A2, A3, A4, A5, A6, A7, \
                0x0100u, A0, A1, A2, A3, A4, A5, A6

#define MASK_128_16_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_2(A6, A5, A4, A3, A2, A1, A0, 0x0302u)

#define MASK_128_16_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_4(A6, A5, A4, A3, A2, A1, A0, 0x0504u)

#define MASK_128_16_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_8(A6, A5, A4, A3, A2, A1, A0, 0x0706u)

#define MASK_128_16_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_16(A6, A5, A4, A3, A2, A1, A0, 0x0908u)

#define MASK_128_16_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_32(A6, A5, A4, A3, A2, A1, A0, 0x0B0Au)

#define MASK_128_16_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_64(A6, A5, A4, A3, A2, A1, A0, 0x0D0Cu)

#define MASK_128_16_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                MASK_128_16_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                MASK_128_16_128(A6, A5, A4, A3, A2, A1, A0, 0x0F0Eu)

#define MASK_128_16 MASK_128_16_256(0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu)

#define MASK_128_32_2(A3, A2, A1, A0) \
                A0, A1, A2, A3, \
                0x03020100ul, A0, A1, A2

#define MASK_128_32_4(A3, A2, A1, A0) \
                MASK_128_32_2(A3, A2, A1, A0), \
                MASK_128_32_2(A2, A1, A0, 0x07060504ul)

#define MASK_128_32_8(A3, A2, A1, A0) \
                MASK_128_32_4(A3, A2, A1, A0), \
                MASK_128_32_4(A2, A1, A0, 0x0B0A0908ul)

#define MASK_128_32_16(A3, A2, A1, A0) \
                MASK_128_32_8(A3, A2, A1, A0), \
                MASK_128_32_8(A2, A1, A0, 0x0F0E0D0Cul)

#define MASK_128_32 MASK_128_32_16(0xFFFFFFFFul, 0xFFFFFFFFul, 0xFFFFFFFFul, 0xFFFFFFFFul)

#define MASK_128_64_2(A1, A0) \
                A0, A1, \
                0x0706050403020100ull, A0

#define MASK_128_64_4(A1, A0) \
                MASK_128_64_2(A1, A0), \
                MASK_128_64_2(A0, 0x0F0E0D0C0B0A0908ull)

#define MASK_128_64 MASK_128_64_4(0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull)

#define TOSTRING0(...) #__VA_ARGS__
#define TOSTRING(x) TOSTRING0(x)

int main() {
    std::cout
            << "/*\n\
 * Copyright 2017 Till Kolditz <till.kolditz@gmail.com>.\n\
 *\n\
 * Licensed under the Apache License, Version 2.0 (the \"License\");\n\
 * you may not use this file except in compliance with the License.\n\
 * You may obtain a copy of the License at\n\
 *\n\
 *      http://www.apache.org/licenses/LICENSE-2.0\n\
 *\n\
 * Unless required by applicable law or agreed to in writing, software\n\
 * distributed under the License is distributed on an \"AS IS\" BASIS,\n\
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
 * See the License for the specific language governing permissions and\n\
 * limitations under the License.\n\
 */\n\
\n\
/* \n\
 * File:   SSE.cpp\n\
 * Author: Till Kolditz <till.kolditz@gmail.com>\n\
 *\n\
 * Created on 25. Februar 2017, 23:48\n\
 */\n\
\n\
#include <column_operators/SSE.hpp>\n\
\n\
namespace v2 {\n\
    namespace bat {\n\
        namespace ops {\n\n";
    std::string mask08 = TOSTRING(MASK_128_08);
    std::string mask08L = TOSTRING(MASK_64_08L);
    std::string mask08H = TOSTRING(MASK_64_08H);
    std::string mask16 = TOSTRING(MASK_128_16);
    std::string mask32 = TOSTRING(MASK_128_32);
    std::string mask64 = TOSTRING(MASK_128_64);
    std::cout << "            const uint8_t SHUFFLE_EPI08_TABLE[65536 * 16] = {";
    for (size_t i = 0; i < mask08.size(); i += 96) {
        std::cout << "\n                " << mask08.substr(i, 96);
    }
    std::cout << "\n            };\n\n";
    std::cout << "            const uint8_t SHUFFLE_EPI08_TABLE_L[256 * 8] = {";
    for (size_t i = 0; i < mask08L.size(); i += 48) {
        std::cout << "\n                " << mask08L.substr(i, 48);
    }
    std::cout << "\n            };\n\n";
    std::cout << "            const uint8_t SHUFFLE_EPI08_TABLE_H[256 * 8] = {";
    for (size_t i = 0; i < mask08H.size(); i += 48) {
        std::cout << "\n                " << mask08H.substr(i, 48);
    }
    std::cout << "\n            };\n\n";
    std::cout << "            const uint16_t SHUFFLE_EPI16_TABLE[256 * 8] = {";
    for (size_t i = 0; i < mask16.size(); i += 72) {
        std::cout << "\n                " << mask16.substr(i, 72);
    }
    std::cout << "\n            };\n\n";
    std::cout << "            const uint32_t SHUFFLE_EPI32_TABLE[16 * 4] = {";
    for (size_t i = 0; i < mask32.size(); i += 56) {
        std::cout << "\n                " << mask32.substr(i, 56);
    }
    std::cout << "\n            };\n\n";
    std::cout << "            const uint64_t SHUFFLE_EPI64_TABLE[4 * 2] = {";
    for (size_t i = 0; i < mask64.size(); i += 46) {
        std::cout << "\n                " << mask64.substr(i, 46);
    }
    std::cout << "\n            };\n\n";
    std::cout << "            const __m128i * const v2_mm128<uint8_t>::SHUFFLE_TABLE = reinterpret_cast<const __m128i*>(SHUFFLE_EPI08_TABLE);\n";
    std::cout << "            const uint64_t * const v2_mm128<uint8_t>::SHUFFLE_TABLE_L = reinterpret_cast<const uint64_t*>(SHUFFLE_EPI08_TABLE_L);\n";
    std::cout << "            const uint64_t * const v2_mm128<uint8_t>::SHUFFLE_TABLE_H = reinterpret_cast<const uint64_t*>(SHUFFLE_EPI08_TABLE_H);\n";
    std::cout << "            const __m128i * const v2_mm128<uint16_t>::SHUFFLE_TABLE = reinterpret_cast<const __m128i*>(SHUFFLE_EPI16_TABLE);\n";
    std::cout << "            const __m128i * const v2_mm128<uint32_t>::SHUFFLE_TABLE = reinterpret_cast<const __m128i*>(SHUFFLE_EPI32_TABLE);\n";
    std::cout << "            const __m128i * const v2_mm128<uint64_t>::SHUFFLE_TABLE = reinterpret_cast<const __m128i*>(SHUFFLE_EPI64_TABLE);\n";
    std::cout << "        }\n\
    }\n\
}" << std::endl;
}


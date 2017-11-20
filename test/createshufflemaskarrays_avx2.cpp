/*
 * Copyright 2017 Till Kolditz <till.kolditz@gmail.com>
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
 * File:   createshufflemaskarrays_avx2.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 07. August 2017, 14:35
 */

#include <iostream>

/*
 * 4 Tables for 8-bit data
 */
#define MASK_64LL_8_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                    A0, A1, A2, A3, A4, A5, A6, A7, \
                    0x00, A0, A1, A2, A3, A4, A5, A6

#define MASK_64LL_8_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LL_8_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LL_8_2(A6, A5, A4, A3, A2, A1, A0, 0x01)

#define MASK_64LL_8_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LL_8_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LL_8_4(A6, A5, A4, A3, A2, A1, A0, 0x02)

#define MASK_64LL_8_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LL_8_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LL_8_8(A6, A5, A4, A3, A2, A1, A0, 0x03)

#define MASK_64LL_8_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LL_8_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LL_8_16(A6, A5, A4, A3, A2, A1, A0, 0x04)

#define MASK_64LL_8_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LL_8_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LL_8_32(A6, A5, A4, A3, A2, A1, A0, 0x05)

#define MASK_64LL_8_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LL_8_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LL_8_64(A6, A5, A4, A3, A2, A1, A0, 0x06)

#define MASK_64LL_8_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LL_8_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LL_8_128(A6, A5, A4, A3, A2, A1, A0, 0x07)

#define MASK_64LH_8_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                    A0, A1, A2, A3, A4, A5, A6, A7, \
                    0x08, A0, A1, A2, A3, A4, A5, A6

#define MASK_64LH_8_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LH_8_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LH_8_2(A6, A5, A4, A3, A2, A1, A0, 0x09)

#define MASK_64LH_8_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LH_8_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LH_8_4(A6, A5, A4, A3, A2, A1, A0, 0x0A)

#define MASK_64LH_8_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LH_8_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LH_8_8(A6, A5, A4, A3, A2, A1, A0, 0x0B)

#define MASK_64LH_8_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LH_8_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LH_8_16(A6, A5, A4, A3, A2, A1, A0, 0x0C)

#define MASK_64LH_8_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LH_8_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LH_8_32(A6, A5, A4, A3, A2, A1, A0, 0x0D)

#define MASK_64LH_8_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LH_8_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LH_8_64(A6, A5, A4, A3, A2, A1, A0, 0x0E)

#define MASK_64LH_8_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64LH_8_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64LH_8_128(A6, A5, A4, A3, A2, A1, A0, 0x0F)

#define MASK_64HL_8_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                    A0, A1, A2, A3, A4, A5, A6, A7, \
                    0x10, A0, A1, A2, A3, A4, A5, A6

#define MASK_64HL_8_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HL_8_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HL_8_2(A6, A5, A4, A3, A2, A1, A0, 0x11)

#define MASK_64HL_8_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HL_8_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HL_8_4(A6, A5, A4, A3, A2, A1, A0, 0x12)

#define MASK_64HL_8_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HL_8_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HL_8_8(A6, A5, A4, A3, A2, A1, A0, 0x13)

#define MASK_64HL_8_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HL_8_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HL_8_16(A6, A5, A4, A3, A2, A1, A0, 0x14)

#define MASK_64HL_8_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HL_8_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HL_8_32(A6, A5, A4, A3, A2, A1, A0, 0x15)

#define MASK_64HL_8_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HL_8_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HL_8_64(A6, A5, A4, A3, A2, A1, A0, 0x16)

#define MASK_64HL_8_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HL_8_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HL_8_128(A6, A5, A4, A3, A2, A1, A0, 0x17)

#define MASK_64HH_8_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                    A0, A1, A2, A3, A4, A5, A6, A7, \
                    0x18, A0, A1, A2, A3, A4, A5, A6

#define MASK_64HH_8_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HH_8_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HH_8_2(A6, A5, A4, A3, A2, A1, A0, 0x19)

#define MASK_64HH_8_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HH_8_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HH_8_4(A6, A5, A4, A3, A2, A1, A0, 0x1A)

#define MASK_64HH_8_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HH_8_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HH_8_8(A6, A5, A4, A3, A2, A1, A0, 0x1B)

#define MASK_64HH_8_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HH_8_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HH_8_16(A6, A5, A4, A3, A2, A1, A0, 0x1C)

#define MASK_64HH_8_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HH_8_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HH_8_32(A6, A5, A4, A3, A2, A1, A0, 0x1D)

#define MASK_64HH_8_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HH_8_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HH_8_64(A6, A5, A4, A3, A2, A1, A0, 0x1E)

#define MASK_64HH_8_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_64HH_8_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_64HH_8_128(A6, A5, A4, A3, A2, A1, A0, 0x1F)

#define MASK_64_08LL MASK_64LL_8_256(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
#define MASK_64_08LH MASK_64LH_8_256(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
#define MASK_64_08HL MASK_64HL_8_256(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)
#define MASK_64_08HH MASK_64HH_8_256(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

/*
 * Table for 16-bit data
 */

#define MASK_128L_16_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                    A0, A1, A2, A3, A4, A5, A6, A7, \
                    0x0100u, A0, A1, A2, A3, A4, A5, A6

#define MASK_128L_16_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128L_16_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128L_16_2(A6, A5, A4, A3, A2, A1, A0, 0x0302u)

#define MASK_128L_16_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128L_16_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128L_16_4(A6, A5, A4, A3, A2, A1, A0, 0x0504u)

#define MASK_128L_16_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128L_16_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128L_16_8(A6, A5, A4, A3, A2, A1, A0, 0x0706u)

#define MASK_128L_16_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128L_16_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128L_16_16(A6, A5, A4, A3, A2, A1, A0, 0x0908u)

#define MASK_128L_16_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128L_16_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128L_16_32(A6, A5, A4, A3, A2, A1, A0, 0x0B0Au)

#define MASK_128L_16_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128L_16_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128L_16_64(A6, A5, A4, A3, A2, A1, A0, 0x0D0Cu)

#define MASK_128L_16_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128L_16_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128L_16_128(A6, A5, A4, A3, A2, A1, A0, 0x0F0Eu)

#define MASK_128H_16_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                    A0, A1, A2, A3, A4, A5, A6, A7, \
                    0x1110u, A0, A1, A2, A3, A4, A5, A6

#define MASK_128H_16_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128H_16_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128H_16_2(A6, A5, A4, A3, A2, A1, A0, 0x1312u)

#define MASK_128H_16_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128H_16_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128H_16_4(A6, A5, A4, A3, A2, A1, A0, 0x1514u)

#define MASK_128H_16_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128H_16_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128H_16_8(A6, A5, A4, A3, A2, A1, A0, 0x1716u)

#define MASK_128H_16_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128H_16_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128H_16_16(A6, A5, A4, A3, A2, A1, A0, 0x1918u)

#define MASK_128H_16_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128H_16_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128H_16_32(A6, A5, A4, A3, A2, A1, A0, 0x1B1Au)

#define MASK_128H_16_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128H_16_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128H_16_64(A6, A5, A4, A3, A2, A1, A0, 0x1D1Cu)

#define MASK_128H_16_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_128H_16_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_128H_16_128(A6, A5, A4, A3, A2, A1, A0, 0x1F1Eu)

#define MASK_128L_16 MASK_128L_16_256(0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu)
#define MASK_128H_16 MASK_128H_16_256(0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu, 0xFFFFu)

/*
 * Table for 32-bit data
 */

#define MASK_256_32_2(A7, A6, A5, A4, A3, A2, A1, A0) \
                    A0, A1, A2, A3, A4, A5, A6, A7, \
                    0x03020100ul, A0, A1, A2, A3, A4, A5, A6

#define MASK_256_32_4(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_256_32_2(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_256_32_2(A6, A5, A4, A3, A2, A1, A0, 0x07060504ul)

#define MASK_256_32_8(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_256_32_4(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_256_32_4(A6, A5, A4, A3, A2, A1, A0, 0x0B0A0908ul)

#define MASK_256_32_16(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_256_32_8(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_256_32_8(A6, A5, A4, A3, A2, A1, A0, 0x0F0E0D0Cul)

#define MASK_256_32_32(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_256_32_16(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_256_32_16(A6, A5, A4, A3, A2, A1, A0, 0x13121110ul)

#define MASK_256_32_64(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_256_32_32(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_256_32_32(A6, A5, A4, A3, A2, A1, A0, 0x17161514ul)

#define MASK_256_32_128(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_256_32_64(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_256_32_64(A6, A5, A4, A3, A2, A1, A0, 0x1B1A1918ul)

#define MASK_256_32_256(A7, A6, A5, A4, A3, A2, A1, A0) \
                    MASK_256_32_128(A7, A6, A5, A4, A3, A2, A1, A0), \
                    MASK_256_32_128(A6, A5, A4, A3, A2, A1, A0, 0x1F1E1D1Cul)

#define MASK_256_32 MASK_256_32_256(0xFFFFFFFFul, 0xFFFFFFFFul, 0xFFFFFFFFul, 0xFFFFFFFFul, 0xFFFFFFFFul, 0xFFFFFFFFul, 0xFFFFFFFFul, 0xFFFFFFFFul)

/*
 * Table for 64-bit data
 */

#define MASK_256_64_2(A3, A2, A1, A0) \
                    A0, A1, A2, A3, \
                    0x0706050403020100ull, A0, A1, A2

#define MASK_256_64_4(A3, A2, A1, A0) \
                    MASK_256_64_2(A3, A2, A1, A0), \
                    MASK_256_64_2(A2, A1, A0, 0x0F0E0D0C0B0A0908ull)

#define MASK_256_64_8(A3, A2, A1, A0) \
                    MASK_256_64_4(A3, A2, A1, A0), \
                    MASK_256_64_4(A2, A1, A0, 0x1716151413121110ull)

#define MASK_256_64_16(A3, A2, A1, A0) \
                    MASK_256_64_8(A3, A2, A1, A0), \
                    MASK_256_64_8(A2, A1, A0, 0x1F1E1D1C1B1A1918ull)

#define MASK_256_64 MASK_256_64_16(0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull)

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
 * File:   createshufflemaskarrays_avx2.cpp\n\
 * Author: Till Kolditz <till.kolditz@gmail.com>\n\
 *\n\
 * Created on 07. August 2017, 14:35\n\
 */\n\
\n\
#ifdef __AVX2__\n\
\n\
#include \"../lib/column_operators/SIMD/AVX2.hpp\"\n\
\n\
namespace ahead {\n\
    namespace bat {\n\
        namespace ops {\n\
            namespace simd {\n\
                namespace avx2 {\n\n";
    std::string mask08LL = TOSTRING(MASK_64_08LL);
    std::string mask08LH = TOSTRING(MASK_64_08LH);
    std::string mask08HL = TOSTRING(MASK_64_08HL);
    std::string mask08HH = TOSTRING(MASK_64_08HH);
    std::string mask16L = TOSTRING(MASK_128L_16);
    std::string mask16H = TOSTRING(MASK_128H_16);
    std::string mask32 = TOSTRING(MASK_256_32);
    std::string mask64 = TOSTRING(MASK_256_64);
    std::cout << "\
                    const uint8_t SHUFFLE_EPI08_TABLE_LL[256 * 8] = {";
    for (size_t i = 0; i < mask08LL.size(); i += 48) {
        std::cout << "\n\
                        " << mask08LL.substr(i, 48);
    }
    std::cout << "\n\
                    };\n\n";
    std::cout << "\
                    const uint8_t SHUFFLE_EPI08_TABLE_LH[256 * 8] = {";
    for (size_t i = 0; i < mask08LH.size(); i += 48) {
        std::cout << "\n\
                        " << mask08LH.substr(i, 48);
    }
    std::cout << "\n\
                    };\n\n";
    std::cout << "\
                    const uint8_t SHUFFLE_EPI08_TABLE_HL[256 * 8] = {";
    for (size_t i = 0; i < mask08HL.size(); i += 48) {
        std::cout << "\n\
                        " << mask08HL.substr(i, 48);
    }
    std::cout << "\n\
                    };\n\n";
    std::cout << "\
                    const uint8_t SHUFFLE_EPI08_TABLE_HH[256 * 8] = {";
    for (size_t i = 0; i < mask08HH.size(); i += 48) {
        std::cout << "\n\
                        " << mask08HH.substr(i, 48);
    }
    std::cout << "\n\
                    };\n\n";
    std::cout << "\
                    const uint16_t SHUFFLE_EPI16_TABLE_L[128 * 16] = {";
    for (size_t i = 0; i < mask16L.size(); i += 72) {
        std::cout << "\n\
                        " << mask16L.substr(i, 72);
    }
    std::cout << "\n\
                    };\n\n";
    std::cout << "\
                    const uint16_t SHUFFLE_EPI16_TABLE_H[128 * 16] = {";
    for (size_t i = 0; i < mask16H.size(); i += 72) {
        std::cout << "\n\
                        " << mask16H.substr(i, 72);
    }
    std::cout << "\n\
                    };\n\n";
    std::cout << "\
                    const uint32_t SHUFFLE_EPI32_TABLE[256 * 8] = {";
    for (size_t i = 0; i < mask32.size(); i += 112) {
        std::cout << "\n\
                        " << mask32.substr(i, 112);
    }
    std::cout << "\n\
                    };\n\n";
    std::cout << "\
                    const uint64_t SHUFFLE_EPI64_TABLE[16 * 4] = {";
    for (size_t i = 0; i < mask64.size(); i += 92) {
        std::cout << "\n\
                        " << mask64.substr(i, 92);
    }
    std::cout
            << "\n\
                    };\n\n\
                    const int64_t * const mm256<uint8_t>::SHUFFLE_TABLE_LL = reinterpret_cast<const int64_t*>(SHUFFLE_EPI08_TABLE_LL);\n\
                    const int64_t * const mm256<uint8_t>::SHUFFLE_TABLE_LH = reinterpret_cast<const int64_t*>(SHUFFLE_EPI08_TABLE_LH);\n\
                    const int64_t * const mm256<uint8_t>::SHUFFLE_TABLE_HL = reinterpret_cast<const int64_t*>(SHUFFLE_EPI08_TABLE_HL);\n\
                    const int64_t * const mm256<uint8_t>::SHUFFLE_TABLE_HH = reinterpret_cast<const int64_t*>(SHUFFLE_EPI08_TABLE_HH);\n\
                    const __m128i * const mm256<uint16_t>::SHUFFLE_TABLE_L = reinterpret_cast<const __m128i*>(SHUFFLE_EPI16_TABLE_L);\n\
                    const __m128i * const mm256<uint16_t>::SHUFFLE_TABLE_H = reinterpret_cast<const __m128i*>(SHUFFLE_EPI16_TABLE_H);\n\
                    const __m256i * const mm256<uint32_t>::SHUFFLE_TABLE = reinterpret_cast<const __m256i*>(SHUFFLE_EPI32_TABLE);\n\
                    const __m256i * const mm256<uint64_t>::SHUFFLE_TABLE = reinterpret_cast<const __m256i*>(SHUFFLE_EPI64_TABLE);\n\
\n\
                }\n\
            }\n\
        }\n\
    }\n\
}\n\
\n\
#endif\n\
"
            << std::endl;
}

#include <cstdlib>
#include <iostream>
#include <random>
#include "../../include/column_operators/SSE.hpp"
#include "../../include/util/stopwatch.hpp"

int main(int argc, char ** argv) {
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " <#values>" << std::endl;
        return 1;
    }

    char* end;
    const size_t NUM = strtoull(argv[1], &end, 10);
    const size_t NUM128 = (NUM / (sizeof(__m128i ) / sizeof(uint8_t)));

    if (NUM == 0) {
        std::cout << "<#values> is not valid! Must be an integer > 0." << std::endl;
        return 2;
    }

    StopWatch sw;
    std::random_device rndDevice;
    std::default_random_engine rndEngine(rndDevice());
    std::uniform_int_distribution<uint16_t> rndDistr;
    __m128i mm = v2::bat::ops::v2_mm128<uint8_t>::set_inc(0);
    __m128i mmResult;
    const size_t NUM_RND = 16;
    const size_t MASK_ARR = 0x0F;

    uint16_t randoms[NUM_RND];
    size_t i = 0;
    for (i = 0; i < NUM_RND; ++i) {
        randoms[i] = rndDistr(rndEngine);
    }

    __m128i * const mmOut = new __m128i [NUM128 + 1];
    __m128i * pmmOut = mmOut;
    for (i = 0; i < NUM128; ++i) {
        _mm_storeu_si128(pmmOut++, _mm_set1_epi16(rndDistr(rndEngine)));
    }

    sw.start();
    for (; i < NUM; ++i) {
        rndDistr(rndEngine);
    }
    sw.stop();
    std::cout << "Runtime for " << i << " rndDistr(rndEngine): " << sw << std::endl;

    ///////////////////
    // single vector //
    ///////////////////
    sw.start();
    for (i = 0; i < NUM; ++i) {
        pmmOut = mmOut;
        uint16_t tmpMask = rndDistr(rndEngine);
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = v2::bat::ops::v2_mm128<uint8_t>::pack_right(mm, tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<uint8_t*>(pmmOut) + nMaskOnes);
    }
    sw.stop();
    std::cout << "Runtime for " << i << " pack_right(random): " << sw << std::endl;

    uint16_t mask = 0x59A3;
    sw.start();
    for (i = 0; i < NUM; ++i) {
        pmmOut = mmOut;
        uint16_t tmpMask = mask;
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = v2::bat::ops::v2_mm128<uint8_t>::pack_right(mm, tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<uint8_t*>(pmmOut) + nMaskOnes);
        mask *= mask;
    }
    sw.stop();
    std::cout << "Runtime for " << i << " pack_right(random2): " << sw << std::endl;

    sw.start();
    for (i = 0; i < NUM; ++i) {
        pmmOut = mmOut;
        uint16_t tmpMask = randoms[i & MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = v2::bat::ops::v2_mm128<uint8_t>::pack_right(mm, tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<uint8_t*>(pmmOut) + nMaskOnes);
    }
    sw.stop();
    std::cout << "Runtime for " << i << " pack_right(array): " << sw << std::endl;

    ///////////////////
    // vector column //
    ///////////////////
    constexpr const size_t SZ = sizeof(__m128i) - 1;
    uint8_t * column = new uint8_t[NUM + SZ];
    for (size_t i = 0; i < (NUM + SZ); ++i) {
        column[i] = rndDistr(rndEngine);
    }

    pmmOut = mmOut;
    __m128i * pMM = reinterpret_cast<__m128i *>(reinterpret_cast<size_t>(column + SZ) & ~SZ); // align to next 16-byte boundary
    sw.start();
    for (i = 0; i < NUM128; ++i) {
        uint16_t tmpMask = rndDistr(rndEngine);
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = v2::bat::ops::v2_mm128<uint8_t>::pack_right(_mm_lddqu_si128(pMM++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<uint8_t*>(pmmOut) + nMaskOnes);
    }
    sw.stop();
    std::cout << "Runtime for " << (i * (sizeof(__m128i) / sizeof(uint8_t))) << " pack_right(column + random): " << sw << std::endl;

    pmmOut = mmOut;
    pMM = reinterpret_cast<__m128i *>(reinterpret_cast<size_t>(column + SZ) & ~SZ); // align to next 16-byte boundary
    mask = 0x59A3;
    sw.start();
    for (i = 0; i < NUM128; ++i) {
        uint16_t tmpMask = mask;
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = v2::bat::ops::v2_mm128<uint8_t>::pack_right(_mm_lddqu_si128(pMM++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<uint8_t*>(pmmOut) + nMaskOnes);
        mask *= mask;
    }
    sw.stop();
    std::cout << "Runtime for " << (i * (sizeof(__m128i) / sizeof(uint8_t))) << " pack_right(column + random2): " << sw << std::endl;

    pmmOut = mmOut;
    pMM = reinterpret_cast<__m128i *>(reinterpret_cast<size_t>(column + SZ) & ~SZ); // align to next 16-byte boundary
    sw.start();
    for (i = 0; i < NUM128; ++i) {
        uint16_t tmpMask = randoms[i & MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = v2::bat::ops::v2_mm128<uint8_t>::pack_right(_mm_lddqu_si128(pMM++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<uint8_t*>(pmmOut) + nMaskOnes);
    }
    sw.stop();
    std::cout << "Runtime for " << (i * (sizeof(__m128i) / sizeof(uint8_t))) << " pack_right(column + array): " << sw << std::endl;

    delete[] column;
    delete[] mmOut;
}


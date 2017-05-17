#include <cstdlib>
#include <iostream>
#include <random>
#include "../../include/column_operators/SSE.hpp"
#include "../../include/util/stopwatch.hpp"

template<typename T>
void test(size_t NUM) {
    const size_t NUM128 = (NUM / (sizeof(__m128i ) / sizeof(T)));
    std::cout << "\tdata size: " << (sizeof(T) * 8) << " bits\n\t# 128-bit vectors: " << NUM128 << std::endl;

    typedef typename ahead::bat::ops::v2_mm128<T>::mask_t mask_t;

    ahead::StopWatch sw;
    std::random_device rndDevice;
    std::default_random_engine rndEngine(rndDevice());
    std::uniform_int_distribution<mask_t> rndDistr;
    __m128i mm = ahead::bat::ops::v2_mm128<T>::set_inc(0);
    __m128i mmResult;

    const size_t NUM_RND = 16;
    const size_t MASK_ARR = 0x0F;
    mask_t randoms[NUM_RND];
    size_t i = 0;
    for (i = 0; i < NUM_RND; ++i) {
        randoms[i] = std::min(rndDistr(rndEngine), static_cast<mask_t>(sizeof(__m128i) / sizeof(T)));
    }

    __m128i * mmOut = new __m128i [NUM128 + 1];
    __m128i * pmmOut = mmOut;
    for (i = 0; i < NUM128; ++i) {
        _mm_storeu_si128(pmmOut++, ahead::bat::ops::v2_mm128<T>::set1(rndDistr(rndEngine)));
    }

    sw.start();
    for (; i < NUM; ++i) {
        rndDistr(rndEngine);
    }
    sw.stop();
    std::cout << "\t\trndDistr(rndEngine):\t" << sw.duration() << std::endl;

    ///////////////////
    // single vector //
    ///////////////////
    sw.start();
    pmmOut = mmOut;
    for (i = 0; i < NUM128; ++i) {
        mask_t tmpMask = rndDistr(rndEngine);
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(mm, tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
    sw.stop();
    std::cout << "\t\tpack_right(random):\t" << sw.duration() << std::endl;

    mask_t mask = static_cast<mask_t>(0x59A3);
    sw.start();
    pmmOut = mmOut;
    for (i = 0; i < NUM128; ++i) {
        mask_t tmpMask = mask;
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(mm, tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
        mask *= mask;
    }
    sw.stop();
    std::cout << "\t\tpack_right(random2):\t" << sw.duration() << std::endl;

    sw.start();
    pmmOut = mmOut;
    for (i = 0; i < NUM128; ++i) {
        mask_t tmpMask = randoms[i & MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(mm, tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
    sw.stop();
    std::cout << "\t\tpack_right(array):\t" << sw.duration() << std::endl;

    ///////////////////
    // vector column //
    ///////////////////
    T * column = new T[NUM];
    for (size_t i = 0; i < NUM; ++i) {
        column[i] = rndDistr(rndEngine);
    }

    pmmOut = mmOut;
    __m128i * pMM = reinterpret_cast<__m128i *>(column);
    sw.start();
    for (i = 0; i < NUM128; ++i) {
        mask_t tmpMask = std::min(rndDistr(rndEngine), static_cast<mask_t>(sizeof(__m128i) / sizeof(T)));
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(_mm_lddqu_si128(pMM++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
    sw.stop();
    std::cout << "\t\tpack_right(column + random): " << sw.duration() << std::endl;

    pmmOut = mmOut;
    pMM = reinterpret_cast<__m128i *>(column);
    mask = static_cast<mask_t>(0x59A3);
    sw.start();
    for (i = 0; i < NUM128; ++i) {
        mask_t tmpMask = std::min(mask, static_cast<mask_t>(sizeof(__m128i) / sizeof(T)));
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(_mm_lddqu_si128(pMM++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
        mask *= mask;
    }
    sw.stop();
    std::cout << "\t\tpack_right(column + random2): " << sw.duration() << std::endl;

    pmmOut = mmOut;
    pMM = reinterpret_cast<__m128i *>(column);
    sw.start();
    for (i = 0; i < NUM128; ++i) {
        mask_t tmpMask = randoms[i & MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(_mm_lddqu_si128(pMM++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
    sw.stop();
    std::cout << "\t\tpack_right(column + array): " << sw.duration() << std::endl;

    T * pResult = reinterpret_cast<T*>(mmOut);
    pMM = reinterpret_cast<__m128i *>(column);
    mask = static_cast<mask_t>(0x59A3);
    sw.start();
    for (i = 0; i < NUM128; ++i) {
        mask_t tmpMask = std::min(mask, static_cast<mask_t>(sizeof(__m128i) / sizeof(T)));
        ahead::bat::ops::v2_mm128<T>::pack_right2(pResult, _mm_lddqu_si128(pMM++), tmpMask);
        mask *= mask;
    }
    sw.stop();
    std::cout << "\t\tpack_right2(column + random2)\r" << sw.duration() << std::endl;

    delete[] column;
    delete[] mmOut;
}

int main(int argc, char ** argv) {
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " <#values>" << std::endl;
        return 1;
    }

    char* end;
    const size_t NUM = strtoull(argv[1], &end, 10);

    if (NUM == 0) {
        std::cout << "<#values> is not valid! Must be an integer > 0." << std::endl;
        return 2;
    } else {
        std::cout << "#total values: " << NUM << std::endl;
    }

    test<uint8_t>(NUM);
    test<uint16_t>(NUM);
    test<uint32_t>(NUM);
}


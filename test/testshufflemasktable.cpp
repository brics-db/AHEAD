#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <random>
#include "SSE.hpp"
#include "../../include/util/stopwatch.hpp"

template<typename T>
void test(const size_t NUM) {
    typedef typename ahead::bat::ops::v2_mm128<T>::mask_t mask_t;
    const size_t NUM128 = (NUM / (sizeof(__m128i ) / sizeof(T)));

    __m128i vector = ahead::bat::ops::v2_mm128<T>::set_inc(static_cast<T>(1));
    size_t maskMax = 1ull << (sizeof(__m128i) / sizeof(T));

    std::random_device rndDevice;
    std::default_random_engine rndEngine(rndDevice());
    std::uniform_int_distribution<T> rndValueDistr;
    std::uniform_int_distribution<mask_t> rndMaskDistr;
    const size_t NUM_RND = 1024 * 64;
    static_assert((NUM_RND & (NUM_RND - 1)) == 0, "NUM_RND must be a power of two!");
    constexpr const size_t MASK_ARR = NUM_RND - 1;
    mask_t randoms[NUM_RND];
    size_t i = 0;

    __m128i * pmmOutOrg = new __m128i [NUM128];
    __m128i * pmmOutAligned = reinterpret_cast<__m128i *>((reinterpret_cast<size_t>(pmmOutOrg) + (sizeof(__m128i) - 1)) & ~(sizeof(__m128i) - 1));
    __m128i * pmmOut = pmmOutAligned;
    for (i = 0; i < NUM128; ++i) {
        _mm_storeu_si128(pmmOut++, ahead::bat::ops::v2_mm128<T>::set1(rndValueDistr(rndEngine)));
    }

    ahead::StopWatch sw;
    // mask_t mask = static_cast<mask_t>(0x59A3);
    for (size_t curMaskMax = 1; curMaskMax < maskMax; ++curMaskMax) {
        for (size_t r = 0; r < NUM_RND; ++r) {
            randoms[r] = rndMaskDistr(rndEngine) % curMaskMax;
        }
        sw.start();
        pmmOut = pmmOutAligned;
        for (i = 0; i < NUM128; ++i) {
            mask_t tmpMask = randoms[i & MASK_ARR];
            // mask_t tmpMask = mask;
            size_t nMaskOnes = __builtin_popcountll(tmpMask);
            __m128i mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(vector, tmpMask);
            _mm_storeu_si128(pmmOut, mmResult);
            pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
            // mask = (mask * mask) % curMaskMax;
        }
        sw.stop();
        std::cout << std::setw(5) << curMaskMax << ": " << sw.duration() << " ns\n";
    }

    delete[] pmmOutOrg;
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

    return 0;
}

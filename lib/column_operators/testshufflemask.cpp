#include <cstdlib>
#include <iostream>
#include <random>
#include <array>
#include <omp.h>
#include "../../include/column_operators/SSE.hpp"
#include "../../include/util/stopwatch.hpp"

#ifndef NPROCS
#error "You must define the maximum number of threads you want to test! Use: -DNRPOCS=xxx"
// let's provide at least a default NPROCS value which should be valid for all modern CPUs
#define NPROCS 2
#endif

struct initcolumns_t {
};

struct initcolumns_in_t : public initcolumns_t {
};

struct initcolumns_out_t : public initcolumns_t {
};

struct initcolumns_inout_t : public initcolumns_t {
};

struct abstract_abstract_context_t {
    ahead::StopWatch sw;
    std::random_device rndDevice;
    std::default_random_engine rndEngine;
    const size_t widthValuesCout;

    abstract_abstract_context_t()
            : abstract_abstract_context_t(12) {
    }

    abstract_abstract_context_t(const size_t widthValuesCout)
            : sw(), rndDevice(), rndEngine(), widthValuesCout(widthValuesCout) {
    }
};

template<typename T>
struct abstract_context_t {
    typedef typename ahead::bat::ops::v2_mm128<T>::mask_t mask_t;

    constexpr static const size_t NUM_RND = 16;
    constexpr static const size_t MASK_ARR = 0x0F;

    abstract_abstract_context_t * paac;
    std::uniform_int_distribution<mask_t> rndDistr;
    mask_t randoms[NUM_RND];
    __m128i mm;
    const size_t NUM;
    const size_t NUM128;

    abstract_context_t(abstract_abstract_context_t & aac, size_t NUM)
            : paac(&aac), rndDistr(), mm(ahead::bat::ops::v2_mm128<T>::set_inc(0)), NUM(NUM), NUM128(NUM / (sizeof(__m128i ) / sizeof(T))) {
        for (size_t i = 0; i < NUM_RND; ++i) {
            randoms[i] = rndDistr(paac->rndEngine) % (sizeof(__m128i) / sizeof(T));
        }
    }
};

template<typename M1, typename M2>
struct column_initializer {
    static_assert(std::is_base_of<initcolumns_t, M1>::value, "M1 must be a subtype of initcolumns_t");
    static_assert(std::is_base_of<initcolumns_t, M2>::value, "M2 must be a subtype of initcolumns_t");

    static __m128i * init(size_t szColumn) {
        return nullptr;
    }
};

template<>
struct column_initializer<initcolumns_in_t, initcolumns_in_t> {
    static __m128i * init(size_t szColumn) {
        return new __m128i [szColumn];
    }
};

template<>
struct column_initializer<initcolumns_in_t, initcolumns_inout_t> {
    static __m128i * init(size_t szColumn) {
        return new __m128i [szColumn];
    }
};

template<>
struct column_initializer<initcolumns_out_t, initcolumns_out_t> {
    static __m128i * init(size_t szColumn) {
        return new __m128i [szColumn];
    }
};

template<>
struct column_initializer<initcolumns_out_t, initcolumns_inout_t> {
    static __m128i * init(size_t szColumn) {
        return new __m128i [szColumn];
    }
};

template<typename T, typename M>
struct concrete_context_t {
    using mask_t = typename abstract_context_t<T>::mask_t;

    abstract_context_t<T> * pac;
    __m128i * pmmIn;
    __m128i * pmmOut;

    concrete_context_t()
            : pac(nullptr), pmmIn(nullptr), pmmOut(nullptr) {
    }

    concrete_context_t(abstract_context_t<T> & ac)
            : pac(&ac), pmmIn(column_initializer<initcolumns_in_t, M>::init(ac.NUM128 + 1)), pmmOut(column_initializer<initcolumns_out_t, M>::init(ac.NUM128 + 1)) {
        if (pmmIn) {
            for (size_t i = 0; i < ac.NUM128; ++i) {
                _mm_storeu_si128(&pmmIn[i], ac.mm);
            }
        }
        if (pmmOut) {
            for (size_t i = 0; i < ac.NUM128; ++i) {
                _mm_storeu_si128(&pmmOut[i], ac.mm);
            }
        }
    }

    void init(abstract_context_t<T> & ac) {
        new (this) concrete_context_t(ac);
    }

    virtual ~concrete_context_t() {
        if (pmmIn) {
            delete[] pmmIn;
            pmmIn = nullptr;
        }
        if (pmmOut) {
            delete[] pmmOut;
            pmmOut = nullptr;
        }
    }
};

template<typename T, typename M>
using TestFn = void (*)(concrete_context_t<T, M> &);

template<typename T, typename M, TestFn<T, M> F, size_t N>
struct Test {
    typedef typename ahead::bat::ops::v2_mm128<T>::mask_t mask_t;

    static void run(abstract_context_t<T> & ac) {
        Test<T, M, F, N - 1>::run(ac);

        std::array<concrete_context_t<T, M>, N> ccs = {};
        std::for_each(ccs.begin(), ccs.end(), [&ac = ac] (concrete_context_t<T, M> & cc) {
                    cc.init(ac);
                });
        ac.paac->sw.start();
#pragma omp parallel for
        for (size_t i = 0; i < N; ++i) {
            F(ccs[i]);
        }
        ac.paac->sw.stop();
        std::cout << '\t' << std::setw(ac.paac->widthValuesCout) << ac.paac->sw.duration();
    }
};

template<typename T, typename M, TestFn<T, M> F>
struct Test<T, M, F, 0> {
    static void run(abstract_context_t<T> & ac) {
    }
};

template<typename T>
void testRndDistrScalar(concrete_context_t<T, initcolumns_out_t> & ctx) {
    T * pOut = reinterpret_cast<T*>(ctx.pmmOut);
    for (size_t i = 0; i < ctx.pac->NUM; ++i) {
        *pOut++ = ctx.pac->rndDistr(ctx.pac->paac->rndEngine);
    }
}

template<typename T>
void testRndDistrSSE(concrete_context_t<T, initcolumns_out_t> & ctx) {
    for (size_t i = 0; i < ctx.pac->NUM128; ++i) {
        ctx.pmmOut[i] = ahead::bat::ops::v2_mm128<T>::set1(ctx.pac->rndDistr(ctx.pac->paac->rndEngine));
    }
}

template<typename T>
void testPack1SingleRandom1(concrete_context_t<T, initcolumns_out_t> & ctx) {
    auto pmmOut = ctx.pmmOut;
    for (size_t i = 0; i < ctx.pac->NUM128; ++i) {
        typename concrete_context_t<T, initcolumns_out_t>::mask_t tmpMask = ctx.pac->rndDistr(ctx.pac->paac->rndEngine) % (sizeof(__m128i) / sizeof(T));
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        __m128i mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(ctx.pac->mm, tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
}

template<typename T>
void testPack1SingleRandom2(concrete_context_t<T, initcolumns_out_t> & ctx) {
    typename concrete_context_t<T, initcolumns_out_t>::mask_t mask = static_cast<typename concrete_context_t<T, initcolumns_out_t>::mask_t>(0x59A3);
    auto pmmOut = ctx.pmmOut;
    for (size_t i = 0; i < ctx.pac->NUM128; ++i) {
        size_t nMaskOnes = __builtin_popcountll(mask);
        __m128i mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(ctx.pac->mm, mask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
        mask *= mask;
    }
}

template<typename T>
void testPack1SingleArray(concrete_context_t<T, initcolumns_out_t> & ctx) {
    auto pmmOut = ctx.pmmOut;
    for (size_t i = 0; i < ctx.pac->NUM128; ++i) {
        typename concrete_context_t<T, initcolumns_out_t>::mask_t tmpMask = ctx.pac->randoms[i & abstract_context_t<T>::MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        __m128i mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(ctx.pac->mm, tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
}

template<typename T>
void testPack1ColumnRandom1(concrete_context_t<T, initcolumns_inout_t> & ctx) {
    auto pmmIn = ctx.pmmIn;
    auto pmmOut = ctx.pmmOut;
    for (size_t i = 0; i < ctx.pac->NUM128; ++i) {
        typename concrete_context_t<T, initcolumns_inout_t>::mask_t tmpMask = ctx.pac->rndDistr(ctx.pac->paac->rndEngine) % (sizeof(__m128i) / sizeof(T));
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        __m128i mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(_mm_lddqu_si128(pmmIn++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
}

template<typename T>
void testPack1ColumnRandom2(concrete_context_t<T, initcolumns_inout_t> & ctx) {
    auto pmmIn = ctx.pmmIn;
    auto pmmOut = ctx.pmmOut;
    typename concrete_context_t<T, initcolumns_inout_t>::mask_t mask = static_cast<typename concrete_context_t<T, initcolumns_inout_t>::mask_t>(0x59A3);
    for (size_t i = 0; i < ctx.pac->NUM128; ++i) {
        typename concrete_context_t<T, initcolumns_inout_t>::mask_t tmpMask = mask % (sizeof(__m128i) / sizeof(T));
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        __m128i mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(_mm_lddqu_si128(pmmIn++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
        mask *= mask;
    }
}

template<typename T>
void testPack1ColumnArray(concrete_context_t<T, initcolumns_inout_t> & ctx) {
    auto pmmIn = ctx.pmmIn;
    auto pmmOut = ctx.pmmOut;
    for (size_t i = 0; i < ctx.pac->NUM128; ++i) {
        typename concrete_context_t<T, initcolumns_inout_t>::mask_t tmpMask = ctx.pac->randoms[i & abstract_context_t<T>::MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        __m128i mmResult = ahead::bat::ops::v2_mm128<T>::pack_right(_mm_lddqu_si128(pmmIn++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
}

template<typename T>
void testPack2ColumnArray(concrete_context_t<T, initcolumns_inout_t> & ctx) {
    auto pmmIn = ctx.pmmIn;
    auto pOut = reinterpret_cast<T*>(ctx.pmmOut);
    for (size_t i = 0; i < ctx.pac->NUM128; ++i) {
        typename concrete_context_t<T, initcolumns_inout_t>::mask_t tmpMask = ctx.pac->randoms[i & abstract_context_t<T>::MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        ahead::bat::ops::v2_mm128<T>::pack_right2(pOut, _mm_lddqu_si128(pmmIn++), tmpMask);
    }
}

template<typename T>
void test(size_t NUM, abstract_abstract_context_t & aac) {
    abstract_context_t<T> ac(aac, NUM);
    std::cout << "\tdata size: " << (sizeof(T) * 8) << " bits\n\t# 128-bit vectors: " << ac.NUM128 << std::endl;

    constexpr static const size_t nprocs = NPROCS;
/*
    std::cout << "\t\t" << std::setw(30) << "rndDistrScalar(rndEngine)";
    Test<T, initcolumns_out_t, testRndDistrScalar, nprocs>::run(ac);
    std::cout << std::endl;

    std::cout << "\t\t" << std::setw(30) << "rndDistrSSE(rndEngine)";
    Test<T, initcolumns_out_t, testRndDistrSSE, nprocs>::run(ac);
    std::cout << std::endl;

    std::cout << "\t\t" << std::setw(30) << "pack_right1 (single, random)";
    Test<T, initcolumns_out_t, testPack1SingleRandom1, nprocs>::run(ac);
    std::cout << std::endl;

    std::cout << "\t\t" << std::setw(30) << "pack_right1 (single, random2)";
    Test<T, initcolumns_out_t, testPack1SingleRandom2, nprocs>::run(ac);
    std::cout << std::endl;

    std::cout << "\t\t" << std::setw(30) << "pack_right1 (single, array)";
    Test<T, initcolumns_out_t, testPack1SingleArray, nprocs>::run(ac);
    std::cout << std::endl;

    std::cout << "\t\t" << std::setw(30) << "pack_right1 (column, random)";
    Test<T, initcolumns_inout_t, testPack1ColumnRandom1, nprocs>::run(ac);
    std::cout << std::endl;

    std::cout << "\t\t" << std::setw(30) << "pack_right1 (column, random2)";
    Test<T, initcolumns_inout_t, testPack1ColumnRandom2, nprocs>::run(ac);
    std::cout << std::endl;
*/
    std::cout << "\t\t" << std::setw(30) << "pack_right1 (column, array)";
    Test<T, initcolumns_inout_t, testPack1ColumnArray, nprocs>::run(ac);
    std::cout << std::endl;

    std::cout << "\t\t" << std::setw(30) << "pack_right2 (column, array)";
    Test<T, initcolumns_inout_t, testPack2ColumnArray, nprocs>::run(ac);
    std::cout << std::endl;
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

    abstract_abstract_context_t aac;

    test<uint8_t>(NUM, aac);
    test<uint16_t>(NUM, aac);
    test<uint32_t>(NUM, aac);
}


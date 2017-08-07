#include <cstdlib>
#include <iostream>
#include <random>
#include <array>
#include <exception>
#include <omp.h>
#include <column_operators/SSE/SSE.hpp>
#include <util/stopwatch.hpp>

#ifndef NPROCS
#error "You must define the maximum number of threads you want to test! Use: -DNRPOCS=xxx"
// let's provide at least a default NPROCS value which should be valid for all modern CPUs
#define NPROCS 2
#endif

struct initcolumns_t {
};

struct initcolumns_in_t :
        public initcolumns_t {
};

struct initcolumns_out_t :
        public initcolumns_t {
};

struct initcolumns_inout_t :
        public initcolumns_t {
};

struct abstract_abstract_context_t {

    constexpr static const size_t NUM_RND = 256;
    constexpr static const size_t MASK_ARR = NUM_RND - 1;

    ahead::StopWatch sw;
    std::random_device rndDevice;
    std::default_random_engine rndEngine;
    const size_t numValues;
    const size_t numRepititions;
    const size_t widthValuesCout;

    abstract_abstract_context_t(
            const size_t numValues)
            : abstract_abstract_context_t(numValues, 1) {
    }

    abstract_abstract_context_t(
            const size_t numValues,
            const size_t numRepititions)
            : abstract_abstract_context_t(numValues, numRepititions, 12) {
    }

    abstract_abstract_context_t(
            const size_t numValues,
            const size_t numRepititions,
            const size_t widthValuesCout)
            : sw(),
              rndDevice(),
              rndEngine(rndDevice()),
              numValues(numValues),
              numRepititions(numRepititions),
              widthValuesCout(widthValuesCout) {
    }
};

template<typename VectorT, typename TypeT, typename MaskT>
struct ac_helper;

template<>
struct ac_helper<__m128i, uint8_t, uint16_t> {
    static const size_t SHIFT = 4;
    static const size_t MASK = 0xF;
};

template<>
struct ac_helper<__m128i, uint16_t, uint8_t> {
    static const size_t SHIFT = 3;
    static const size_t MASK = 0x7;
};

template<>
struct ac_helper<__m128i, uint32_t, uint8_t> {
    static const size_t SHIFT = 2;
    static const size_t MASK = 0x3;
};

template<>
struct ac_helper<__m128i, uint64_t, uint8_t> {
    static const size_t SHIFT = 1;
    static const size_t MASK = 0x1;
};

template<typename T>
struct abstract_context_t {
    typedef typename ahead::bat::ops::sse::v2_mm128<T>::mask_t mask_t;
    typedef ac_helper<__m128i, T, mask_t> helper;

    abstract_abstract_context_t * paac;
    __m128i mm;
    const size_t numMM128;
    std::uniform_int_distribution<size_t> rndDistr;
    mask_t randoms[abstract_abstract_context_t::NUM_RND];

    abstract_context_t(
            abstract_abstract_context_t & aac)
            : paac(&aac),
              mm(ahead::bat::ops::sse::v2_mm128<T>::set_inc(0)),
              numMM128(aac.numValues / (sizeof(__m128i ) / sizeof(T))),
              rndDistr(0, abstract_abstract_context_t::NUM_RND * (sizeof(__m128i) / sizeof(T)) - 1) {
            }

            void setSelectivity(size_t selectivity) {
                if (selectivity > 100) {
                    throw std::runtime_error("abstract_context_t::setSelectivity : Selectivity must not be >100");
                } else if (selectivity == 0) {
                    // set no bit
                    for (size_t i = 0; i < abstract_abstract_context_t::NUM_RND; ++i) {
                        randoms[i] = static_cast<mask_t>(0);
                    }
                } else if (selectivity == 100) {
                    // set all bits
                    for (size_t i = 0; i < abstract_abstract_context_t::NUM_RND; ++i) {
                        randoms[i] = static_cast<mask_t>(-1);
                    }
                } else {
                    for (size_t i = 0; i < abstract_abstract_context_t::NUM_RND; ++i) {
                        randoms[i] = static_cast<mask_t>(0);
                    }
                    double dSel = static_cast<double>(selectivity) / 100.0;
                    size_t numMaskBitsTotal = abstract_abstract_context_t::NUM_RND * (sizeof(__m128i) / sizeof(T));
                    size_t numMaskBitsSet = 0;
                    double ratio = 0.0;
                    do {
                        size_t bit = rndDistr(paac->rndEngine);
                        mask_t subMask = randoms[bit >> helper::SHIFT];
                        if (0 == (subMask & (1 << (bit & helper::MASK)))) {
                            randoms[bit >> helper::SHIFT] = subMask | (1 << (bit & helper::MASK));
                            ++numMaskBitsSet;
                            ratio = static_cast<double>(numMaskBitsSet) / static_cast<double>(numMaskBitsTotal);
                        }
                    }while (ratio < dSel);
                }
                size_t numMaskBitsTotal = abstract_abstract_context_t::NUM_RND * (sizeof(__m128i) / sizeof(T));
                size_t numMaskBitsSet = 0;
                for (size_t i = 0; i < abstract_abstract_context_t::NUM_RND; ++i) {
                    numMaskBitsSet += __builtin_popcountll(static_cast<size_t>(randoms[i]));
                }
                std::cout << (static_cast<double>(numMaskBitsSet) / static_cast<double>(numMaskBitsTotal));
            }
        };

template<typename M1, typename M2>
struct column_initializer {
    static_assert(std::is_base_of<initcolumns_t, M1>::value, "M1 must be a subtype of initcolumns_t");
    static_assert(std::is_base_of<initcolumns_t, M2>::value, "M2 must be a subtype of initcolumns_t");

    static __m128i * init(
            size_t szColumn) {
        return nullptr;
    }
};

template<>
struct column_initializer<initcolumns_in_t, initcolumns_in_t> {
    static __m128i * init(
            size_t szColumn) {
        return new __m128i [szColumn];
    }
};

template<>
struct column_initializer<initcolumns_in_t, initcolumns_inout_t> {
    static __m128i * init(
            size_t szColumn) {
        return new __m128i [szColumn];
    }
};

template<>
struct column_initializer<initcolumns_out_t, initcolumns_out_t> {
    static __m128i * init(
            size_t szColumn) {
        return new __m128i [szColumn];
    }
};

template<>
struct column_initializer<initcolumns_out_t, initcolumns_inout_t> {
    static __m128i * init(
            size_t szColumn) {
        return new __m128i [szColumn];
    }
};

template<typename T, typename M>
struct concrete_context_t {
    using mask_t = typename abstract_context_t<T>::mask_t;

    // TODO: copy lookup tables into the concrete context for pack_right_1

    abstract_context_t<T> * pac;
    __m128i * pmmIn;
    __m128i * pmmOut;

    concrete_context_t()
            : pac(nullptr),
              pmmIn(nullptr),
              pmmOut(nullptr) {
    }

    concrete_context_t(
            abstract_context_t<T> & ac)
            : pac(&ac),
              pmmIn(column_initializer<initcolumns_in_t, M>::init(ac.numMM128 + 1)),
              pmmOut(column_initializer<initcolumns_out_t, M>::init(ac.numMM128 + 1)) {
        if (pmmIn) {
            for (size_t i = 0; i < ac.numMM128; ++i) {
                _mm_storeu_si128(&pmmIn[i], ac.mm);
            }
        }
        if (pmmOut) {
            for (size_t i = 0; i < ac.numMM128; ++i) {
                _mm_storeu_si128(&pmmOut[i], ac.mm);
            }
        }
    }

    void init(
            abstract_context_t<T> & ac) {
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
    typedef typename ahead::bat::ops::sse::v2_mm128<T>::mask_t mask_t;

    static void run(
            abstract_context_t<T> & ac) {
        Test<T, M, F, N - 1>::run(ac);

        std::array<concrete_context_t<T, M>, N> ccs = {};
        std::for_each(ccs.begin(), ccs.end(), [&ac = ac] (concrete_context_t<T, M> & cc) {
                    cc.init(ac);
                });
        ac.paac->sw.start();
#pragma omp parallel for
        for (size_t i = 0; i < N; ++i) {
            for (size_t r = 0; r < ac.paac->numRepititions; ++r) {
                F(ccs[i]);
            }
        }
        ac.paac->sw.stop();
        std::cout << ',' << ac.paac->sw.duration();
    }
};

template<typename T, typename M, TestFn<T, M> F>
struct Test<T, M, F, 0> {
    static void run(
            abstract_context_t<T> & ac) {
    }
};

template<typename T>
void testPack1ColumnArray(
        concrete_context_t<T, initcolumns_inout_t> & ctx) {
    typedef typename concrete_context_t<T, initcolumns_inout_t>::mask_t mask_t;
    auto pmmIn = ctx.pmmIn;
    auto pmmOut = ctx.pmmOut;
    for (size_t i = 0; i < ctx.pac->numMM128; ++i) {
        mask_t tmpMask = ctx.pac->randoms[i & abstract_abstract_context_t::MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        __m128i mmResult = ahead::bat::ops::sse::v2_mm128<T>::pack_right(_mm_lddqu_si128(pmmIn++), tmpMask);
        _mm_storeu_si128(pmmOut, mmResult);
        pmmOut = reinterpret_cast<__m128i *>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
}

template<typename T>
void testPack2ColumnArray(
        concrete_context_t<T, initcolumns_inout_t> & ctx) {
    typedef typename concrete_context_t<T, initcolumns_inout_t>::mask_t mask_t;
    auto pmmIn = ctx.pmmIn;
    auto pOut = reinterpret_cast<T*>(ctx.pmmOut);
    for (size_t i = 0; i < ctx.pac->numMM128; ++i) {
        mask_t tmpMask = ctx.pac->randoms[i & abstract_abstract_context_t::MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        ahead::bat::ops::sse::v2_mm128<T>::pack_right2(pOut, _mm_lddqu_si128(pmmIn++), tmpMask);
    }
}

template<typename T>
void test(
        abstract_abstract_context_t & aac) {
    constexpr static const size_t nprocs = 1; // NPROCS;
    abstract_context_t<T> ac(aac);
    std::cout << "# #data bits\t" << (sizeof(T) * 8) << "\n#128-bit vectors\t" << (ac.numMM128 * aac.numRepititions) << "\t(" << ac.numMM128 << " repeated " << aac.numRepititions << " times)"
            << std::endl;
    std::cout << "selectivity,lookup table,iteration\n";
    for (size_t selectivity = 0; selectivity <= 100; selectivity += 10) {
        ac.setSelectivity(selectivity);
        Test<T, initcolumns_inout_t, testPack1ColumnArray, nprocs>::run(ac);
        Test<T, initcolumns_inout_t, testPack2ColumnArray, nprocs>::run(ac);
        std::cout << '\n';
    }
    std::cout << "\n\n" << std::endl;
}

int main(
        int argc,
        char ** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <#values> (<#repititions>)" << std::endl;
        return 1;
    }

    char* end;
    const size_t numValues = strtoull(argv[1], &end, 10);
    size_t numRepititions = 1;
    if (numValues == 0) {
        std::cerr << "<#values> is invalid! Must be an integer > 0." << std::endl;
        return 2;
    }
    if (argc > 2) {
        numRepititions = strtoull(argv[2], &end, 10);
        if (numRepititions == 0) {
            std::cerr << "<#repititions> is invalid! Must be an integer > 0." << std::endl;
            return 3;
        }
    }
    if (numValues % abstract_abstract_context_t::NUM_RND) {
        std::cerr << "<#values> must be divisable by " << abstract_abstract_context_t::NUM_RND << std::endl;
        return 4;
    }

    std::cout << "# #total values: " << (numValues * numRepititions) << "\t(" << numValues << " repeated " << numRepititions << " times)" << std::endl;
    abstract_abstract_context_t aac(numValues, numRepititions);

    test<uint8_t>(aac);
    test<uint16_t>(aac);
    test<uint32_t>(aac);
    test<uint64_t>(aac);
}

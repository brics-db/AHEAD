#include <cstdlib>
#include <iostream>
#include <random>
#include <array>
#include <exception>
#include <omp.h>
#include <ColumnStore.h>
#include "../lib/column_operators/SIMD/SIMD.hpp"
#include <util/stopwatch.hpp>

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

    static const constexpr size_t NUM_RND = 64;
    static const constexpr size_t MASK_ARR = NUM_RND - 1;

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
    static const size_t FULL = 0xFFFF;
};

template<>
struct ac_helper<__m128i, uint16_t, uint8_t> {
    static const size_t SHIFT = 3;
    static const size_t MASK = 0x7;
    static const size_t FULL = 0xFF;
};

template<>
struct ac_helper<__m128i, uint32_t, uint8_t> {
    static const size_t SHIFT = 2;
    static const size_t MASK = 0x3;
    static const size_t FULL = 0x0F;
};

template<>
struct ac_helper<__m128i, uint64_t, uint8_t> {
    static const size_t SHIFT = 1;
    static const size_t MASK = 0x1;
    static const size_t FULL = 0x03;
};

#ifdef __AVX2__
template<>
struct ac_helper<__m256i, uint8_t, uint32_t> {
    static const size_t SHIFT = 5;
    static const size_t MASK = 0x1F;
    static const size_t FULL = 0xFFFFFFFF;
};

template<>
struct ac_helper<__m256i, uint16_t, uint16_t> {
    static const size_t SHIFT = 4;
    static const size_t MASK = 0x0F;
    static const size_t FULL = 0xFFFF;
};

template<>
struct ac_helper<__m256i, uint32_t, uint8_t> {
    static const size_t SHIFT = 3;
    static const size_t MASK = 0x07;
    static const size_t FULL = 0xFF;
};

template<>
struct ac_helper<__m256i, uint64_t, uint8_t> {
    static const size_t SHIFT = 2;
    static const size_t MASK = 0x03;
    static const size_t FULL = 0x0F;
};
#endif

#ifdef AHEAD_AVX512
template<>
struct ac_helper<__m512i, uint8_t, uint64_t> {
    static const size_t SHIFT = 6;
    static const size_t MASK = 0x2F;
    static const size_t FULL = 0xFFFFFFFFFFFFFFFF;
};

template<>
struct ac_helper<__m512i, uint16_t, uint32_t> {
    static const size_t SHIFT = 5;
    static const size_t MASK = 0x1F;
    static const size_t FULL = 0xFFFFFFFF;
};

template<>
struct ac_helper<__m512i, uint32_t, uint16_t> {
    static const size_t SHIFT = 4;
    static const size_t MASK = 0x0F;
    static const size_t FULL = 0xFFFF;
};

template<>
struct ac_helper<__m512i, uint64_t, uint8_t> {
    static const size_t SHIFT = 3;
    static const size_t MASK = 0x07;
    static const size_t FULL = 0xFF;
};
#endif

template<typename V, typename T>
struct abstract_context_t {
    typedef typename ahead::bat::ops::simd::mm<V, T>::mask_t mask_t;
    typedef ac_helper<V, T, mask_t> helper;

    abstract_abstract_context_t * paac;
    V mm;
    const size_t numMM;
    std::uniform_int_distribution<size_t> rndDistr;
    mask_t randoms[abstract_abstract_context_t::NUM_RND];

    abstract_context_t(
            abstract_abstract_context_t & aac)
            : paac(&aac),
              mm(ahead::bat::ops::simd::mm<V, T>::set_inc(0)),
              numMM(aac.numValues / (sizeof(V) / sizeof(T))),
              rndDistr(0, abstract_abstract_context_t::NUM_RND * (sizeof(V) / sizeof(T)) - 1) {
    }

    void setSelectivity(
            size_t selectivity) {
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
                randoms[i] = helper::FULL;
            }
        } else {
            for (size_t i = 0; i < abstract_abstract_context_t::NUM_RND; ++i) {
                randoms[i] = static_cast<mask_t>(0);
            }
            double dSel = static_cast<double>(selectivity) / 100.0;
            size_t numMaskBitsTotal = abstract_abstract_context_t::NUM_RND * (sizeof(V) / sizeof(T));
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
            } while (ratio < dSel);
        }
        size_t numMaskBitsTotal = abstract_abstract_context_t::NUM_RND * (sizeof(V) / sizeof(T));
        size_t numMaskBitsSet = 0;
        for (size_t i = 0; i < abstract_abstract_context_t::NUM_RND; ++i) {
            numMaskBitsSet += __builtin_popcountll(static_cast<size_t>(randoms[i]));
        }
        std::cout << std::fixed << (static_cast<double>(numMaskBitsSet) / static_cast<double>(numMaskBitsTotal)) << std::defaultfloat;
    }
};

template<typename V, typename M1, typename M2>
struct column_initializer {
    static_assert(std::is_base_of<initcolumns_t, M1>::value, "M1 must be a subtype of initcolumns_t");
    static_assert(std::is_base_of<initcolumns_t, M2>::value, "M2 must be a subtype of initcolumns_t");

    static V * init(
            size_t szColumn) {
        return nullptr;
    }
};

template<typename V>
struct column_initializer<V, initcolumns_in_t, initcolumns_in_t> {
    static V * init(
            size_t szColumn) {
        return new V[szColumn];
    }
};

template<typename V>
struct column_initializer<V, initcolumns_in_t, initcolumns_inout_t> {
    static V * init(
            size_t szColumn) {
        return new V[szColumn];
    }
};

template<typename V>
struct column_initializer<V, initcolumns_out_t, initcolumns_out_t> {
    static V * init(
            size_t szColumn) {
        return new V[szColumn];
    }
};

template<typename V>
struct column_initializer<V, initcolumns_out_t, initcolumns_inout_t> {
    static V * init(
            size_t szColumn) {
        return new V[szColumn];
    }
};

template<typename V, typename T, typename M>
struct concrete_context_t {
    using mask_t = typename abstract_context_t<V, T>::mask_t;

    // TODO: copy lookup tables into the concrete context for pack_right_1

    abstract_context_t<V, T> * pac;
    V * pmmInUnaligned;
    V * pmmIn;
    V * pmmOutUnaligned;
    V * pmmOut;

    concrete_context_t()
            : pac(nullptr),
              pmmInUnaligned(nullptr),
              pmmIn(nullptr),
              pmmOutUnaligned(nullptr),
              pmmOut(nullptr) {
    }

    concrete_context_t(
            abstract_context_t<V, T> & ac)
            : pac(&ac),
              pmmInUnaligned(column_initializer<V, initcolumns_in_t, M>::init(ac.numMM + 2)),
              pmmIn(ahead::align_to<sizeof(V)>(pmmInUnaligned)),
              pmmOutUnaligned(column_initializer<V, initcolumns_out_t, M>::init(ac.numMM + 2)),
              pmmOut(ahead::align_to<sizeof(V)>(pmmOutUnaligned)) {
        if (pmmIn) {
            for (size_t i = 0; i < ac.numMM; ++i) {
                pmmIn[i] = ac.mm;
            }
        }
        if (pmmOut) {
            for (size_t i = 0; i < ac.numMM; ++i) {
                pmmOut[i] = ac.mm;
            }
        }
    }

    void init(
            abstract_context_t<V, T> & ac) {
        new (this) concrete_context_t(ac);
    }

    virtual ~concrete_context_t() {
        if (pmmInUnaligned) {
            delete[] pmmInUnaligned;
            pmmInUnaligned = nullptr;
            pmmIn = nullptr;
        }
        if (pmmOutUnaligned) {
            delete[] pmmOutUnaligned;
            pmmOutUnaligned = nullptr;
            pmmOut = nullptr;
        }
    }
};

template<typename V, typename T, typename M>
using TestFn = void (*)(concrete_context_t<V, T, M> &);

template<typename V, typename T, typename M, TestFn<V, T, M> F>
struct Test {
    typedef typename ahead::bat::ops::simd::mm<V, T>::mask_t mask_t;

    static void run(
            abstract_context_t<V, T> & ac) {
        concrete_context_t<V, T, M> cc(ac);
        ac.paac->sw.start();
        for (size_t r = 0; r < ac.paac->numRepititions; ++r) {
            F(cc);
        }
        ac.paac->sw.stop();
        std::cout << ',' << ac.paac->sw.duration();
    }
};

template<typename V, typename T>
void testPack1ColumnArray(
        concrete_context_t<V, T, initcolumns_inout_t> & ctx) {
    typedef typename concrete_context_t<V, T, initcolumns_inout_t>::mask_t mask_t;
    auto pmmIn = ctx.pmmIn;
    auto pmmOut = ctx.pmmOut;
    for (size_t i = 0; i < ctx.pac->numMM; ++i) {
        mask_t tmpMask = ctx.pac->randoms[i & abstract_abstract_context_t::MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        auto mmResult = ahead::bat::ops::simd::mm<V, T>::pack_right(ahead::bat::ops::simd::mm<V, T>::loadu(pmmIn++), tmpMask);
        ahead::bat::ops::simd::mm<V, T>::storeu(pmmOut, mmResult);
        pmmOut = reinterpret_cast<V*>(reinterpret_cast<T*>(pmmOut) + nMaskOnes);
    }
}

template<typename V, typename T>
void testPack2ColumnArray(
        concrete_context_t<V, T, initcolumns_inout_t> & ctx) {
    typedef typename concrete_context_t<V, T, initcolumns_inout_t>::mask_t mask_t;
    auto pmmIn = ctx.pmmIn;
    auto pOut = reinterpret_cast<T*>(ctx.pmmOut);
    for (size_t i = 0; i < ctx.pac->numMM; ++i) {
        mask_t tmpMask = ctx.pac->randoms[i & abstract_abstract_context_t::MASK_ARR];
        size_t nMaskOnes = __builtin_popcountll(tmpMask);
        ahead::bat::ops::simd::mm<V, T>::pack_right2(pOut, ahead::bat::ops::simd::mm<V, T>::loadu(pmmIn++), tmpMask);
    }
}

template<typename V>
struct vector_length_string;

template<>
struct vector_length_string<__m128i> {
static const constexpr char * const value = "128";
};

#ifdef __AVX2__
template<>
struct vector_length_string<__m256i> {
static constexpr const char * const value = "256";
};
#endif

#ifdef __AVX512F__
template<>
struct vector_length_string<__m512i> {
static constexpr const char * const value = "512";
};
#endif

template<typename V, typename T>
void test(
        abstract_abstract_context_t & aac) {
    abstract_context_t<V, T> ac(aac);
    std::cout << "# " << std::setw(20) << "data bits: " << (sizeof(T) * 8);
    std::cout << "\n#" << std::setw(6) << vector_length_string<V>::value << "-bit vectors: " << (ac.numMM * aac.numRepititions) << " (" << ac.numMM << " repeated " << aac.numRepititions
            << " times)\n";
    std::cout << "selectivity,lookup table,iteration" << std::endl;
    for (size_t selectivity = 0; selectivity <= 100; selectivity++) {
        ac.setSelectivity(selectivity);
        Test<V, T, initcolumns_inout_t, testPack1ColumnArray>::run(ac);
        Test<V, T, initcolumns_inout_t, testPack2ColumnArray>::run(ac);
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

    test<__m128i, uint8_t>(aac);
    test<__m128i, uint16_t>(aac);
    test<__m128i, uint32_t>(aac);
    test<__m128i, uint64_t>(aac);

#ifdef __AVX2__
    test<__m256i, uint8_t>(aac);
    test<__m256i, uint16_t>(aac);
    test<__m256i, uint32_t>(aac);
    test<__m256i, uint64_t>(aac);
#endif

#ifdef AHEAD_AVX512
    test<__m512i, uint8_t>(aac);
    test<__m512i, uint16_t>(aac);
    test<__m512i, uint32_t>(aac);
    test<__m512i, uint64_t>(aac);
#endif
}

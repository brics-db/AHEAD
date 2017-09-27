// Copyright (c) 2016-2017 Till Kolditz
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
 * File:   encdecAN_SSE.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:22
 */

#ifndef ENCDECAN_SSE_TCC
#define ENCDECAN_SSE_TCC

#include <type_traits>
#include <immintrin.h>

#include <column_storage/TempStorage.hpp>
#include <column_operators/ANbase.hpp>
#include "SSEAN.hpp"
#include "../miscellaneous.hpp"

#ifdef __GNUC__
#pragma GCC target "sse4.2"
#else
#warning "Forcing SSE 4.2 code is not yet implemented for this compiler"
#endif

namespace ahead {
    namespace bat {
        namespace ops {
            namespace simd {
                namespace sse {

                    /*
                     template<typename Head, typename Tail>
                     BAT<Head, typename TypeMap<Tail>::v2_encoded_t>*
                     encodeAN(BAT<Head, Tail>* arg, typename TypeMap<Tail>::v2_encoded_t::type_t A = std::get<ANParametersSelector<Tail>::As->size() - 1>(*ANParametersSelector<Tail>::As)) {

                     typedef typename TypeMap<Tail>::v2_encoded_t enctail_t;
                     typedef typename enctail_t::type_t tail_t;
                     typedef typename BAT<Head, enctail_t>::coldesc_head_t cd_head_t;
                     typedef typename BAT<Head, enctail_t>::coldesc_tail_t cd_tail_t;

                     static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                     static_assert(std::is_base_of<v2_base_t, Tail>::value, "Tail must be a base type");

                     auto result = new TempBAT<Head, enctail_t>(cd_head_t(arg->head.metaData),
                     cd_tail_t(ColumnMetaData(sizeof(typename TypeMap<Tail>::v2_encoded_t::type_t), A, ext_euclidean(A, sizeof(tail_t)), enctail_t::UNENC_MAX_U, enctail_t::UNENC_MIN)));
                     result->reserve(arg->size());
                     auto iter = arg->begin();
                     for (; iter->hasNext(); ++*iter) {
                     result->append(std::make_pair(iter->head(), static_cast<tail_t>(iter->tail()) * A));
                     }
                     delete iter;
                     return result; // possibly empty
                     }

                     template<typename Head, typename ResTail>
                     std::vector<bool>*
                     checkAN(BAT<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {

                     typedef typename ResTail::type_t res_t;

                     static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                     static_assert(std::is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");

                     res_t Ainv = static_cast<res_t>(arg->tail.metaData.AN_Ainv);

                     auto result = new std::vector<bool>(arg->size());
                     auto iter = arg->begin();
                     for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                     if (static_cast<res_t>(iter->tail() * aInv) > unEncMaxU) {
                     (*result)[i] = true;
                     }
                     }
                     delete iter;
                     return result; // possibly empty
                     }

                     template<typename Head, typename ResTail>
                     std::pair<TempBAT<Head, typename ResTail::v2_unenc_t>*, std::vector<bool>*> decodeAN(BAT<Head, ResTail>* arg) {

                     typedef typename ResTail::type_t restail_t;
                     typedef typename ResTail::v2_unenc_t Tail;
                     typedef typename Tail::type_t tail_t;

                     static_assert(std::is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                     static_assert(std::is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");

                     restail_t Ainv = static_cast<restail_t>(arg->tail.metaData.AN_Ainv);
                     restail_t unencMaxU = static_cast<restail_t>(arg->tail.metaData.AN_unencMaxU);
                     auto result = skeletonHead<Head, Tail>(arg);
                     result->reserve(arg->size());
                     auto vec = new std::vector<bool>(arg->size());
                     auto iter = arg->begin();
                     size_t pos = 0;
                     for (; iter->hasNext(); ++*iter, ++pos) {
                     auto t = static_cast<restail_t>(iter->tail() * Ainv);
                     result->append(std::make_pair(iter->head(), static_cast<tail_t>(t)));
                     if (t > unencMaxU) {
                     (*vec)[pos] = true;
                     }
                     }
                     delete iter;
                     return std::make_pair(result, vec);
                     }
                     */

                    namespace Private {
                        template<typename Head, typename Tail>
                        struct CheckAndDecodeAN {
                            typedef typename Head::type_t head_t;
                            typedef typename Tail::type_t tail_t;
                            typedef typename Head::v2_unenc_t v2_head_unenc_t;
                            typedef typename Tail::v2_unenc_t v2_tail_unenc_t;
                            typedef typename v2_head_unenc_t::type_t head_unenc_t;
                            typedef typename v2_tail_unenc_t::type_t tail_unenc_t;
                            typedef typename smaller_type<head_t, tail_t>::type_t smaller_t;
                            typedef typename smaller_type<head_unenc_t, tail_unenc_t>::type_t smaller_unenc_t;
                            typedef typename larger_type<head_t, tail_t>::type_t larger_t;
                            typedef typename larger_type<head_unenc_t, tail_unenc_t>::type_t larger_unenc_t;
                            typedef typename v2_mm128_AN<smaller_t>::mask_t smaller_mask_t;
                            typedef typename v2_mm128_AN<larger_t>::mask_t larger_mask_t;
                            typedef typename std::tuple<BAT<v2_head_unenc_t, v2_tail_unenc_t>*, std::vector<bool>*, std::vector<bool>*> result_t;

                            static result_t doIt(
                                    BAT<Head, Tail>* arg) {
                                static_assert(std::is_base_of<v2_anencoded_t, Head>::value || std::is_base_of<v2_anencoded_t, Tail>::value, "At least one of Head and Tail must be an AN-encoded type");

                                constexpr const bool isHeadSmaller = larger_type<head_t, tail_t>::isSecondLarger;
                                constexpr const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                                constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                                constexpr const size_t smallersPerMM128 = sizeof(__m128i) / sizeof (smaller_t);
                                constexpr const size_t largersPerMM128 = sizeof(__m128i) / sizeof (larger_t);
                                constexpr const size_t factor = sizeof(larger_t) / sizeof(smaller_t);
                                constexpr const bool isSmallerEncoded = isHeadSmaller ? isHeadEncoded : isTailEncoded;
                                constexpr const bool isLargerEncoded = isHeadSmaller ? isTailEncoded : isHeadEncoded;

                                oid_t szArg = arg->size();

                                std::vector<bool> *vec1 = (isHeadEncoded ? new std::vector<bool>(szArg) : nullptr);
                                std::vector<bool> *vec2 = (isTailEncoded ? new std::vector<bool>(szArg) : nullptr);
                                auto vecS = isHeadSmaller ? vec1 : vec2;
                                auto vecL = isHeadSmaller ? vec2 : vec1;

                                auto result = new TempBAT<v2_head_unenc_t, v2_tail_unenc_t>();
                                result->reserve(szArg + smallersPerMM128); // reserve more data to compensate for writing after the last bytes, since writing the very last vector will write 16 Bytes and not just the remaining ones

                                auto pmmH = reinterpret_cast<__m128i *>(arg->head.container->data());
                                auto pmmHEnd = reinterpret_cast<__m128i *>(arg->head.container->data() + szArg);
                                auto pmmT = reinterpret_cast<__m128i *>(arg->tail.container->data());
                                auto pmmTEnd = reinterpret_cast<__m128i *>(arg->tail.container->data() + szArg);
                                __m128i * pmmS = isHeadSmaller ? pmmH : pmmT;
                                __m128i * pmmSEnd = isHeadSmaller ? pmmHEnd : pmmTEnd;
                                __m128i * pmmL = isHeadSmaller ? pmmT : pmmH;
                                __m128i mmDecS, mmDecL;
                                auto pRH = reinterpret_cast<head_unenc_t*>(result->head.container->data());
                                auto pRT = reinterpret_cast<tail_unenc_t*>(result->tail.container->data());
                                auto pmmRS = reinterpret_cast<__m128i *>(smaller_type<head_t, tail_t>::get(reinterpret_cast<head_t *>(pRH), reinterpret_cast<tail_t *>(pRT)));
                                auto pmmRL = reinterpret_cast<__m128i *>(larger_type<head_t, tail_t>::get(reinterpret_cast<head_t *>(pRH), reinterpret_cast<tail_t *>(pRT)));

                                head_t hAinv = static_cast<head_t>(arg->head.metaData.AN_Ainv);
                                head_t hUnencMaxU = static_cast<head_t>(arg->head.metaData.AN_unencMaxU);
                                tail_t tAinv = static_cast<tail_t>(arg->tail.metaData.AN_Ainv);
                                tail_t tUnencMaxU = static_cast<tail_t>(arg->tail.metaData.AN_unencMaxU);
                                auto mmASinv = isHeadSmaller ? mm128<head_t>::set1(hAinv) : mm128<tail_t>::set1(tAinv);
                                auto mmALinv = isHeadSmaller ? mm128<tail_t>::set1(tAinv) : mm128<head_t>::set1(hAinv);
                                auto mmASDmax = isHeadSmaller ? mm128<head_t>::set1(hUnencMaxU) : mm128<tail_t>::set1(tUnencMaxU);
                                auto mmALDmax = isHeadSmaller ? mm128<tail_t>::set1(tUnencMaxU) : mm128<head_t>::set1(hUnencMaxU);
                                size_t pos = 0;
                                while (pmmS <= (pmmSEnd - 1)) {
                                    auto mm = _mm_lddqu_si128(pmmS++);
                                    if (isSmallerEncoded) {
                                        v2_mm128_AN<smaller_t>::detect(mmDecS, mm, mmASinv, mmASDmax, vecS, pos);
                                        mmDecS = mm128<smaller_t, smaller_unenc_t>::convert(mmDecS);
                                    } else {
                                        mmDecS = mm;
                                    }
                                    _mm_storeu_si128(pmmRS, mmDecS);
                                    pmmRS = reinterpret_cast<__m128i *>(reinterpret_cast<smaller_unenc_t *>(pmmRS) + smallersPerMM128);
                                    for (size_t i = 0; i < factor; ++i) {
                                        mm = _mm_lddqu_si128(pmmL++);
                                        if (isLargerEncoded) {
                                            v2_mm128_AN<larger_t>::detect(mmDecL, mm, mmALinv, mmALDmax, vecL, pos);
                                            mmDecL = mm128<larger_t, larger_unenc_t>::convert(mmDecL);
                                        } else {
                                            mmDecL = mm;
                                        }
                                        pos += largersPerMM128;
                                        _mm_storeu_si128(pmmRL, mmDecL);
                                        pmmRL = reinterpret_cast<__m128i *>(reinterpret_cast<larger_unenc_t *>(pmmRL) + largersPerMM128);
                                    }
                                }
                                result->overwrite_size(pos); // "register" the number of values we added
                                auto iter = arg->begin();
                                for (*iter += pos; iter->hasNext(); ++*iter, ++pos) {
                                    head_t decH = isHeadEncoded ? static_cast<head_t>(iter->head() * hAinv) : iter->head();
                                    tail_t decT = isTailEncoded ? static_cast<tail_t>(iter->tail() * tAinv) : iter->tail();
                                    if (isHeadEncoded && (decH > hUnencMaxU)) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTailEncoded && (decT > tUnencMaxU)) {
                                        (*vec2)[pos] = true;
                                    }
                                    result->append(std::make_pair(static_cast<head_unenc_t>(decH), static_cast<tail_unenc_t>(decT)));
                                }
                                delete iter;
                                return make_tuple(result, vec1, vec2);
                            }
                        };

                        template<typename Tail>
                        struct CheckAndDecodeAN<v2_void_t, Tail> {
                            typedef v2_void_t Head;
                            typedef typename Tail::type_t tail_t;
                            typedef typename Tail::v2_unenc_t v2_tail_unenc_t;
                            typedef typename v2_tail_unenc_t::type_t tail_unenc_t;
                            typedef typename v2_mm128_AN<tail_t>::mask_t tail_mask_t;
                            typedef typename std::tuple<BAT<v2_void_t, v2_tail_unenc_t>*, std::vector<bool>*, std::vector<bool>*> result_t;

                            static result_t doIt(
                                    BAT<Head, Tail>* arg) {
                                static_assert(std::is_base_of<v2_anencoded_t, Tail>::value, "Tail must be an AN-encoded type");

                                constexpr const size_t tailsPerMM128 = sizeof(__m128i) / sizeof (tail_t);

                                tail_t tAinv = static_cast<tail_t>(arg->tail.metaData.AN_Ainv);
                                tail_t tUnencMaxU = static_cast<tail_t>(arg->tail.metaData.AN_unencMaxU);

                                oid_t szArg = arg->size();
                                std::vector<bool> *vec = new std::vector<bool>(szArg);
                                auto result = new TempBAT<v2_void_t, v2_tail_unenc_t>();
                                result->reserve(szArg + tailsPerMM128); // reserve more data to compensate for writing after the last bytes, since writing the very last vector will write 16 Bytes and not just the remaining ones
                                auto pT = arg->tail.container->data();
                                auto pTEnd = pT + szArg;
                                auto pmmT = reinterpret_cast<__m128i *>(pT);
                                auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                                auto mmATInv = mm128<tail_t>::set1(tAinv);
                                auto mmATDmax = mm128<tail_t>::set1(tUnencMaxU);
                                __m128i mmDec;
                                auto pRT = reinterpret_cast<tail_unenc_t *>(result->tail.container->data());
                                auto pmmRT = reinterpret_cast<__m128i *>(pRT);

                                size_t pos = 0;
                                for (; pmmT <= (pmmTEnd - 1); ++pmmT, pos += tailsPerMM128) {
                                    v2_mm128_AN<tail_t>::detect(mmDec, _mm_lddqu_si128(pmmT), mmATInv, mmATDmax, vec, pos);
                                    mmDec = mm128<tail_t, tail_unenc_t>::convert(mmDec);
                                    _mm_storeu_si128(pmmRT, mmDec);
                                    pmmRT = reinterpret_cast<__m128i *>(reinterpret_cast<tail_unenc_t *>(pmmRT) + tailsPerMM128);
                                }
                                result->overwrite_size(pos); // "register" the number of values we added
                                auto iter = arg->begin();
                                for (*iter += pos; iter->hasNext(); ++*iter, ++pos) {
                                    tail_t decT = static_cast<tail_t>(iter->tail() * tAinv);
                                    if (decT > tUnencMaxU) {
                                        (*vec)[pos] = true;
                                    }
                                    result->append(static_cast<tail_unenc_t>(decT));
                                }
                                delete iter;
                                return make_tuple(result, nullptr, vec);
                            }
                        };
                    }

                    template<typename Head, typename Tail>
                    std::tuple<BAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>*, std::vector<bool>*, std::vector<bool>*> checkAndDecodeAN(
                            BAT<Head, Tail>* arg) {
                        return Private::CheckAndDecodeAN<Head, Tail>::doIt(arg);
                    }

                }
            }
        }
    }
}

#endif /* ENCDECAN_SSE_TCC */

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
 * File:   encdec.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:22
 */

#ifndef ENCDECAN_SSE_TCC
#define ENCDECAN_SSE_TCC

#include <type_traits>
#include <immintrin.h>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>
#include <column_operators/SSE.hpp>
#include <column_operators/SSECMP.hpp>
#include <column_operators/SSEAN.hpp>
#include <column_operators/Normal/miscellaneous.tcc>

namespace v2 {
    namespace bat {
        namespace ops {

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
                    typedef typename v2_mm128_AN<head_t>::mask_t head_mask_t;
                    typedef typename v2_mm128_AN<tail_t>::mask_t tail_mask_t;
                    typedef typename std::tuple<BAT<v2_head_unenc_t, v2_tail_unenc_t>*, std::vector<bool>*, std::vector<bool>*> result_t;

                    static result_t doIt(BAT<Head, Tail>* arg) {
                        static_assert(std::is_base_of<v2_anencoded_t, Head>::value || std::is_base_of<v2_anencoded_t, Tail>::value, "At least one of Head and Tail must be an AN-encoded type");

                        constexpr const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                        constexpr const size_t headsPerMM128 = sizeof(__m128i) / sizeof (head_t);
                        constexpr const head_mask_t maskMaskH = static_cast<head_mask_t>((1ull << headsPerMM128) - 1);
                        constexpr const size_t tailsPerMM128 = sizeof(__m128i) / sizeof (tail_t);
                        constexpr const tail_mask_t maskMaskT = static_cast<tail_mask_t>((1ull << tailsPerMM128) - 1);

                        head_t hAinv = static_cast<head_t>(arg->head.metaData.AN_Ainv);
                        head_t hUnencMaxU = static_cast<head_t>(arg->head.metaData.AN_unencMaxU);
                        tail_t tAinv = static_cast<tail_t>(arg->tail.metaData.AN_Ainv);
                        tail_t tUnencMaxU = static_cast<tail_t>(arg->tail.metaData.AN_unencMaxU);

                        std::vector<bool> *vec1 = (isHeadEncoded ? new std::vector<bool>(arg->size()) : nullptr);
                        std::vector<bool> *vec2 = (isTailEncoded ? new std::vector<bool>(arg->size()) : nullptr);
                        auto result = new TempBAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>();
                        oid_t szArg = arg->size();
                        result->reserve(szArg + (headsPerMM128 > tailsPerMM128 ? headsPerMM128 : tailsPerMM128)); // reserve more data to compensate for writing after the last bytes, since writing the very last vector will write 16 Bytes and not just the remaining ones
                        auto pH = arg->head.container->data();
                        auto pmmH = reinterpret_cast<__m128i *>(pH);
                        auto pT = arg->tail.container->data();
                        auto pTEnd = pT + szArg;
                        auto pmmT = reinterpret_cast<__m128i *>(pT);
                        auto pmmTEnd = reinterpret_cast<__m128i *>(pTEnd);
                        auto mmAHInv = v2_mm128<head_t>::set1(hAinv);
                        auto mmAHDmax = v2_mm128<head_t>::set1(hUnencMaxU);
                        auto mmATInv = v2_mm128<tail_t>::set1(tAinv);
                        auto mmATDmax = v2_mm128<tail_t>::set1(tUnencMaxU);
                        __m128i mmDecH, mmDecT;
                        auto pRH = reinterpret_cast<head_unenc_t*>(result->head.container->data());
                        auto pmmRH = reinterpret_cast<__m128i *>(pRH);
                        auto pRT = reinterpret_cast<tail_unenc_t*>(result->tail.container->data());
                        auto pmmRT = reinterpret_cast<__m128i *>(pRT);
                        head_mask_t maskHBad, maskHOk, maskHOk2;
                        tail_mask_t maskTBad, maskTOk, maskTOk2;

                        size_t pos = 0;
                        while (pmmT <= (pmmTEnd - 1)) {
                            if (larger_type<head_t, tail_t>::isFirstLarger) {
                                constexpr const size_t factor = sizeof(head_t) / sizeof(tail_t);
                                if (isTailEncoded) {
                                    maskTBad = v2_mm128_AN<tail_t>::detect(mmDecT, *pmmT++, mmATInv, mmATDmax, vec2, pos);
                                    mmDecT = v2_mm128_cvt<tail_t, tail_unenc_t>(mmDecT);
                                } else {
                                    mmDecT = _mm_lddqu_si128(pmmT++);
                                    maskTBad = 0;
                                }
                                maskTOk = (~maskTBad) & maskMaskT; // only copy valid values
                                maskTOk2 = 0;
                                for (size_t i = 0; i < factor; ++i) {
                                    if (isHeadEncoded) {
                                        maskHBad = v2_mm128_AN<head_t>::detect(mmDecH, *pmmH++, mmAHInv, mmAHDmax, vec1, pos);
                                        mmDecH = v2_mm128_cvt<head_t, head_unenc_t>(mmDecH);
                                    } else {
                                        mmDecH = _mm_lddqu_si128(pmmH++);
                                        maskHBad = 0;
                                    }
                                    pos += headsPerMM128;
                                    maskHOk = (~maskHBad) & maskMaskH;
                                    auto shift = i * headsPerMM128;
                                    head_mask_t maskBoth = static_cast<head_mask_t>(((static_cast<tail_mask_t>(maskHOk) << shift) & maskTOk) >> shift);
                                    maskTOk2 |= static_cast<tail_mask_t>(maskBoth) << shift;
                                    if (maskBoth) {
                                        _mm_storeu_si128(pmmRH, v2_mm128<head_unenc_t>::pack_right(mmDecH, maskBoth));
                                        pmmRH = reinterpret_cast<__m128i *>(reinterpret_cast<head_unenc_t*>(pmmRH) + __builtin_popcount(maskBoth));
                                    }
                                }
                                if (isTailEncoded) {
                                    if (maskTOk2) {
                                        _mm_storeu_si128(pmmRT, v2_mm128<tail_unenc_t>::pack_right(mmDecT, maskTOk2));
                                        pmmRT = reinterpret_cast<__m128i *>(reinterpret_cast<tail_unenc_t*>(pmmRT) + __builtin_popcount(maskTOk2));
                                    }
                                } else {
                                    _mm_storeu_si128(pmmRT++, mmDecT);
                                }
                            } else {
                                constexpr const size_t factor = sizeof(tail_t) / sizeof(head_t);
                                if (isHeadEncoded) {
                                    maskHBad = v2_mm128_AN<head_t>::detect(mmDecH, *pmmH++, mmAHInv, mmAHDmax, vec1, pos);
                                    mmDecH = v2_mm128_cvt<head_t, head_unenc_t>(mmDecH);
                                } else {
                                    mmDecH = _mm_lddqu_si128(pmmH++);
                                    maskHBad = 0;
                                }
                                maskHOk = (~maskHBad) & maskMaskH; // only copy valid values
                                maskHOk2 = 0;
                                for (size_t i = 0; i < factor; ++i) {
                                    if (isTailEncoded) {
                                        maskTBad = v2_mm128_AN<tail_t>::detect(mmDecT, *pmmT++, mmATInv, mmATDmax, vec2, pos);
                                        mmDecT = v2_mm128_cvt<tail_t, tail_unenc_t>(mmDecT);
                                    } else {
                                        mmDecT = _mm_lddqu_si128(pmmT++);
                                        maskTBad = 0;
                                    }
                                    pos += tailsPerMM128;
                                    maskTOk = (~maskTBad) & maskMaskT;
                                    auto shift = i * tailsPerMM128;
                                    tail_mask_t maskBoth = static_cast<tail_mask_t>(((static_cast<head_mask_t>(maskTOk) << shift) & maskHOk) >> shift);
                                    maskHOk2 |= static_cast<head_mask_t>(maskBoth) << shift;
                                    if (maskBoth) {
                                        _mm_storeu_si128(pmmRT, v2_mm128<tail_unenc_t>::pack_right(mmDecT, maskBoth));
                                        pmmRT = reinterpret_cast<__m128i *>(reinterpret_cast<tail_unenc_t*>(pmmRT) + __builtin_popcount(maskBoth));
                                    }
                                }
                                if (isHeadEncoded) {
                                    if (maskHOk2) {
                                        _mm_storeu_si128(pmmRH, v2_mm128<head_unenc_t>::pack_right(mmDecH, maskHOk2));
                                        pmmRH = reinterpret_cast<__m128i *>(reinterpret_cast<head_unenc_t*>(pmmRH) + __builtin_popcount(maskHOk2));
                                    }
                                } else {
                                    _mm_storeu_si128(pmmRH++, mmDecH);
                                }
                            }
                        }
                        result->overwrite_size(reinterpret_cast<decltype(pRT)>(pmmRT) - pRT); // "register" the number of values we added
                        auto iter = arg->begin();
                        for (*iter += pos; iter->hasNext(); ++*iter, ++pos) {
                            head_t decH = isHeadEncoded ? static_cast<head_t>(iter->head() * hAinv) : iter->head();
                            tail_t decT = isTailEncoded ? static_cast<tail_t>(iter->tail() * tAinv) : iter->tail();
                            bool isHeadOK = !isHeadEncoded || (decH <= hUnencMaxU);
                            if (!isHeadOK) {
                                (*vec1)[pos] = true;
                            }
                            bool isTailOK = !isTailEncoded || (decT <= tUnencMaxU);
                            if (!isTailOK) {
                                (*vec2)[pos] = true;
                            }
                            if (isHeadOK && isTailOK) {
                                result->append(std::make_pair(static_cast<head_unenc_t>(decH), static_cast<tail_unenc_t>(decT)));
                            }
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

                    static result_t doIt(BAT<Head, Tail>* arg) {
                        static_assert(std::is_base_of<v2_anencoded_t, Tail>::value, "Tail must be an AN-encoded type");

                        constexpr const size_t tailsPerMM128 = sizeof(__m128i) / sizeof (tail_t);
                        constexpr const tail_mask_t maskMaskT = static_cast<tail_mask_t>((1ull << tailsPerMM128) - 1);

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
                        auto mmATInv = v2_mm128<tail_t>::set1(tAinv);
                        auto mmATDmax = v2_mm128<tail_t>::set1(tUnencMaxU);
                        __m128i mmDec;
                        auto pRT = reinterpret_cast<tail_unenc_t*>(result->tail.container->data());
                        auto pmmRT = reinterpret_cast<__m128i *>(pRT);

                        size_t pos = 0;
                        for (; pmmT <= (pmmTEnd - 1); ++pmmT) {
                            auto mm = _mm_lddqu_si128(pmmT);
                            tail_mask_t maskTBad = v2_mm128_AN<tail_t>::detect(mmDec, mm, mmATInv, mmATDmax, vec, pos);
                            tail_mask_t maskOK = (~maskTBad) & maskMaskT;
                            auto mmDec2 = v2_mm128_cvt<tail_t, tail_unenc_t>(mmDec);
                            pos += tailsPerMM128;
                            if (maskOK) { // any valid code words at all?
                                _mm_storeu_si128(pmmRT, v2_mm128<tail_unenc_t>::pack_right(mmDec2, maskOK));
                                pmmRT = reinterpret_cast<__m128i *>(reinterpret_cast<tail_unenc_t*>(pmmRT) + __builtin_popcount(maskOK));
                            }
                        }
                        result->overwrite_size(reinterpret_cast<decltype(pRT)>(pmmRT) - pRT); // "register" the number of values we added
                        auto iter = arg->begin();
                        for (*iter += pos; iter->hasNext(); ++*iter, ++pos) {
                            tail_t decT = static_cast<tail_t>(iter->tail() * tAinv);
                            if (decT > tUnencMaxU) {
                                (*vec)[pos] = true;
                            } else {
                                result->append(static_cast<tail_unenc_t>(decT));
                            }
                        }
                        delete iter;
                        return make_tuple(result, nullptr, vec);
                    }
                };
            }

            template<typename Head, typename Tail>
            std::tuple<BAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>*, std::vector<bool>*, std::vector<bool>*> checkAndDecodeAN(BAT<Head, Tail>* arg) {
                return Private::CheckAndDecodeAN<Head, Tail>::doIt(arg);
            }

        }
    }
}

#endif /* ENCDECAN_SSE_TCC */

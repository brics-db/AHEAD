// Copyright (c) 2016 Till Kolditz
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

/* 
 * File:   encdec.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:22
 */

#ifndef ENCDEC_TCC
#define ENCDEC_TCC

#include <type_traits>

#include <util/resilience.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename Head, typename Tail>
            Bat<Head, typename TypeMap<Tail>::v2_encoded_t>* encodeAN(Bat<Head, Tail>* arg, typename TypeMap<Tail>::v2_encoded_t::type_t A = TypeMap<Tail>::v2_encoded_t::A) {
                typedef typename TypeMap<Tail>::v2_encoded_t::type_t tail_t;
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_base_t, Tail>::value, "Tail must be a base type");

                auto result = new TempBat<Head, typename TypeMap<Tail>::v2_encoded_t > (arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(iter->head(), static_cast<tail_t> (iter->tail()) * A));
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename ResTail>
            vector<bool>* checkAN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");

                auto result = new vector<bool>(arg->size());
                auto iter = arg->begin();
                for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                    if ((iter->tail() * aInv) <= unEncMaxU) {
                        (*result)[i] = true;
                    }
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename ResTail>
            pair<Bat<Head, typename ResTail::unenc_v2_t>*, vector<bool>*> decodeAN(Bat<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");

                typedef typename ResTail::unenc_v2_t Tail;
                typedef typename Tail::type_t tail_t;
                auto result = new TempBat<Head, Tail>(arg->size());
                auto vec = new vector<bool>;
                vec->resize(arg->size());
                auto iter = arg->begin();
                size_t pos = 0;
                for (; iter->hasNext(); ++*iter, ++pos) {
                    auto t = iter->tail() * aInv;
                    result->append(make_pair(iter->head(), static_cast<tail_t> (t)));
                    if (t > unEncMaxU) {
                        (*vec)[pos] = true;
                    }
                }
                delete iter;
                return make_pair(result, vec);
            }

            template<typename Head, typename Tail, typename HEnc = typename TypeMap<Head>::v2_encoded_t, typename TEnc = typename TypeMap<Tail>::v2_encoded_t>
            tuple<Bat<typename Head::unenc_v2_t, typename Tail::unenc_v2_t>*, vector<bool>*, vector<bool>*> checkAndDecodeAN(Bat<Head, Tail>* arg, typename HEnc::type_t aInvH = HEnc::A_INV, typename HEnc::type_t aUnencMaxUH = HEnc::A_UNENC_MAX_U, typename TEnc::type_t aInvT = TEnc::A_INV, typename TEnc::type_t aUnencMaxUT = TEnc::A_UNENC_MAX_U) {
                static_assert(is_base_of<v2_anencoded_t, Head>::value || is_base_of<v2_anencoded_t, Tail>::value, "At least one of Head and Tail must be an AN-encoded type");

                const bool isHeadEncoded = is_base_of<v2_anencoded_t, Head>::value;
                const bool isTailEncoded = is_base_of<v2_anencoded_t, Tail>::value;
                size_t sizeBAT = arg->size();
                vector<bool> *vec1 = (isHeadEncoded ? new vector<bool>(arg->size()) : nullptr);
                vector<bool> *vec2 = (isTailEncoded ? new vector<bool>(arg->size()) : nullptr);
                auto result = new TempBat<typename Head::unenc_v2_t, typename Tail::unenc_v2_t > (sizeBAT);
                auto iter = arg->begin();
                for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                    if (isHeadEncoded & isTailEncoded) {
                        auto decH = iter->head() * aInvH;
                        auto decT = iter->tail() * aInvT;
                        if (decH <= aUnencMaxUH) {
                            (*vec1)[i] = true;
                        }
                        if (decT <= aUnencMaxUT) {
                            (*vec2)[i] = true;
                        }
                        result->append(make_pair(static_cast<typename Tail::unenc_v2_t::type_t> (decH), static_cast<typename Tail::unenc_v2_t::type_t> (decT)));
                    } else if (isHeadEncoded) {
                        auto decH = iter->head() * aInvH;
                        if (decH <= aUnencMaxUH) {
                            (*vec1)[i] = true;
                        }
                        result->append(make_pair(static_cast<typename Tail::unenc_v2_t::type_t> (decH), iter->tail()));
                    } else {
                        auto decT = iter->tail() * aInvT;
                        if (decT <= aUnencMaxUT) {
                            (*vec2)[i] = true;
                        }
                        result->append(make_pair(iter->head(), static_cast<typename Tail::unenc_v2_t::type_t> (decT)));
                    }
                }
                delete iter;
                return make_tuple(result, vec1, vec2);
            }
        }
    }
}

#endif /* ENCDEC_TCC */

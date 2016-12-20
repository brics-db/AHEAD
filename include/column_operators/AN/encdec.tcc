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

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

namespace v2 {
    namespace bat {
        namespace ops {

            template<typename Head, typename Tail>
            BAT<Head, typename TypeMap<Tail>::v2_encoded_t>*
            encodeAN (BAT<Head, Tail>* arg, typename TypeMap<Tail>::v2_encoded_t::type_t A = get<ANParametersSelector<Tail>::As->size () - 1 > (*ANParametersSelector<Tail>::As)) {

                typedef typename TypeMap<Tail>::v2_encoded_t enctail_t;
                typedef typename enctail_t::type_t tail_t;
                typedef typename BAT<Head, enctail_t>::coldesc_head_t cd_head_t;
                typedef typename BAT<Head, enctail_t>::coldesc_tail_t cd_tail_t;

                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_base_t, Tail>::value, "Tail must be a base type");

                auto result = new TempBAT<Head, enctail_t>(cd_head_t(arg->head.metaData), cd_tail_t(ColumnMetaData(sizeof (typename TypeMap<Tail>::v2_encoded_t::type_t), A, ext_euclidean(A, sizeof (tail_t)), enctail_t::UNENC_MAX_U, enctail_t::UNENC_MIN)));
                result->reserve(arg->size());
                auto iter = arg->begin();
                for (; iter->hasNext(); ++*iter) {
                    result->append(make_pair(iter->head(), static_cast<tail_t>(iter->tail()) * A));
                }
                delete iter;
                return result; // possibly empty
            }

            template<typename Head, typename ResTail>
            vector<bool>*
            checkAN (BAT<Head, ResTail>* arg, typename ResTail::type_t aInv = ResTail::A_INV, typename ResTail::type_t unEncMaxU = ResTail::A_UNENC_MAX_U) {

                typedef typename ResTail::type_t res_t;

                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");

                res_t Ainv = static_cast<res_t>(arg->tail.metaData.AN_Ainv);

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
            pair<TempBAT<Head, typename ResTail::v2_unenc_t>*, vector<bool>*>
            decodeAN (BAT<Head, ResTail>* arg) {

                typedef typename ResTail::type_t restail_t;
                typedef typename ResTail::v2_unenc_t Tail;
                typedef typename Tail::type_t tail_t;

                static_assert(is_base_of<v2_base_t, Head>::value, "Head must be a base type");
                static_assert(is_base_of<v2_anencoded_t, ResTail>::value, "ResTail must be an AN-encoded type");

                restail_t Ainv = static_cast<restail_t>(arg->tail.metaData.AN_Ainv);
                restail_t unencMaxU = static_cast<restail_t>(arg->tail.metaData.AN_unencMaxU);
                auto result = skeletonHead<Head, Tail>(arg);
                result->reserve(arg->size());
                auto vec = new vector<bool>;
                vec->resize(arg->size());
                auto iter = arg->begin();
                size_t pos = 0;
                for (; iter->hasNext(); ++*iter, ++pos) {
                    auto t = iter->tail() * Ainv;
                    result->append(make_pair(iter->head(), static_cast<tail_t>(t)));
                    if (t > unencMaxU) {
                        (*vec)[pos] = true;
                    }
                }
                delete iter;
                return make_pair(result, vec);
            }

            template<typename Head, typename Tail>
            tuple<BAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>*, vector<bool>*, vector<bool>*>
            checkAndDecodeAN (BAT<Head, Tail>* arg) {

                typedef typename Head::type_t head_t;
                typedef typename Tail::type_t tail_t;

                static_assert(is_base_of<v2_anencoded_t, Head>::value || is_base_of<v2_anencoded_t, Tail>::value, "At least one of Head and Tail must be an AN-encoded type");

                const bool isHeadEncoded = is_base_of<v2_anencoded_t, Head>::value;
                const bool isTailEncoded = is_base_of<v2_anencoded_t, Tail>::value;
                head_t hAinv = static_cast<head_t>(arg->head.metaData.AN_Ainv);
                head_t hUnencMaxU = static_cast<head_t>(arg->head.metaData.AN_unencMaxU);
                tail_t tAinv = static_cast<tail_t>(arg->tail.metaData.AN_Ainv);
                tail_t tUnencMaxU = static_cast<tail_t>(arg->tail.metaData.AN_unencMaxU);
                vector<bool> *vec1 = (isHeadEncoded ? new vector<bool>(arg->size()) : nullptr);
                vector<bool> *vec2 = (isTailEncoded ? new vector<bool>(arg->size()) : nullptr);
                auto result = new TempBAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t > ();
                result->reserve(arg->size());
                auto iter = arg->begin();
                for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                    if (isHeadEncoded & isTailEncoded) {
                        auto decH = iter->head() * hAinv;
                        auto decT = iter->tail() * tAinv;
                        if (decH <= hUnencMaxU) {
                            (*vec1)[i] = true;
                        }
                        if (decT <= tUnencMaxU) {
                            (*vec2)[i] = true;
                        }
                        result->append(make_pair(static_cast<typename Tail::v2_unenc_t::type_t>(decH), static_cast<typename Tail::v2_unenc_t::type_t>(decT)));
                    } else if (isHeadEncoded) {
                        auto decH = iter->head() * hAinv;
                        if (decH <= hUnencMaxU) {
                            (*vec1)[i] = true;
                        }
                        result->append(make_pair(static_cast<typename Tail::v2_unenc_t::type_t>(decH), iter->tail()));
                    } else {
                        auto decT = iter->tail() * tAinv;
                        if (decT <= tUnencMaxU) {
                            (*vec2)[i] = true;
                        }
                        result->append(make_pair(iter->head(), static_cast<typename Tail::v2_unenc_t::type_t>(decT)));
                    }
                }
                delete iter;
                return make_tuple(result, vec1, vec2);
            }
        }
    }
}

#endif /* ENCDEC_TCC */

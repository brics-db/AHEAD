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

#ifndef ENCDECAN_SCALAR_TCC
#define ENCDECAN_SCALAR_TCC

#include <type_traits>

#include <column_storage/Storage.hpp>
#include <column_operators/Normal/miscellaneous.tcc>

namespace ahead {
    namespace bat {
        namespace ops {

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

            namespace Private {
                template<typename Head, typename Tail>
                struct CheckAndDecodeAN {

                    typedef typename Head::v2_unenc_t v2_head_unenc_t;
                    typedef typename v2_head_unenc_t::type_t head_unenc_t;
                    typedef typename Tail::v2_unenc_t v2_tail_unenc_t;
                    typedef typename v2_tail_unenc_t::type_t tail_unenc_t;
                    typedef std::tuple<BAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>*, std::vector<bool>*, std::vector<bool>*> result_t;

                    static result_t doIt(BAT<Head, Tail>* arg) {

                        typedef typename Head::type_t head_t;
                        typedef typename Tail::type_t tail_t;

                        static_assert(std::is_base_of<v2_anencoded_t, Head>::value || std::is_base_of<v2_anencoded_t, Tail>::value, "At least one of Head and Tail must be an AN-encoded type");

                        constexpr const bool isHeadEncoded = std::is_base_of<v2_anencoded_t, Head>::value;
                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                        head_t hAinv = static_cast<head_t>(arg->head.metaData.AN_Ainv);
                        head_t hUnencMaxU = static_cast<head_t>(arg->head.metaData.AN_unencMaxU);
                        tail_t tAinv = static_cast<tail_t>(arg->tail.metaData.AN_Ainv);
                        tail_t tUnencMaxU = static_cast<tail_t>(arg->tail.metaData.AN_unencMaxU);
                        std::vector<bool> *vec1 = (isHeadEncoded ? new std::vector<bool>(arg->size()) : nullptr);
                        std::vector<bool> *vec2 = (isTailEncoded ? new std::vector<bool>(arg->size()) : nullptr);
                        auto result = new TempBAT<typename Head::v2_unenc_t, typename Tail::v2_unenc_t>();
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        for (size_t pos = 0; iter->hasNext(); ++*iter, ++pos) {
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
                        return std::make_tuple(result, vec1, vec2);
                    }
                };

                template<typename Tail>
                struct CheckAndDecodeAN<v2_void_t, Tail> {

                    typedef typename Tail::v2_unenc_t v2_tail_unenc_t;
                    typedef typename v2_tail_unenc_t::type_t tail_unenc_t;
                    typedef std::tuple<BAT<v2_void_t, v2_tail_unenc_t>*, std::vector<bool>*, std::vector<bool>*> result_t;

                    static result_t doIt(BAT<v2_void_t, Tail>* arg) {

                        typedef typename Tail::type_t tail_t;

                        static_assert(std::is_base_of<v2_anencoded_t, Tail>::value, "At least one of Head and Tail must be an AN-encoded type");

                        constexpr const bool isTailEncoded = std::is_base_of<v2_anencoded_t, Tail>::value;
                        tail_t tAinv = static_cast<tail_t>(arg->tail.metaData.AN_Ainv);
                        tail_t tUnencMaxU = static_cast<tail_t>(arg->tail.metaData.AN_unencMaxU);
                        std::vector<bool> *vec2 = (isTailEncoded ? new std::vector<bool>(arg->size()) : nullptr);
                        auto result = new TempBAT<v2_void_t, v2_tail_unenc_t>();
                        result->reserve(arg->size());
                        auto iter = arg->begin();
                        for (size_t i = 0; iter->hasNext(); ++*iter, ++i) {
                            auto decT = static_cast<tail_t>(iter->tail() * tAinv);
                            if (decT > tUnencMaxU) {
                                (*vec2)[i] = true;
                            } else {
                                result->append(static_cast<tail_unenc_t>(decT));
                            }
                        }
                        delete iter;
                        return std::make_tuple(result, nullptr, vec2);
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

#endif /* ENCDECAN_SCALAR_TCC */

// Copyright (c) 2017 Till Kolditz
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
 * File:   miscellaneous.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 30-05-2017 10:01
 */
#ifndef LIB_COLUMN_OPERATORS_MISCELLANEOUS_HPP_
#define LIB_COLUMN_OPERATORS_MISCELLANEOUS_HPP_

#include <optional>

#include <column_storage/Storage.hpp>

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                template<typename T>
                struct equals {

                    bool
                    operator()(
                            T t1,
                            T t2
                            ) const {
                        return t1 == t2;
                    }

                    static
                    bool
                    eq(
                            T t1,
                            T t2
                            ) {
                        return t1 == t2;
                    }
                };

                template<>
                struct equals<str_t> {

                    bool
                    operator()(
                            str_t s1,
                            str_t s2
                            ) const {
                        if (s1 == nullptr) {
                            return s2 == nullptr;
                        }
                        if (s2 == nullptr) {
                            return false;
                        }
                        return strcmp(s1, s2) == 0;
                    }

                    static
                    bool
                    eq(
                            str_t s1,
                            str_t s2
                            ) {
                        if (s1 == nullptr) {
                            return s2 == nullptr;
                        }
                        if (s2 == nullptr) {
                            return false;
                        }
                        return strcmp(s1, s2) == 0;
                    }
                };

                template<typename T>
                struct hash {
                    typedef T hash_t;

                    T
                    operator()(
                            T const & t
                            ) const {
                        return t;
                    }

                    static
                    T
                    get(
                            T const & t
                            ) {
                        return t;
                    }
                };

                /**
                 * The following function is taken almost verbatim from (currently) the first answer at:
                 * http://stackoverflow.com/questions/15518418/whats-behind-the-hashcode-method-for-string-in-java
                 */
                template<>
                struct hash<str_t> {
                    typedef size_t hash_t;

                    hash_t
                    operator()(
                            str_t const & s
                            ) const {
                        return get(s);
                    }

                    static
                    hash_t
                    get(
                            str_t const & s
                            ) {
                        size_t len = std::strlen(s);
                        hash_t hash(0), multiplier(1);
                        for (ssize_t i = len - 1; i >= 0; --i) {
                            hash += s[i] * multiplier;
                            int shifted = multiplier << 5;
                            multiplier = shifted - multiplier;
                        }
                        return hash;
                    }
                };
            }

            template<typename TargetHead, typename TargetTail, typename Head, typename Tail>
            TempBAT<TargetHead, TargetTail> *
            skeleton(
                    BAT<Head, Tail> * arg
                    ) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg->head.metaData), coldesc_tail_t(arg->tail.metaData));
            }

            template<typename TargetHead, typename TargetTail, typename Head, typename Tail>
            TempBAT<TargetHead, TargetTail> *
            skeletonHead(
                    BAT<Head, Tail> * arg
                    ) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg->head.metaData), coldesc_tail_t());
            }

            template<typename TargetHead, typename TargetTail, typename Head, typename Tail>
            TempBAT<TargetHead, TargetTail> *
            skeletonTail(
                    BAT<Head, Tail> * arg
                    ) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(), coldesc_tail_t(arg->tail.metaData));
            }

            template<typename TargetHead, typename TargetTail, typename Head1, typename Tail1, typename Head2, typename Tail2>
            TempBAT<TargetHead, TargetTail> *
            skeletonJoin(
                    BAT<Head1, Tail1> * arg1,
                    BAT<Head2, Tail2> * arg2
                    ) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg1->head.metaData), coldesc_tail_t(arg2->tail.metaData));
            }

            template<typename Head, typename Tail>
            std::optional<typename Tail::v2_select_t::type_t>
            findFirstHead(
                    BAT<Head, Tail> * bat,
                    typename Head::type_t const value) {
                auto iter = bat->begin();
                for(; iter->hasNext(); ++*iter) {
                    if (iter->head() == value) {
                        auto tail = iter->tail();
                        delete iter;
                        return std::optional<typename Tail::v2_select_t::type_t>(tail);
                    }
                }
                delete iter;
                return std::optional<typename Tail::v2_select_t::type_t>();
            }

            template<typename Head, typename Tail>
            std::optional<typename Head::v2_select_t::type_t>
            findFirstTail(
                    BAT<Head, Tail> * bat,
                    typename Tail::type_t const value) {
                auto iter = bat->begin();
                for(; iter->hasNext(); ++*iter) {
                    if (iter->tail() == value) {
                        auto head = iter->head();
                        delete iter;
                        return std::optional<typename Head::v2_select_t::type_t>(head);
                    }
                }
                delete iter;
                return std::optional<typename Head::v2_select_t::type_t>();
            }

            template<typename T, typename U>
            struct is_instance_of {
                constexpr static const bool value = false;
            };

            template<typename T>
            struct is_instance_of<T, T> {
                constexpr static const bool value = true;
            };

        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_MISCELLANEOUS_HPP_ */

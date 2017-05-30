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

#include <column_storage/Storage.hpp>

namespace ahead {
    namespace bat {
        namespace ops {

            namespace Private {

                struct eqstr {
                    bool operator()(str_t s1, str_t s2) const {
                        if (s1 == nullptr) {
                            return s2 == nullptr;
                        }
                        if (s2 == nullptr) {
                            return false;
                        }
                        return strcmp(s1, s2) == 0;
                    }
                };

                /**
                 * The following function is taken almost verbatim from (currently) the first answer at:
                 * http://stackoverflow.com/questions/15518418/whats-behind-the-hashcode-method-for-string-in-java
                 */
                struct hashstr {
                    size_t operator()(str_t const &s) const {
                        size_t len = std::strlen(s);
                        size_t hash(0), multiplier(1);
                        for (int i = len - 1; i >= 0; --i) {
                            hash += s[i] * multiplier;
                            int shifted = multiplier << 5;
                            multiplier = shifted - multiplier;
                        }
                        return hash;
                    }
                };
            }

            template<typename TargetHead, typename TargetTail, typename Head, typename Tail>
            TempBAT<TargetHead, TargetTail>*
            skeleton(BAT<Head, Tail>* arg) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg->head.metaData), coldesc_tail_t(arg->tail.metaData));
            }

            template<typename TargetHead, typename TargetTail, typename Head, typename Tail>
            TempBAT<TargetHead, TargetTail>*
            skeletonHead(BAT<Head, Tail>* arg) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg->head.metaData), coldesc_tail_t());
            }

            template<typename TargetHead, typename TargetTail, typename Head, typename Tail>
            TempBAT<TargetHead, TargetTail>*
            skeletonTail(BAT<Head, Tail>* arg) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(), coldesc_tail_t(arg->tail.metaData));
            }

            template<typename TargetHead, typename TargetTail, typename Head1, typename Tail1, typename Head2, typename Tail2>
            TempBAT<TargetHead, TargetTail>*
            skeletonJoin(BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg1->head.metaData), coldesc_tail_t(arg2->tail.metaData));
            }

        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_MISCELLANEOUS_HPP_ */

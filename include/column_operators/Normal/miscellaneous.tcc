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
 * File:   miscellaneous.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 21:00
 */

#ifndef MISCELLANEOUS_TCC
#define MISCELLANEOUS_TCC

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

typedef enum {

    left, right
} hash_side_t;

namespace v2 {
    namespace bat {
        namespace ops {
            namespace Private {

                struct eqstr {

                    bool operator() (str_t s1, str_t s2) const {
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

                    size_t operator() (str_t const &s) const {
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
            skeleton (BAT<Head, Tail>* arg) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg->head.metaData), coldesc_tail_t(arg->tail.metaData));
            }

            template<typename TargetHead, typename TargetTail, typename Head, typename Tail>
            TempBAT<TargetHead, TargetTail>*
            skeletonHead (BAT<Head, Tail>* arg) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg->head.metaData), coldesc_tail_t());
            }

            template<typename TargetHead, typename TargetTail, typename Head, typename Tail>
            TempBAT<TargetHead, TargetTail>*
            skeletonTail (BAT<Head, Tail>* arg) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(), coldesc_tail_t(arg->tail.metaData));
            }

            template<typename TargetHead, typename TargetTail, typename Head1, typename Tail1, typename Head2, typename Tail2>
            TempBAT<TargetHead, TargetTail>*
            skeletonJoin (BAT<Head1, Tail1>* arg1, BAT<Head2, Tail2>* arg2) {
                typedef TempBAT<TargetHead, TargetTail> bat_t;
                typedef typename bat_t::coldesc_head_t coldesc_head_t;
                typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
                return new bat_t(coldesc_head_t(arg1->head.metaData), coldesc_tail_t(arg2->tail.metaData));
            }

            namespace Private {

                template<typename Head, typename Tail>
                struct copy0 {

                    typedef typename Head::v2_copy_t CHead;
                    typedef typename Tail::v2_copy_t CTail;

                    TempBAT<CHead, CTail> * operator() (BAT<Head, Tail> * arg) {
                        auto result = skeleton<CHead, CTail>(arg);
                        result->reserve(arg->size());
                        auto *iter = arg->begin();
                        for (; iter->hasNext(); ++*iter) {
                            result->append(std::make_pair(std::move(iter->head()), std::move(iter->tail())));
                        }
                        delete iter;
                        return result;
                    }
                };

                template<typename Tail>
                struct copy0<v2_void_t, Tail> {

                    typedef typename Tail::v2_copy_t CTail;

                    TempBAT<v2_void_t, CTail> * operator() (BAT<v2_void_t, Tail> * arg) {
                        auto result = skeleton<v2_void_t, CTail>(arg);
                        result->reserve(arg->size());
                        auto *iter = arg->begin();
                        for (; iter->hasNext(); ++*iter) {
                            result->append(std::move(iter->tail()));
                        }
                        delete iter;
                        return result;
                    }
                };

            }

            template<typename Head, typename Tail>
            TempBAT<typename Head::v2_copy_t, typename Tail::v2_copy_t>*
            copy (BAT<Head, Tail>* arg) {
                return Private::copy0<typename Head::v2_copy_t, typename Tail::v2_copy_t > ()(arg);
            }
        }
    }
}

#endif /* MISCELLANEOUS_TCC */

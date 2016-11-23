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
 * File:   aggregate.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:26
 */

#ifndef AGGREGATE_AN_TCC
#define AGGREGATE_AN_TCC

#include <type_traits>

#include <util/resilience.hpp>

namespace v2 {
    namespace bat {
        namespace ops {

            /**
             * Multiplies the tail values of each of the two Bat's and sums everything up.
             * @param arg1
             * @param arg2
             * @return A single sum of the pair-wise products of the two Bats
             */
            template<typename Result, typename Head1, typename Tail1, typename Head2, typename Tail2, typename ResEnc = typename TypeMap<Result>::v2_encoded_t, typename T1Enc = typename TypeMap<Tail1>::v2_encoded_t, typename T2Enc = typename TypeMap<Tail2>::v2_encoded_t>
            tuple<Bat<v2_resoid_t, Result>*, vector<bool>*, vector<bool>*>
            aggregate_mul_sumAN(
                    Bat<Head1, Tail1>* arg1,
                    Bat<Head2, Tail2>* arg2,
                    typename Result::type_t init = typename Result::type_t(0),
                    // typename T1Enc::type_t AT1 = T1Enc::A,
                    typename T1Enc::type_t AT1inv = T1Enc::A_INV,
                    typename T1Enc::type_t AT1unencMaxU = T1Enc::A_UNENC_MAX_U,
                    // typename T2Enc::type_t AT2 = T2Enc::A,
                    typename T2Enc::type_t AT2inv = T2Enc::A_INV,
                    typename T2Enc::type_t AT2unencMaxU = T2Enc::A_UNENC_MAX_U,
                    typename ResEnc::type_t RA = ResEnc::A,
                    typename v2_resoid_t::type_t AOID = v2_resoid_t::A
                    ) {
                typedef typename Result::type_t result_t;
                const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                const bool isTail2Encoded = is_base_of<v2_anencoded_t, Tail2>::value;
                const bool isResultEncoded = is_base_of<v2_anencoded_t, Result>::value;
                typename Result::type_t total = init;
                vector<bool>* vec1 = (isTail1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                vector<bool>* vec2 = (isTail2Encoded ? new vector<bool>(arg2->size()) : nullptr);
                auto iter1 = arg1->begin();
                auto iter2 = arg2->begin();
                for (size_t i = 0; iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2, ++i) {
                    typename T1Enc::type_t x1 = iter1->tail() * (isTail1Encoded ? AT1inv : 1);
                    typename T2Enc::type_t x2 = iter2->tail() * (isTail2Encoded ? AT2inv : 1);
                    if (isTail1Encoded && x1 <= AT1unencMaxU) {
                        (*vec1)[i] = true;
                    }
                    if (isTail2Encoded && x2 <= AT2unencMaxU) {
                        (*vec2)[i] = true;
                    }
                    total += static_cast<result_t> (x1) * static_cast<result_t> (x2);
                }
                if (isResultEncoded)
                    total *= RA;
                delete iter2;
                delete iter1;
                auto bat = new TempBat<v2_resoid_t, Result>();
                bat->append(make_pair(0 * AOID, total));
                return make_tuple(bat, vec1, vec2);
            }
        }
    }
}

#endif /* AGGREGATE_AN_TCC */

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
 * File:   hashjoin.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 22. November 2016, 16:24
 */

#ifndef HASHJOIN_AN_TCC
#define HASHJOIN_AN_TCC

#include <type_traits>

#include <util/resilience.hpp>

namespace v2 {
    namespace bat {
        namespace ops {
            namespace Private {

                template<typename Head1, typename Tail1, typename Head2, typename Tail2>
                struct hashjoinAN {
                    typedef typename Head1::v2_select_t head1_v2_select_t;
                    typedef typename Tail2::v2_select_t tail2_v2_select_t;
                    typedef typename TypeMap<Head1>::v2_encoded_t H1Enc;
                    typedef typename H1Enc::type_t h1enc_t;
                    typedef typename TypeMap<Tail1>::v2_encoded_t T1Enc;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename TypeMap<Head2>::v2_encoded_t H2Enc;
                    typedef typename H2Enc::type_t h2enc_t;
                    typedef typename TypeMap<Tail2>::v2_encoded_t T2Enc;
                    typedef typename T2Enc::type_t t2enc_t;

                    tuple<Bat<head1_v2_select_t, tail2_v2_select_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
                    operator()(
                            Bat<Head1, Tail1>* arg1,
                            Bat<Head2, Tail2>* arg2,
                            hash_side_t hashside = hash_side_t::right,
                            // h1enc_t AH1 = H1Enc::A,
                            h1enc_t AH1inv = H1Enc::A_INV,
                            h1enc_t AH1UnencMaxU = H1Enc::A_UNENC_MAX_U,
                            t1enc_t AT1 = T1Enc::A,
                            t1enc_t AT1inv = T1Enc::A_INV,
                            t1enc_t AT1UnencMaxU = T1Enc::A_UNENC_MAX_U,
                            h2enc_t AH2 = H2Enc::A,
                            h2enc_t AH2inv = H2Enc::A_INV,
                            h2enc_t AH2UnencMaxU = H2Enc::A_UNENC_MAX_U,
                            // t2enc_t AT2 = T2Enc::A,
                            t2enc_t AT2inv = T2Enc::A_INV,
                            t2enc_t AT2UnencMaxU = T2Enc::A_UNENC_MAX_U
                            ) {
                        const bool isHead1Encoded = is_base_of<v2_anencoded_t, Head1>::value;
                        const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                        const bool isHead2Encoded = is_base_of<v2_anencoded_t, Head2>::value;
                        const bool isTail2Encoded = is_base_of<v2_anencoded_t, Tail2>::value;
                        auto bat = new TempBat<head1_v2_select_t, tail2_v2_select_t>();
                        vector<bool> *vec1 = (isHead1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec2 = (isTail1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec3 = (isHead2Encoded ? new vector<bool>(arg2->size()) : nullptr);
                        vector<bool> *vec4 = (isTail2Encoded ? new vector<bool>(arg2->size()) : nullptr);
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        if (iter1->hasNext() && iter2->hasNext()) { // only really continue when both BATs are not empty
                            size_t pos = 0;
                            if (hashside == hash_side_t::left) {
                                // let's ignore the joinSide for now and use that sizes as a measure, which is of course oversimplified
                                google::dense_hash_map<typename Tail1::type_t, vector<typename Head1::type_t> > hashMap;
                                hashMap.set_empty_key(Tail1::dhm_emptykey);
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // build
                                    auto h = iter1->head();
                                    auto t = iter1->tail();
                                    if (isHead1Encoded && (h * AH1inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTail1Encoded && (t * AT1inv) > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    hashMap[t].push_back(h);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // probe
                                    auto h = iter2->head();
                                    auto t = iter2->tail();
                                    if (isHead2Encoded && (h * AH2inv) > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    if (isTail2Encoded && (t * AT2inv) > AT2UnencMaxU) {
                                        (*vec4)[pos] = true;
                                    }
                                    auto mapIter = hashMap.find(static_cast<typename Tail1::type_t> (isTail1Encoded ? (isHead2Encoded ? h : (static_cast<typename Tail1::type_t> (h) * AT1)) : (isHead2Encoded ? (h * AH2inv) : h)));
                                    if (mapIter != mapEnd) {
                                        for (auto matched : mapIter->second) {
                                            bat->append(make_pair(matched, t));
                                        }
                                    }
                                }
                            } else {
                                google::dense_hash_map<typename Head2::type_t, vector<typename Tail2::type_t> > hashMap;
                                hashMap.set_empty_key(Head2::dhm_emptykey);
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // build
                                    auto h = iter2->head();
                                    auto t = iter2->tail();
                                    if (isHead2Encoded && (h * AH2inv) > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    if (isTail2Encoded && (t * AT2inv) > AT2UnencMaxU) {
                                        (*vec4)[pos] = true;
                                    }
                                    hashMap[h].push_back(t);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // probe
                                    auto h = iter1->head();
                                    auto t = iter1->tail();
                                    if (isHead1Encoded && (h * AH1inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTail1Encoded && (t * AT1inv) > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    auto mapIter = hashMap.find(static_cast<typename Head2::type_t> (isHead2Encoded ? (isTail1Encoded ? t : (static_cast<typename Head2::type_t> (t) * AH2)) : (isTail1Encoded ? (t * AT1inv) : t)));
                                    if (mapIter != mapEnd) {
                                        for (auto matched : mapIter->second) {
                                            bat->append(make_pair(h, matched));
                                        }
                                    }
                                }
                            }
                        }
                        delete iter1;
                        delete iter2;
                        return make_tuple(bat, vec1, vec2, vec3, vec4);
                    }
                };

                template<typename Head1, typename Tail1, typename Head2>
                struct hashjoinAN<Head1, Tail1, Head2, v2_str_t> {
                    typedef typename Head1::v2_select_t head1_v2_select_t;
                    typedef typename TypeMap<Head1>::v2_encoded_t H1Enc;
                    typedef typename H1Enc::type_t h1enc_t;
                    typedef typename TypeMap<Tail1>::v2_encoded_t T1Enc;
                    typedef typename T1Enc::type_t t1enc_t;
                    typedef typename TypeMap<Head2>::v2_encoded_t H2Enc;
                    typedef typename H2Enc::type_t h2enc_t;

                    tuple<Bat<head1_v2_select_t, v2_str_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
                    operator()(
                            Bat<Head1, Tail1>* arg1,
                            Bat<Head2, v2_str_t>* arg2,
                            hash_side_t hashside = hash_side_t::right,
                            __attribute__((unused)) h1enc_t AH1 = H1Enc::A,
                            h1enc_t AH1inv = H1Enc::A_INV,
                            h1enc_t AH1UnencMaxU = H1Enc::A_UNENC_MAX_U,
                            t1enc_t AT1 = T1Enc::A,
                            t1enc_t AT1inv = T1Enc::A_INV,
                            t1enc_t AT1UnencMaxU = T1Enc::A_UNENC_MAX_U,
                            h2enc_t AH2 = H2Enc::A,
                            h2enc_t AH2inv = H2Enc::A_INV,
                            h2enc_t AH2UnencMaxU = H2Enc::A_UNENC_MAX_U
                            ) {
                        const bool isHead1Encoded = is_base_of<v2_anencoded_t, Head1>::value;
                        const bool isTail1Encoded = is_base_of<v2_anencoded_t, Tail1>::value;
                        const bool isHead2Encoded = is_base_of<v2_anencoded_t, Head2>::value;
                        auto bat = new TempBat<head1_v2_select_t, v2_str_t>();
                        vector<bool> *vec1 = (isHead1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec2 = (isTail1Encoded ? new vector<bool>(arg1->size()) : nullptr);
                        vector<bool> *vec3 = (isHead2Encoded ? new vector<bool>(arg2->size()) : nullptr);
                        auto iter1 = arg1->begin();
                        auto iter2 = arg2->begin();
                        if (iter1->hasNext() && iter2->hasNext()) { // only really continue when both BATs are not empty
                            size_t pos = 0;
                            if (hashside == hash_side_t::left) {
                                // let's ignore the joinSide for now and use that sizes as a measure, which is of course oversimplified
                                google::dense_hash_map<typename Tail1::type_t, vector<typename Head1::type_t> > hashMap;
                                hashMap.set_empty_key(Tail1::dhm_emptykey);
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // build
                                    auto h = iter1->head();
                                    auto t = iter1->tail();
                                    if (isHead1Encoded && (h * AH1inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTail1Encoded && (t * AT1inv) > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    hashMap[t].push_back(h);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // probe
                                    auto h = iter2->head();
                                    auto t = iter2->tail();
                                    if (isHead2Encoded && (h * AH2inv) > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    auto mapIter = hashMap.find(static_cast<typename Tail1::type_t> (isTail1Encoded ? (isHead2Encoded ? h : (static_cast<typename Tail1::type_t> (h) * AT1)) : (isHead2Encoded ? (h * AH2inv) : h)));
                                    if (mapIter != mapEnd) {
                                        for (auto matched : mapIter->second) {
                                            bat->append(make_pair(matched, t));
                                        }
                                    }
                                }
                            } else {
                                google::dense_hash_map<typename Head2::type_t, vector<str_t> > hashMap;
                                hashMap.set_empty_key(Head2::dhm_emptykey);
                                for (; iter2->hasNext(); ++*iter2, ++pos) { // build
                                    auto h = iter2->head();
                                    auto t = iter2->tail();
                                    if (isHead2Encoded && (h * AH2inv) > AH2UnencMaxU) {
                                        (*vec3)[pos] = true;
                                    }
                                    hashMap[h].push_back(t);
                                }
                                auto mapEnd = hashMap.end();
                                pos = 0;
                                for (; iter1->hasNext(); ++*iter1, ++pos) { // probe
                                    auto h = iter1->head();
                                    auto t = iter1->tail();
                                    if (isHead1Encoded && (h * AH1inv) > AH1UnencMaxU) {
                                        (*vec1)[pos] = true;
                                    }
                                    if (isTail1Encoded && (t * AT1inv) > AT1UnencMaxU) {
                                        (*vec2)[pos] = true;
                                    }
                                    auto mapIter = hashMap.find(static_cast<typename Head2::type_t> (isHead2Encoded ? (isTail1Encoded ? t : (static_cast<typename Head2::type_t> (t) * AH2)) : (isTail1Encoded ? (t * AT1inv) : t)));
                                    if (mapIter != mapEnd) {
                                        for (auto matched : mapIter->second) {
                                            bat->append(make_pair(h, matched));
                                        }
                                    }
                                }
                            }
                        }
                        delete iter1;
                        delete iter2;
                        return make_tuple(bat, vec1, vec2, vec3, nullptr);
                    }
                };
            }

            template<typename Head1, typename Tail1, typename Head2, typename Tail2>
            tuple<Bat<typename Head1::v2_select_t, typename Tail2::v2_select_t>*, vector<bool>*, vector<bool>*, vector<bool>*, vector<bool>*>
            hashjoinAN(
                    Bat<Head1, Tail1>* arg1,
                    Bat<Head2, Tail2>* arg2,
                    hash_side_t hashside = hash_side_t::right
                    ) {
                return Private::hashjoinAN<Head1, Tail1, Head2, Tail2>()(arg1, arg2, hashside);
            }
        }
    }
}

#endif /* HASHJOIN_AN_TCC */

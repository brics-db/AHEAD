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
 * File:   ANhelper.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 12-06-2017 16:33
 */
#ifndef LIB_COLUMN_OPERATORS_AN_ANHELPER_TCC_
#define LIB_COLUMN_OPERATORS_AN_ANHELPER_TCC_

#include <type_traits>

namespace ahead {
    namespace bat {
        namespace ops {

            template<typename V2T, bool encoded>
            struct ANReturnTypeSelector {
                typedef typename TypeMap<V2T>::v2_encoded_t::v2_select_t v2_type_t;
                typedef typename v2_type_t::type_t type_t;
            };

            template<typename V2T>
            struct ANReturnTypeSelector<V2T, false> {
                typedef typename V2T::v2_select_t v2_type_t;
                typedef typename v2_type_t::type_t type_t;
            };

            /**
             * AN encoded values
             */
            template<typename T, typename U, bool>
            struct ANhelper0 {
                constexpr static T getIfEncoded(
                        U const & value) {
                    return static_cast<T>(value);
                }

                constexpr static T getIfEncoded(
                        U const && value) {
                    return static_cast<T>(value);
                }

                constexpr static T mulIfEncoded(
                        U const & encoded,
                        U const & factor) {
                    return static_cast<T>(encoded * factor);
                }

                constexpr static T mulIfEncoded(
                        U const && encoded,
                        U const && factor) {
                    return static_cast<T>(encoded * factor);
                }

                inline static AN_indicator_vector * createIndicatorVector() {
                    auto vec = new AN_indicator_vector;
                    vec->reserve(32);
                    return vec;
                }
            };

            /**
             * unencoded values
             */
            template<typename T, typename U>
            struct ANhelper0<T, U, false> {
                constexpr static T getIfEncoded(
                        U const & value) {
                    (void) value;
                    return T(0);
                }

                constexpr static T getIfEncoded(
                        U const && value) {
                    (void) value;
                    return T(0);
                }

                constexpr static T mulIfEncoded(
                        U const & unencoded,
                        U const & factor) {
                    (void) factor;
                    return static_cast<T>(unencoded);
                }

                constexpr static T mulIfEncoded(
                        U const && unencoded,
                        U const && factor) {
                    (void) factor;
                    return static_cast<T>(unencoded);
                }

                inline static AN_indicator_vector * createIndicatorVector() {
                    return nullptr;
                }
            };

            template<typename V2T, typename U = typename ANReturnTypeSelector<V2T, std::is_base_of<v2_anencoded_t, V2T>::value>::v2_type_t::type_t>
            struct ANhelper :
                    public ANhelper0<typename V2T::type_t, U, std::is_base_of<v2_anencoded_t, V2T>::value> {

                constexpr static const bool isEncoded = std::is_base_of<v2_anencoded_t, V2T>::value;

                typedef ANhelper0<typename V2T::type_t, U, isEncoded> BaseHelper;
                using v2_type_t = typename ANReturnTypeSelector<V2T, isEncoded>::v2_type_t;
                typedef U type_t;

                template<typename V>
                constexpr static U getIfEncoded(
                        V const & value) {
                    return ANhelper0<U, V, isEncoded>::getIfEncoded(value);
                }

                template<typename V>
                constexpr static U getIfEncoded(
                        V const && value) {
                    return ANhelper0<U, V, isEncoded>::getIfEncoded(std::forward<V const>(value));
                }

                constexpr static U mulIfEncoded(
                        U const & value,
                        U const & AInv) {
                    return BaseHelper::mulIfEncoded(value, AInv);
                }

                constexpr static U mulIfEncoded(
                        U const && value,
                        U const && AInv) {
                    return BaseHelper::mulIfEncoded(std::forward<U const>(value), std::forward<U const>(AInv));
                }

                inline static AN_indicator_vector * createIndicatorVector() {
                    return BaseHelper::createIndicatorVector();
                }
            };

        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_AN_ANHELPER_TCC_ */

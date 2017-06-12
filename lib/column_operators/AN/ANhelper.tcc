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

namespace ahead {
    namespace bat {
        namespace ops {

            template<typename V2T, bool encoded>
            struct ANReturnTypeSelector {
                typedef typename TypeMap<V2T>::v2_encoded_t::v2_select_t v2_select_t;
            };

            template<typename V2T>
            struct ANReturnTypeSelector<V2T, false> {
                typedef typename V2T::v2_select_t v2_select_t;
            };

            /**
             * AN encoded values
             */
            template<typename T, typename U, bool>
            struct ANhelper0 {
                constexpr static T getValue(
                        U const & value) {
                    return static_cast<T>(value);
                }

                constexpr static T getValue(
                        U const && value) {
                    return static_cast<T>(value);
                }

                constexpr static T decode(
                        U const & encoded,
                        U const & AInv) {
                    return static_cast<T>(encoded * AInv);
                }

                constexpr static T decode(
                        U const && encoded,
                        U const && AInv) {
                    return static_cast<T>(encoded * AInv);
                }
            };

            /**
             * unencoded values
             */
            template<typename T, typename U>
            struct ANhelper0<T, U, false> {
                constexpr static T getValue(
                        U const & value) {
                    (void) value;
                    return T(0);
                }

                constexpr static T getValue(
                        U const && value) {
                    (void) value;
                    return T(0);
                }

                constexpr static T decode(
                        U const & unencoded,
                        U const & AInv) {
                    (void) AInv;
                    return static_cast<T>(unencoded);
                }

                constexpr static T decode(
                        U const && unencoded,
                        U const && AInv) {
                    (void) AInv;
                    return static_cast<T>(unencoded);
                }
            };

            template<typename V2T, typename U>
            struct ANhelper :
                    public ANhelper0<typename V2T::type_t, U, std::is_base_of<v2_anencoded_t, V2T>::value> {

                using v2_select_t = typename ANReturnTypeSelector<typename V2T::type_t, std::is_base_of<v2_anencoded_t, V2T>::value>::v2_select_t;
            };

        }
    }
}

#endif /* LIB_COLUMN_OPERATORS_AN_ANHELPER_TCC_ */

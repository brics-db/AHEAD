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
 * File:   ModularRedundant.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 07-04-2017 14:02
 */
#ifndef UTIL_MODULARREDUNDANT_HPP_
#define UTIL_MODULARREDUNDANT_HPP_

#include <array>
#include <exception>

namespace ahead {

    class modularity_exception :
            public std::runtime_error {
        using std::runtime_error::runtime_error;
    };

    template<typename _Tp, std::size_t _Modularity>
    struct NModularRedundantValue {
        typedef typename std::array<_Tp, _Modularity>::size_type size_type;

        std::array<_Tp, _Modularity> values;

        constexpr static const size_t modularity = _Modularity;

        NModularRedundantValue()
                : NModularRedundantValue(_Tp(0)) {
        }

        NModularRedundantValue(
                _Tp val)
                : values() {
            values.fill(val);
        }

        _Tp & operator[](
                size_type idx) {
            return values.at(idx);
        }
    };

    template<typename _Tp>
    struct DMRValue :
            public NModularRedundantValue<_Tp, 2> {
        using NModularRedundantValue<_Tp, 2>::NModularRedundantValue;

        constexpr static const size_t modularity = 2;
    };

    template<typename _Tp>
    struct TMRValue :
            public NModularRedundantValue<_Tp, 3> {
        using NModularRedundantValue<_Tp, 3>::NModularRedundancy;

        constexpr static const size_t modularity = 3;
    };

    template<std::size_t _Int, typename _Tp, std::size_t _Modularity>
    constexpr _Tp&
    get(
            NModularRedundantValue<_Tp, _Modularity>& __in) noexcept
            {
        return std::get<_Int>(__in.values);
    }

    template<std::size_t _Int, typename _Tp, std::size_t _Modularity>
    constexpr _Tp &&
    get(
            NModularRedundantValue<_Tp, _Modularity> && __in) noexcept
            {
        return std::get<_Int>(std::forward<NModularRedundantValue<_Tp, _Modularity>>(__in.values));
    }

    template<std::size_t _Int, typename _Tp, std::size_t _Modularity>
    constexpr const _Tp&
    get(
            const NModularRedundantValue<_Tp, _Modularity>& __in) noexcept
            {
        return std::get(__in.values);
    }

    template<typename _Tp, std::size_t _Modularity>
    struct __get_voted;

    template<typename _Tp>
    struct __get_voted<_Tp, 2> {
        static _Tp&
        get(
                NModularRedundantValue<_Tp, 2> & __in) {
            if (ahead::get<0>(__in) == ahead::get<1>(__in)) {
                return ahead::get<0>(__in);
            }
            throw modularity_exception("DMR: values do not match");
        }
        static _Tp get(
                NModularRedundantValue<_Tp, 2> && __in) {
            if (ahead::get<0>(__in) == ahead::get<1>(__in)) {
                return ahead::get<0>(__in);
            }
            throw modularity_exception("DMR: values do not match");
        }
        static const _Tp&
        get(
                const NModularRedundantValue<_Tp, 2> & __in) {
            if (ahead::get<0>(__in) == ahead::get<1>(__in)) {
                return ahead::get<0>(__in);
            }
            throw modularity_exception("DMR: values do not match");
        }
    };

    template<typename _Tp>
    struct __get_voted<_Tp, 3> {
        static _Tp&
        get(
                NModularRedundantValue<_Tp, 3> & __in) {
            if (ahead::get<0>(__in) == ahead::get<1>(__in) || ahead::get<0>(__in) == ahead::get<2>(__in)) {
                return ahead::get<0>(__in);
            } else if (ahead::get<1>(__in) == ahead::get<2>(__in)) {
                ahead::get<0>(__in) = ahead::get<1>(__in);
                return ahead::get<1>(__in);
            }
            throw modularity_exception("DMR: values do not match");
        }
        static _Tp get(
                NModularRedundantValue<_Tp, 3> && __in) {
            if (ahead::get<0>(__in) == ahead::get<1>(__in) || ahead::get<0>(__in) == ahead::get<2>(__in)) {
                return ahead::get<0>(__in);
            } else if (ahead::get<1>(__in) == ahead::get<2>(__in)) {
                ahead::get<0>(__in) = ahead::get<1>(__in);
                return ahead::get<1>(__in);
            }
            throw modularity_exception("DMR: values do not match");
        }
        static const _Tp&
        get(
                const NModularRedundantValue<_Tp, 3> & __in) {
            if (ahead::get<0>(__in) == ahead::get<1>(__in) || ahead::get<0>(__in) == ahead::get<2>(__in)) {
                return ahead::get<0>(__in);
            } else if (ahead::get<1>(__in) == ahead::get<2>(__in)) {
                ahead::get<0>(__in) = ahead::get<1>(__in);
                return ahead::get<1>(__in);
            }
            throw modularity_exception("DMR: values do not match");
        }
    };

    template<typename _Tp, std::size_t _Modularity>
    _Tp & vote_majority(
            NModularRedundantValue<_Tp, _Modularity> & __in) {
        return ahead::__get_voted<_Tp, _Modularity>::get(__in);
    }

    template<typename _Tp, std::size_t _Modularity>
    _Tp vote_majority(
            NModularRedundantValue<_Tp, _Modularity> && __in) {
        return ahead::__get_voted<_Tp, _Modularity>::get(std::forward<NModularRedundantValue<_Tp, 3>>(__in));
    }

    template<typename _Tp, std::size_t _Modularity>
    const _Tp & vote_majority(
            const NModularRedundantValue<_Tp, _Modularity> & __in) {
        return ahead::__get_voted<_Tp, _Modularity>::get(__in);
    }
}

#endif /* UTIL_MODULARREDUNDANT_HPP_ */

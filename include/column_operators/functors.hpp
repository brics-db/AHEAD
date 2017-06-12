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
 * File:   functors.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 24-05-2017 14:04
 */
#ifndef INCLUDE_COLUMN_OPERATORS_FUNCTORS_HPP_
#define INCLUDE_COLUMN_OPERATORS_FUNCTORS_HPP_

namespace ahead {
    namespace bat {
        namespace ops {

            template<typename _Op = void>
            struct add {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) + _Op().operator()(args...)))
                        -> decltype(std::forward<_Tp>(__t) + _Op().operator()(args...))
                        {
                    return std::forward<_Tp>(__t) + _Op().operator()(args...);
                }
            };

            template<>
            struct add<void> {
                template<typename _Tp, typename _Up>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Up&& __u) const noexcept(noexcept(std::forward<_Tp>(__t) + std::forward<_Up>(__u)))
                        -> decltype(std::forward<_Tp>(__t) + std::forward<_Up>(__u))
                        {
                    return std::forward<_Tp>(__t) + std::forward<_Up>(__u);
                }
            };

            template<typename _Op = void>
            struct sub {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) - _Op().operator()(args...)))
                        -> decltype(std::forward<_Tp>(__t) - _Op().operator()(args...))
                        {
                    return std::forward<_Tp>(__t) - _Op().operator()(args...);
                }
            };

            template<>
            struct sub<void> {
                template<typename _Tp, typename _Up>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Up&& __u) const noexcept(noexcept(std::forward<_Tp>(__t) - std::forward<_Up>(__u)))
                        -> decltype(std::forward<_Tp>(__t) - std::forward<_Up>(__u))
                        {
                    return std::forward<_Tp>(__t) - std::forward<_Up>(__u);
                }
            };

            template<typename _Op = void>
            struct mul {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) * _Op().operator()(args...)))
                        -> decltype(std::forward<_Tp>(__t) * _Op().operator()(args...))
                        {
                    return std::forward<_Tp>(__t) * _Op().operator()(args...);
                }
            };

            template<>
            struct mul<void> {
                template<typename _Tp, typename _Up>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Up&& __u) const noexcept(noexcept(std::forward<_Tp>(__t) * std::forward<_Up>(__u)))
                        -> decltype(std::forward<_Tp>(__t) * std::forward<_Up>(__u))
                        {
                    return std::forward<_Tp>(__t) * std::forward<_Up>(__u);
                }
            };

            template<typename _Op = void>
            struct div {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) / _Op().operator()(args...)))
                        -> decltype(std::forward<_Tp>(__t) / _Op().operator()(args...))
                        {
                    return std::forward<_Tp>(__t) / _Op().operator()(args...);
                }
            };

            template<>
            struct div<void> {
                template<typename _Tp, typename _Up>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Up&& __u) const noexcept(noexcept(std::forward<_Tp>(__t) / std::forward<_Up>(__u)))
                        -> decltype(std::forward<_Tp>(__t) / std::forward<_Up>(__u))
                        {
                    return std::forward<_Tp>(__t) / std::forward<_Up>(__u);
                }
            };

        }
    }
}

#endif /* INCLUDE_COLUMN_OPERATORS_FUNCTORS_HPP_ */

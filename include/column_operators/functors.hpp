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

#include <util/v2types.hpp>
#include <cstring>

namespace ahead {
    namespace bat {
        namespace ops {

            struct functor {
            };

            template<typename _Op = void>
            struct ADD :
                    public functor {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) + _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(std::forward<_Tp>(__t) + _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return std::forward<_Tp>(__t) + _Op().operator()(std::forward<_Types>(args)...);
                }
            };

            template<>
            struct ADD<void> :
                    public functor {
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
            struct SUB :
                    public functor {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) - _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(std::forward<_Tp>(__t) - _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return std::forward<_Tp>(__t) - _Op().operator()(std::forward<_Types>(args)...);
                }
            };

            template<>
            struct SUB<void> :
                    public functor {
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
            struct MUL :
                    public functor {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) * _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(std::forward<_Tp>(__t) * _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return std::forward<_Tp>(__t) * _Op().operator()(std::forward<_Types>(args)...);
                }
            };

            template<>
            struct MUL<void> :
                    public functor {
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
            struct DIV :
                    public functor {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) / _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(std::forward<_Tp>(__t) / _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return std::forward<_Tp>(__t) / _Op().operator()(std::forward<_Types>(args)...);
                }
            };

            template<>
            struct DIV<void> :
                    public functor {
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

            template<typename _Op = void>
            struct AND :
                    public functor {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) & _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(std::forward<_Tp>(__t) & _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return std::forward<_Tp>(__t) & _Op().operator()(std::forward<_Types>(args)...);
                }
            };

            template<>
            struct AND<void> :
                    public functor {
                template<typename _Tp, typename _Up>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Up&& __u) const noexcept(noexcept(std::forward<_Tp>(__t) & std::forward<_Up>(__u)))
                        -> decltype(std::forward<_Tp>(__t) & std::forward<_Up>(__u))
                        {
                    return std::forward<_Tp>(__t) & std::forward<_Up>(__u);
                }
            };

            template<typename _Op = void>
            struct OR :
                    public functor {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) | _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(std::forward<_Tp>(__t) | _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return std::forward<_Tp>(__t) | _Op().operator()(std::forward<_Types>(args)...);
                }
            };

            template<>
            struct OR<void> :
                    public functor {
                template<typename _Tp, typename _Up>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Up&& __u) const noexcept(noexcept(std::forward<_Tp>(__t) | std::forward<_Up>(__u)))
                        -> decltype(std::forward<_Tp>(__t) | std::forward<_Up>(__u))
                        {
                    return std::forward<_Tp>(__t) | std::forward<_Up>(__u);
                }
            };

            template<typename _Op = void>
            struct NOT :
                    public functor {
                template<typename ... _Types>
                constexpr
                auto operator()(
                        _Types && ... args) const noexcept(noexcept(! _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(! _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return !_Op().operator()(std::forward<_Types>(args)...);
                }
            };

            template<>
            struct NOT<void> :
                    public functor {
                template<typename _Up>
                constexpr
                auto operator()(
                        _Up&& __u) const noexcept(noexcept(! std::forward<_Up>(__u)))
                        -> decltype(! std::forward<_Up>(__u))
                        {
                    return !std::forward<_Up>(__u);
                }
            };

            template<typename _Op = void>
            struct EQ :
                    public functor {
                template<typename _Tp, typename ... _Types>
                constexpr
                auto operator()(
                        _Tp && __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<_Tp>(__t) == _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(std::forward<_Tp>(__t) == _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return std::forward<_Tp>(__t) == _Op().operator()(std::forward<_Types>(args)...);
                }

                template<typename ... _Types>
                constexpr
                auto operator()(
                        str_t && __t,
                        _Types && ... args) const noexcept(noexcept(std::forward<str_t>(__t) == _Op().operator()(std::forward<_Types>(args)...)))
                        -> decltype(std::forward<str_t>(__t) == _Op().operator()(std::forward<_Types>(args)...))
                        {
                    return EQ()(std::strcmp(std::forward<str_t>(__t), _Op().operator()(std::forward<_Types>(args)...)), 0);
                }

                template<typename ... _Types>
                constexpr
                auto operator()(
                        cstr_t && __t,
                        _Types && ... args) const noexcept(noexcept(EQ()(std::strcmp(std::forward<cstr_t>(__t), _Op().operator()(std::forward<_Types>(args)...)), 0)))
                        -> decltype(EQ()(std::strcmp(std::forward<cstr_t>(__t), _Op().operator()(std::forward<_Types>(args)...)), 0))
                        {
                    return EQ()(std::strcmp(std::forward<cstr_t>(__t), _Op().operator()(std::forward<_Types>(args)...)), 0);
                }
            };

            template<>
            struct EQ<void> :
                    public functor {
                template<typename _Tp, typename _Up>
                constexpr
                auto operator()(
                        _Tp&& __t,
                        _Up&& __u) const noexcept(noexcept(std::forward<_Tp>(__t) == std::forward<_Up>(__u)))
                        -> decltype(std::forward<_Tp>(__t) == std::forward<_Up>(__u))
                        {
                    return std::forward<_Tp>(__t) == std::forward<_Up>(__u);
                }

                constexpr
                auto operator()(
                        str_t && __t,
                        str_t && __u) const noexcept(noexcept(std::strcmp(std::forward<str_t>(__t), std::forward<str_t>(__u)) == 0))
                        -> decltype(std::strcmp(std::forward<str_t>(__t), std::forward<str_t>(__u)) == 0)
                        {
                    return std::strcmp(std::forward<str_t>(__t), std::forward<str_t>(__u)) == 0;
                }

                constexpr
                auto operator()(
                        cstr_t && __t,
                        str_t && __u) const noexcept(noexcept(std::strcmp(std::forward<cstr_t>(__t), std::forward<str_t>(__u)) == 0))
                        -> decltype(std::strcmp(std::forward<cstr_t>(__t), std::forward<str_t>(__u)) == 0)
                        {
                    return std::strcmp(std::forward<cstr_t>(__t), std::forward<str_t>(__u)) == 0;
                }

                constexpr
                auto operator()(
                        str_t && __t,
                        cstr_t && __u) const noexcept(noexcept(std::strcmp(std::forward<str_t>(__t), std::forward<cstr_t>(__u)) == 0))
                        -> decltype(std::strcmp(std::forward<str_t>(__t), std::forward<cstr_t>(__u)) == 0)
                        {
                    return std::strcmp(std::forward<str_t>(__t), std::forward<cstr_t>(__u)) == 0;
                }

                constexpr
                auto operator()(
                        cstr_t && __t,
                        cstr_t && __u) const noexcept(noexcept(std::strcmp(std::forward<cstr_t>(__t), std::forward<cstr_t>(__u)) == 0))
                        -> decltype(std::strcmp(std::forward<cstr_t>(__t), std::forward<cstr_t>(__u)) == 0)
                        {
                    return std::strcmp(std::forward<cstr_t>(__t), std::forward<cstr_t>(__u)) == 0;
                }
            };

        }
    }
}

#endif /* INCLUDE_COLUMN_OPERATORS_FUNCTORS_HPP_ */

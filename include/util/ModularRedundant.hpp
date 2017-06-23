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
#include <tuple>
#include <type_traits>
#include <sstream>

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
    constexpr _Tp &
    get(
            NModularRedundantValue<_Tp, _Modularity> & __in) noexcept
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
    constexpr const _Tp &
    get(
            const NModularRedundantValue<_Tp, _Modularity> & __in) noexcept
            {
        return std::get(__in.values);
    }

    template<std::size_t _Int, typename _Tp, std::size_t _Modularity>
    constexpr const _Tp &&
    get(
            const NModularRedundantValue<_Tp, _Modularity> && __in) noexcept
            {
        return std::get<_Int>(std::forward<NModularRedundantValue<_Tp, _Modularity>>(__in.values));
    }

    namespace Private {
        template<std::size_t _Modularity, typename ... _Types>
        struct __get_voted {
        };

        template<typename _Tp>
        struct __get_voted<2, _Tp> {
            typedef NModularRedundantValue<_Tp, 2> DMR;
            static _Tp&
            value(
                    DMR & __in) {
                if (ahead::get<0>(__in) == ahead::get<1>(__in)) {
                    return ahead::get<0>(__in);
                }
                throw modularity_exception("DMR: values do not match");
            }

            static _Tp value(
                    DMR && __in) {
                if (ahead::get<0>(std::forward<DMR>(__in)) == ahead::get<1>(std::forward<DMR>(__in))) {
                    return ahead::get<0>(std::forward<DMR>(__in));
                }
                throw modularity_exception("DMR: values do not match");
            }

            static const _Tp&
            value(
                    const DMR & __in) {
                if (ahead::get<0>(__in) == ahead::get<1>(__in)) {
                    return ahead::get<0>(__in);
                }
                throw modularity_exception("DMR: values do not match");
            }

            static const _Tp get(
                    const DMR && __in) {
                if (ahead::get<0>(std::forward<DMR>(__in)) == ahead::get<1>(std::forward<DMR>(__in))) {
                    return ahead::get<0>(std::forward<DMR>(__in));
                }
                throw modularity_exception("DMR: values do not match");
            }
        };

        template<typename _Tp>
        struct __get_voted<3, _Tp> {
            typedef NModularRedundantValue<_Tp, 3> TMR;
            static _Tp &
            value(
                    TMR & __in) {
                if (ahead::get<0>(__in) == ahead::get<1>(__in) || ahead::get<0>(__in) == ahead::get<2>(__in)) {
                    return ahead::get<0>(__in);
                } else if (ahead::get<1>(__in) == ahead::get<2>(__in)) {
                    ahead::get<0>(__in) = ahead::get<1>(__in);
                    return ahead::get<1>(__in);
                }
                throw modularity_exception("DMR: values do not match");
            }

            static _Tp &&
            value(
                    TMR && __in) {
                if (ahead::get<0>(std::forward<TMR>(__in)) == ahead::get<1>(std::forward<TMR>(__in)) || ahead::get<0>(std::forward<TMR>(__in)) == ahead::get<2>(std::forward<TMR>(__in))) {
                    return ahead::get<0>(std::forward<TMR>(__in));
                } else if (ahead::get<1>(std::forward<TMR>(__in)) == ahead::get<2>(std::forward<TMR>(__in))) {
                    ahead::get<0>(std::forward<TMR>(__in)) = ahead::get<1>(std::forward<TMR>(__in));
                    return ahead::get<1>(std::forward<TMR>(__in));
                }
                throw modularity_exception("DMR: values do not match");
            }

            static const _Tp &
            value(
                    const TMR & __in) {
                if (ahead::get<0>(__in) == ahead::get<1>(__in) || ahead::get<0>(__in) == ahead::get<2>(__in)) {
                    return ahead::get<0>(__in);
                } else if (ahead::get<1>(__in) == ahead::get<2>(__in)) {
                    ahead::get<0>(__in) = ahead::get<1>(__in);
                    return ahead::get<1>(__in);
                }
                throw modularity_exception("DMR: values do not match");
            }

            static const _Tp &&
            value(
                    const TMR && __in) {
                if (ahead::get<0>(std::forward<TMR>(__in)) == ahead::get<1>(std::forward<TMR>(__in)) || ahead::get<0>(std::forward<TMR>(__in)) == ahead::get<2>(std::forward<TMR>(__in))) {
                    return ahead::get<0>(std::forward<TMR>(__in));
                } else if (ahead::get<1>(std::forward<TMR>(__in)) == ahead::get<2>(std::forward<TMR>(__in))) {
                    ahead::get<0>(std::forward<TMR>(__in)) = ahead::get<1>(std::forward<TMR>(__in));
                    return ahead::get<1>(std::forward<TMR>(__in));
                }
                throw modularity_exception("DMR: values do not match");
            }
        };

        template<std::size_t _Modularity, std::size_t N, typename ... _Types>
        struct __get_voted_tuple_helper {
        };

        template<std::size_t N, typename ... _Types>
        struct __get_voted_tuple_helper<2, N, _Types...> {
            typedef NModularRedundantValue<std::tuple<_Types...>, 2> DMR;
            static constexpr decltype(auto) isSizeSame(
                    DMR & __in) {
                return std::tuple_cat(__get_voted_tuple_helper<2, N - 1, _Types...>::isSizeSame(__in),
                        std::make_tuple(std::get<N - 1>(ahead::get<0>(__in))->size() == std::get<N - 1>(ahead::get<1>(__in))->size()));
            }
            template<typename ... _Types2>
            static decltype(auto) get(
                    DMR & __in1,
                    std::tuple<_Types2...> __in2) {
                auto sub = __get_voted_tuple_helper<2, N - 1, _Types...>::get(__in1, __in2);
                if (!std::get<N - 1>(__in2)) {
                    throw modularity_exception("DMR: sizes don't match");
                }
                return std::tuple_cat(sub, std::make_tuple(std::get<N - 1>(ahead::get<0>(__in1))->size()));
            }
            static decltype(auto) content(
                    DMR & __in) {
                auto sub = __get_voted_tuple_helper<2, N - 1, _Types...>::content(__in);
                auto bat1 = std::get<0>(ahead::get<0>(__in));
                auto bat2 = std::get<0>(ahead::get<1>(__in));
                if (bat1->size() != bat2->size()) {
                    std::stringstream sserr;
                    sserr << "DMR: size does not match! BATType = <" << bat1->type_head().pretty_name() << ", " << bat1->type_tail().pretty_name() << "> (@" << __FILE__ << ':' << __LINE__ << ')';
                    throw modularity_exception(sserr.str().c_str());
                }
                auto iter1 = bat1->begin();
                auto iter2 = bat2->begin();
                bool isSame = true;
                for (; isSame && iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2) {
                    isSame &= iter1->tail() == iter2->tail();
                }
                delete iter1;
                delete iter2;
                if (!isSame) {
                    std::stringstream sserr;
                    sserr << "DMR: content does not match! BATType = <" << bat1->type_head().pretty_name() << ", " << bat1->type_tail().pretty_name() << "> (@" << __FILE__ << ':' << __LINE__ << ')';
                    throw modularity_exception(sserr.str().c_str());
                }
                return std::tuple_cat(sub, std::make_tuple(std::get<N - 1>(ahead::get<0>(__in))));
            }
        };

        template<typename ... _Types>
        struct __get_voted_tuple_helper<2, 1, _Types...> {
            typedef NModularRedundantValue<std::tuple<_Types...>, 2> DMR;
            static constexpr decltype(auto) isSizeSame(
                    DMR & __in) {
                return std::make_tuple(std::get<0>(ahead::get<0>(__in))->size() == std::get<0>(ahead::get<1>(__in))->size());
            }
            template<typename ... _Types2>
            static decltype(auto) get(
                    DMR & __in1,
                    std::tuple<_Types2...> __in2) {
                if (!std::get<0>(__in2)) {
                    throw modularity_exception("DMR: sizes don't match");
                }
                return std::make_tuple(std::get<0>(ahead::get<0>(__in1))->size());
            }
            static decltype(auto) content(
                    DMR & __in) {
                auto bat1 = std::get<0>(ahead::get<0>(__in));
                auto bat2 = std::get<0>(ahead::get<1>(__in));
                if (bat1->size() != bat2->size()) {
                    std::stringstream sserr;
                    sserr << "DMR: size does not match! BATType = <" << bat1->type_head().pretty_name() << ", " << bat1->type_tail().pretty_name() << "> (@" << __FILE__ << ':' << __LINE__ << ')';
                    throw modularity_exception(sserr.str().c_str());
                }
                auto iter1 = bat1->begin();
                auto iter2 = bat2->begin();
                bool isSame = true;
                for (; isSame && iter1->hasNext() && iter2->hasNext(); ++*iter1, ++*iter2) {
                    isSame &= iter1->tail() == iter2->tail();
                }
                delete iter1;
                delete iter2;
                if (!isSame) {
                    std::stringstream sserr;
                    sserr << "DMR: content does not match! BATType = <" << bat1->type_head().pretty_name() << ", " << bat1->type_tail().pretty_name() << "> (@" << __FILE__ << ':' << __LINE__ << ')';
                    throw modularity_exception(sserr.str().c_str());
                }
                return std::make_tuple(std::get<0>(ahead::get<0>(__in)));
            }
        };

        template<typename ... _Types>
        struct __get_voted<2, _Types...> {
            typedef NModularRedundantValue<std::tuple<_Types...>, 2> DMR;
            static decltype(auto) size(
                    DMR & __in) {
                return __get_voted_tuple_helper<2, std::tuple_size<std::tuple<_Types...>>::value, _Types...>::get(__in,
                        __get_voted_tuple_helper<2, std::tuple_size<std::tuple<_Types...>>::value, _Types...>::isSizeSame(__in));
            }
            static decltype(auto) content(
                    DMR & __in) {
                return __get_voted_tuple_helper<2, std::tuple_size<std::tuple<_Types...>>::value, _Types...>::content(__in);
            }
        };

        template<typename BATType>
        oid_t BATsize(
                BATType * bat1,
                BATType * bat2) {
            oid_t sz1 = bat1->size();
            oid_t sz2 = bat2->size();
            if (sz1 != sz2) {
                std::stringstream sserr;
                sserr << "DMR: sizes don't match! BATType = <" << bat1->type_head().pretty_name() << ", " << bat1->type_tail().pretty_name() << "> (@" << __FILE__ << ':' << __LINE__ << ')';
                throw modularity_exception(sserr.str().c_str());
            }
            return bat1->size();
        }
    }

    template<typename _Tp, std::size_t _Modularity>
    _Tp & vote_majority_value(
            NModularRedundantValue<_Tp, _Modularity> & __in) {
        return ahead::Private::__get_voted<_Modularity, _Tp>::value(__in);
    }

    template<typename _Tp, std::size_t _Modularity>
    _Tp && vote_majority_value(
            NModularRedundantValue<_Tp, _Modularity> && __in) {
        return ahead::Private::__get_voted<_Modularity, _Tp>::value(std::forward<NModularRedundantValue<_Tp, _Modularity>>(__in));
    }

    template<typename _Tp, std::size_t _Modularity>
    const _Tp & vote_majority_value(
            const NModularRedundantValue<_Tp, _Modularity> & __in) {
        return ahead::Private::__get_voted<_Modularity, _Tp>::value(__in);
    }

    template<typename _Tp, std::size_t _Modularity>
    const _Tp && vote_majority_value(
            const NModularRedundantValue<_Tp, _Modularity> && __in) {
        return ahead::Private::__get_voted<_Modularity, _Tp>::value(std::forward<NModularRedundantValue<_Tp, _Modularity>>(__in));
    }

    template<std::size_t _Modularity, typename ... _Types>
    auto vote_majority_tuple(
            NModularRedundantValue<std::tuple<_Types...>, _Modularity> & __in)
            ->decltype(ahead::Private::__get_voted<_Modularity, _Types...>::content(__in)) {
        return ahead::Private::__get_voted<_Modularity, _Types...>::content(__in);
    }

    template<std::size_t _Modularity, typename ... _Types>
    auto vote_majority_tuple_size(
            NModularRedundantValue<std::tuple<_Types...>, _Modularity> & __in)
            ->decltype(ahead::Private::__get_voted<_Modularity, _Types...>::size(__in)) {
        return ahead::Private::__get_voted<_Modularity, _Types...>::size(__in);
    }

}

#endif /* UTIL_MODULARREDUNDANT_HPP_ */

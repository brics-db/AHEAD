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
 * File: v2typeconversion.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 31. March 2017, 09:54
 */

#ifndef V2TYPECONVERSION_HPP
#define V2TYPECONVERSION_HPP

#include <cinttypes>

#include <boost/multiprecision/cpp_int.hpp>

using boost::multiprecision::uint128_t;

namespace ahead {

    namespace Private {

        template<typename T, typename S>
        struct v2converter {
        };

        template<>
        struct v2converter<uint64_t, uint128_t> {
            static uint64_t doIt(uint128_t & source) {
                uint64_t target = 0;
                const unsigned limb_num = source.backend().size(); // number of limbs
                const unsigned limb_bits = sizeof(boost::multiprecision::limb_type) * CHAR_BIT; // size of limb in bits
                for (unsigned i = 0; i < limb_num && ((i * limb_bits) < (sizeof(target) * 8)); ++i) {
                    target |= (source.backend().limbs()[i]) << (i * limb_bits);
                }
                return target;
            }
        };

    }

    template<typename T, typename S>
    T v2convert(S & source) {
        return Private::v2converter<T, S>::doIt(source);
    }

}

#endif /* V2TYPECONVERSION_HPP */

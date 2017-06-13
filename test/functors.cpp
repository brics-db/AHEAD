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
 * File:   functors.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 24-05-2017 14:39
 */

#include <iostream>

#include <column_operators/functors.hpp>

template<typename Op, typename ... _Types>
constexpr
auto function(
        _Types ... args) noexcept(noexcept(Op()(std::forward<_Types>(args)...)))
        -> decltype(Op()(std::forward<_Types>(args)...)) {
    return Op()(std::forward<_Types>(args)...);
}

int main(
        int argc,
        char ** argv) {
    char* end;
    ssize_t a(5), b(4), c(3), d(2);
    if (argc > 1) {
        a = strtoll(argv[1], &end, 10);
    }
    if (argc > 2) {
        b = strtoll(argv[2], &end, 10);
    }
    if (argc > 3) {
        c = strtoll(argv[3], &end, 10);
    }
    if (argc > 4) {
        d = strtoll(argv[4], &end, 10);
    }
    std::cout << "a+b=" << function<ahead::bat::ops::ADD<>>(a, b) << std::endl;
    std::cout << "a-b=" << function<ahead::bat::ops::SUB<>>(a, b) << std::endl;
    std::cout << "a*b=" << function<ahead::bat::ops::MUL<>>(a, b) << std::endl;
    std::cout << "a/b=" << function<ahead::bat::ops::DIV<>>(a, b) << std::endl;
    std::cout << "a+b+c=" << function<ahead::bat::ops::ADD<ahead::bat::ops::ADD<>>>(a, b, c) << std::endl;
    std::cout << "a+(b*(c/d))=" << function<ahead::bat::ops::ADD<ahead::bat::ops::MUL<ahead::bat::ops::DIV<>>> >(a, b, c, d) << std::endl;
}

// Copyright (c) 2016-2017 Till Kolditz
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
 * File:   ColumnMetaData.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 9. Dezember 2016, 12:26
 */

#ifndef COLUMNMETADATA_HPP
#define COLUMNMETADATA_HPP

#include <cinttypes>
#include <variant>
#include <type_traits>
#include <climits>

#include <util/resilience.hpp>

namespace ahead {

    struct bits_t {
        uint32_t value;

        constexpr bits_t()
                : value(0) {
        }

        constexpr bits_t(
                uint32_t value)
                : value(value) {
        }
    };

    struct bytes_t {
        uint32_t value;

        constexpr bytes_t()
                : value(0) {
        }

        constexpr bytes_t(
                uint32_t value)
                : value(value) {
        }
    };

    template<typename T>
    inline constexpr bits_t size_bits = bits_t(sizeof(T) * CHAR_BIT);

    template<typename T>
    inline constexpr size_t size_bits_v = sizeof(T) * CHAR_BIT;

    template<typename T>
    inline constexpr bytes_t size_bytes = bytes_t(sizeof(T));

    template<typename T>
    inline constexpr size_t size_bytes_v = sizeof(T);

    struct data_width_t {
        std::variant<bits_t, bytes_t> var;

        constexpr data_width_t(
                bits_t value)
                : var(value) {
        }

        constexpr data_width_t(
                bytes_t value)
                : var(value) {
        }

        data_width_t(
                const data_width_t & other)
                : var(other.var) {
        }

        data_width_t(
                const data_width_t && other)
                : var(other.var) {
        }

        data_width_t & operator=(
                const data_width_t & other) = default;

        data_width_t & operator=(
                data_width_t && other) = default;

        data_width_t & operator=(
                const bits_t & value) {
            var = value;
            return *this;
        }

        data_width_t & operator=(
                const bytes_t & value) {
            var = value;
            return *this;
        }
    };

    template<typename T>
    constexpr inline uint32_t get(
            data_width_t);

    template<>
    constexpr inline uint32_t get<bits_t>(
            data_width_t dw) {
        return std::visit([](auto && arg) -> uint32_t {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, bits_t>) {
                return arg.value;
            } else if constexpr (std::is_same_v<T, bytes_t>) {
                return arg.value * CHAR_BIT;
            }
        }, dw.var);
    }

    template<>
    constexpr inline uint32_t get<bytes_t>(
            data_width_t dw) {
        return std::visit([](auto && arg) -> uint32_t {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, bits_t>) {
                return arg.value / CHAR_BIT;
            } else if constexpr (std::is_same_v<T, bytes_t>) {
                return arg.value;
            }
        }, dw.var);
    }

    struct ColumnMetaData {

        data_width_t width;
        uint64_t seqbase;
        A_t AN_A; // at most 16 bit A's for now
        uint64_t AN_Ainv;
        uint64_t AN_unencMaxU;
        int64_t AN_unencMinS;

        ColumnMetaData()
                : width(bits_t(0)),
                  seqbase(0),
                  AN_A(1),
                  AN_Ainv(1),
                  AN_unencMaxU(0),
                  AN_unencMinS(0) {
        }

        ColumnMetaData(
                data_width_t width)
                : width(width),
                  seqbase(0),
                  AN_A(1),
                  AN_Ainv(1),
                  AN_unencMaxU(0),
                  AN_unencMinS(0) {
        }

        ColumnMetaData(
                oid_t seqbase)
                : width(bits_t(0)),
                  seqbase(seqbase),
                  AN_A(1),
                  AN_Ainv(1),
                  AN_unencMaxU(0),
                  AN_unencMinS(0) {
        }

        ColumnMetaData(
                data_width_t width,
                A_t AN_A,
                uint64_t AN_Ainv,
                uint64_t AN_unencMaxU,
                int64_t AN_unencMinS)
                : width(width),
                  seqbase(0),
                  AN_A(AN_A),
                  AN_Ainv(AN_Ainv),
                  AN_unencMaxU(AN_unencMaxU),
                  AN_unencMinS(AN_unencMinS) {
        }

        ColumnMetaData(
                const ColumnMetaData & cmd) = default;

        ColumnMetaData(
                ColumnMetaData && cmd) = default;

        ColumnMetaData & operator=(
                const ColumnMetaData &) = default;
    };

}

#endif /* COLUMNMETADATA_HPP */

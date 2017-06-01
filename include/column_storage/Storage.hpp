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
 * Storage.hpp
 *
 *  Created on: 31.03.2017
 *      Author: Till Kolditz <till.kolditz@gmail.com>
 */

#ifndef COLUMN_STORAGE_STORAGE_HPP_
#define COLUMN_STORAGE_STORAGE_HPP_

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/ColumnBat.h>
#include <column_storage/TempStorage.hpp>

namespace ahead {

    extern template class ColumnBAT<v2_tinyint_t> ;
    extern template class ColumnBAT<v2_shortint_t> ;
    extern template class ColumnBAT<v2_int_t> ;
    extern template class ColumnBAT<v2_bigint_t> ;
    extern template class ColumnBAT<v2_char_t> ;
    extern template class ColumnBAT<v2_str_t> ;
    extern template class ColumnBAT<v2_fixed_t> ;
    extern template class ColumnBAT<v2_id_t> ;
    extern template class ColumnBAT<v2_size_t> ;
    extern template class ColumnBAT<v2_oid_t> ;
    extern template class ColumnBAT<v2_restiny_t> ;
    extern template class ColumnBAT<v2_resshort_t> ;
    extern template class ColumnBAT<v2_resint_t> ;
    extern template class ColumnBAT<v2_resbigint_t> ;

#define TEMPBAT(V2Head) \
extern template class TempBAT<V2Head, v2_void_t> ; \
extern template class TempBAT<V2Head, v2_tinyint_t> ; \
extern template class TempBAT<V2Head, v2_shortint_t> ; \
extern template class TempBAT<V2Head, v2_int_t> ; \
extern template class TempBAT<V2Head, v2_bigint_t> ; \
extern template class TempBAT<V2Head, v2_char_t> ; \
extern template class TempBAT<V2Head, v2_str_t> ; \
extern template class TempBAT<V2Head, v2_fixed_t> ; \
extern template class TempBAT<V2Head, v2_id_t> ; \
extern template class TempBAT<V2Head, v2_size_t> ; \
extern template class TempBAT<V2Head, v2_oid_t> ; \
extern template class TempBAT<V2Head, v2_restiny_t> ; \
extern template class TempBAT<V2Head, v2_resshort_t> ; \
extern template class TempBAT<V2Head, v2_resint_t> ; \
extern template class TempBAT<V2Head, v2_resbigint_t> ; \
extern template class TempBAT<V2Head, v2_resstr_t> ;

    TEMPBAT(v2_void_t)
    TEMPBAT(v2_tinyint_t)
    TEMPBAT(v2_shortint_t)
    TEMPBAT(v2_int_t)
    TEMPBAT(v2_bigint_t)
    TEMPBAT(v2_char_t)
    TEMPBAT(v2_str_t)
    TEMPBAT(v2_fixed_t)
    TEMPBAT(v2_id_t)
    TEMPBAT(v2_size_t)
    TEMPBAT(v2_oid_t)
    TEMPBAT(v2_restiny_t)
    TEMPBAT(v2_resshort_t)
    TEMPBAT(v2_resint_t)
    TEMPBAT(v2_resbigint_t)
    TEMPBAT(v2_resstr_t)

#undef TEMPBAT

}

#endif /* COLUMN_STORAGE_STORAGE_HPP_ */

// Copyright (c) 2010 Benjamin Schlegel
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

/***
 * @author Benjamin Schlegel
 */
#ifndef BAT_H
#define BAT_H

#include <atomic>
#include <utility>

#include <boost/type_index.hpp>

#include <ColumnStore.h>
#include <column_storage/BatIterator.h>
#include <column_storage/ColumnMetaData.hpp>
#include <column_storage/ColumnDescriptor.hpp>

template<typename Head, typename Tail>
struct BAT {

    typedef Head v2_head_t;
    typedef Tail v2_tail_t;
    typedef typename v2_head_t::type_t head_t;
    typedef typename v2_tail_t::type_t tail_t;
    typedef typename ColumnDescriptorContainerType<v2_head_t>::container_t coldesc_head_container_t;
    typedef typename ColumnDescriptorContainerType<v2_tail_t>::container_t coldesc_tail_container_t;
    typedef ColumnDescriptor<v2_head_t, coldesc_head_container_t> coldesc_head_t;
    typedef ColumnDescriptor<v2_tail_t, coldesc_tail_container_t> coldesc_tail_t;

    coldesc_head_t head;
    coldesc_tail_t tail;

    BAT()
            : head(ColumnMetaData(sizeof(typename Head::type_t))), tail(sizeof(typename Tail::type_t)) {
    }

    BAT(coldesc_head_t head, coldesc_tail_t tail)
            : head(std::move(head)), tail(std::move(tail)) {
    }

    virtual ~BAT() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BATIterator<Head, Tail>* begin() = 0;

    /** append an item */
    virtual void append(std::pair<head_t, tail_t>& p) = 0;
    virtual void append(std::pair<head_t, tail_t>&& p) = 0;

    virtual BAT<Tail, Head>* reverse() = 0;

    virtual BAT<Head, Head>* mirror_head() = 0;

    virtual BAT<Tail, Tail>* mirror_tail() = 0;

    /** size of column, obtained through the iterator */
    virtual unsigned size() = 0;

    /** Compute the actual memory consumption of the BAT */
    virtual size_t consumption() = 0;

    /** Compute the projected memory consumption of the BAT -- especially for AN encoded BATs */
    virtual size_t consumptionProjected() = 0;

    virtual boost::typeindex::type_index type_head() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_head_t>();
    }

    virtual boost::typeindex::type_index type_tail() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_tail_t>();
    }
};

template<>
struct BAT<v2_void_t, v2_void_t> {

    typedef v2_void_t Head;
    typedef Head v2_head_t;
    typedef v2_void_t Tail;
    typedef Tail v2_tail_t;
    typedef typename v2_head_t::type_t head_t;
    typedef typename v2_tail_t::type_t tail_t;
    typedef typename ColumnDescriptorContainerType<v2_head_t>::container_t coldesc_head_container_t;
    typedef typename ColumnDescriptorContainerType<v2_tail_t>::container_t coldesc_tail_container_t;
    typedef ColumnDescriptor<v2_head_t, coldesc_head_container_t> coldesc_head_t;
    typedef ColumnDescriptor<v2_tail_t, coldesc_tail_container_t> coldesc_tail_t;

    coldesc_head_t head;
    coldesc_tail_t tail;

    BAT()
            : head(), tail() {
    }

    BAT(coldesc_head_t& head, coldesc_tail_t& tail)
            : head(head), tail(tail) {
    }

    BAT(coldesc_head_t&& head, coldesc_tail_t&& tail)
            : head(std::forward<coldesc_head_t>(head)), tail(std::forward<coldesc_tail_t>(tail)) {
    }

    virtual ~BAT() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BATIterator<Head, Tail>* begin() = 0;

    /** append an item */
    virtual void append(std::pair<head_t, tail_t>& p) = 0;
    virtual void append(std::pair<head_t, tail_t>&& p) = 0;

    virtual BAT<Tail, Head>* reverse() = 0;

    virtual BAT<Head, Head>* mirror_head() = 0;

    virtual BAT<Tail, Tail>* mirror_tail() = 0;

    /** size of column, obtained through the iterator */
    virtual unsigned size() = 0;

    /** Compute the actual memory consumption of the BAT */
    virtual size_t consumption() = 0;

    /** Compute the projected memory consumption of the BAT -- especially for AN encoded BATs */
    virtual size_t consumptionProjected() = 0;

    virtual boost::typeindex::type_index type_head() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_head_t>();
    }

    virtual boost::typeindex::type_index type_tail() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_tail_t>();
    }
};

template<typename Head>
struct BAT<Head, v2_void_t> {

    typedef v2_void_t Tail;
    typedef Head v2_head_t;
    typedef Tail v2_tail_t;
    typedef typename v2_head_t::type_t head_t;
    typedef typename v2_tail_t::type_t tail_t;
    typedef typename ColumnDescriptorContainerType<v2_head_t>::container_t coldesc_head_container_t;
    typedef typename ColumnDescriptorContainerType<v2_tail_t>::container_t coldesc_tail_container_t;
    typedef ColumnDescriptor<v2_head_t, coldesc_head_container_t> coldesc_head_t;
    typedef ColumnDescriptor<v2_tail_t, coldesc_tail_container_t> coldesc_tail_t;

    coldesc_head_t head;
    coldesc_tail_t tail;

    BAT()
            : head(), tail() {
    }

    BAT(coldesc_head_t& head, coldesc_tail_t& tail)
            : head(head), tail(tail) {
    }

    BAT(coldesc_head_t&& head, coldesc_tail_t&& tail)
            : head(std::forward<coldesc_head_t>(head)), tail(std::forward<coldesc_tail_t>(tail)) {
    }

    virtual ~BAT() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BATIterator<Head, Tail>* begin() = 0;

    /** append an item */
    virtual void append(std::pair<head_t, tail_t>& p) = 0;
    virtual void append(std::pair<head_t, tail_t>&& p) = 0;
    virtual void append(head_t & t) = 0;
    virtual void append(head_t && t) = 0;

    virtual BAT<Tail, Head>* reverse() = 0;

    virtual BAT<Head, Head>* mirror_head() = 0;

    virtual BAT<Tail, Tail>* mirror_tail() = 0;

    /** size of column, obtained through the iterator */
    virtual unsigned size() = 0;

    /** Compute the actual memory consumption of the BAT */
    virtual size_t consumption() = 0;

    /** Compute the projected memory consumption of the BAT -- especially for AN encoded BATs */
    virtual size_t consumptionProjected() = 0;

    virtual boost::typeindex::type_index type_head() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_head_t>();
    }

    virtual boost::typeindex::type_index type_tail() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_tail_t>();
    }
};

template<typename Tail>
struct BAT<v2_void_t, Tail> {

    typedef v2_void_t Head;
    typedef Head v2_head_t;
    typedef Tail v2_tail_t;
    typedef typename v2_head_t::type_t head_t;
    typedef typename v2_tail_t::type_t tail_t;
    typedef typename ColumnDescriptorContainerType<v2_head_t>::container_t coldesc_head_container_t;
    typedef typename ColumnDescriptorContainerType<v2_tail_t>::container_t coldesc_tail_container_t;
    typedef ColumnDescriptor<v2_head_t, coldesc_head_container_t> coldesc_head_t;
    typedef ColumnDescriptor<v2_tail_t, coldesc_tail_container_t> coldesc_tail_t;

    coldesc_head_t head;
    coldesc_tail_t tail;

    BAT()
            : head(), tail() {
    }

    BAT(coldesc_head_t& head, coldesc_tail_t& tail)
            : head(head), tail(tail) {
    }

    BAT(coldesc_head_t&& head, coldesc_tail_t&& tail)
            : head(std::forward<coldesc_head_t>(head)), tail(std::forward<coldesc_tail_t>(tail)) {
    }

    virtual ~BAT() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BATIterator<Head, Tail>* begin() = 0;

    /** append an item */
    virtual void append(std::pair<head_t, tail_t>& p) = 0;
    virtual void append(std::pair<head_t, tail_t>&& p) = 0;
    virtual void append(tail_t & t) = 0;
    virtual void append(tail_t && t) = 0;

    virtual BAT<Tail, Head>* reverse() = 0;

    virtual BAT<Head, Head>* mirror_head() = 0;

    virtual BAT<Tail, Tail>* mirror_tail() = 0;

    /** size of column, obtained through the iterator */
    virtual unsigned size() = 0;

    /** Compute the actual memory consumption of the BAT */
    virtual size_t consumption() = 0;

    /** Compute the projected memory consumption of the BAT -- especially for AN encoded BATs */
    virtual size_t consumptionProjected() = 0;

    virtual boost::typeindex::type_index type_head() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_head_t>();
    }

    virtual boost::typeindex::type_index type_tail() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_tail_t>();
    }
};

#endif


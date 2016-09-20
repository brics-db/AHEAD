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


/***
 * @author Benjamin Schlegel
 */
#ifndef BAT_H
#define BAT_H

#include <utility>

#include <boost/type_index.hpp>

#include <ColumnStore.h>
#include <column_storage/BatIterator.h>

using namespace std;

template<typename Type, typename Container>
class ColumnDescriptor {
public:
    typedef Type v2_type_t;
    typedef Container container_t;
    typedef typename Type::type_t type_t;

    Container *container;

    ColumnDescriptor() : container(new Container) {
    }

    ColumnDescriptor(Container* heap) : container(heap) {
    }

    ColumnDescriptor(ColumnDescriptor& cd) : container(cd.container) {
    }

    ColumnDescriptor(ColumnDescriptor&& cd) : container(cd.container) {
    }

    virtual ~ColumnDescriptor() {
        if (container) {
            delete container;
        }
    }
};

template<>
class ColumnDescriptor<v2_void_t, void> {
public:
    typedef v2_void_t v2_type_t;
    typedef v2_void_t::type_t type_t;

    oid_t seqbase;

    ColumnDescriptor() : seqbase(0) {
    }

    ColumnDescriptor(oid_t seqbase) : seqbase(seqbase) {
    }

    virtual ~ColumnDescriptor() {
    }
};

template<typename Head, typename Tail>
class Bat {
public:
    typedef Head v2_head_t;
    typedef Tail v2_tail_t;
    typedef typename Head::type_t head_t;
    typedef typename Tail::type_t tail_t;

    virtual ~Bat() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BatIterator<Head, Tail>* begin() = 0;

    /** append an item */
    virtual void append(pair<head_t, tail_t>& p) = 0;
    virtual void append(pair<head_t, tail_t>&& p) = 0;

    virtual Bat<Tail, Head>* reverse() = 0;

    virtual Bat<Head, Head>* mirror_head() = 0;

    virtual Bat<Tail, Tail>* mirror_tail() = 0;

    /** size of column, obtained through the iterator */
    virtual unsigned size() = 0;

    /** Compute the actual memory consumption of the BAT */
    virtual size_t consumption() = 0;

    virtual boost::typeindex::type_index type_head() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_head_t>();
    }

    virtual boost::typeindex::type_index type_tail() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<v2_tail_t>();
    }
};

#endif


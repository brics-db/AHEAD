// Copyright (c) 2016 Till Kolditz
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
 * File:   ColumnDescriptor.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 9. Dezember 2016, 12:27
 */

#ifndef COLUMNDESCRIPTOR_HPP
#define COLUMNDESCRIPTOR_HPP

#include <cstdint>
#include <vector>
#include <atomic>
#include <utility>
#include <ColumnStore.h>

template<typename V2Type>
struct ColumnDescriptorContainerType {

    typedef typename V2Type::type_t type_t;
    typedef std::vector<type_t> container_t;
};

template<>
struct ColumnDescriptorContainerType<v2_void_t> {

    typedef void container_t;
};

template<typename V2Type, typename Container>
class ColumnDescriptor {

    atomic<ssize_t> *refcount;

public:
    typedef V2Type v2_type_t;
    typedef Container container_t;
    typedef typename V2Type::type_t type_t;
    typedef ColumnMetaData metadata_t;

    std::shared_ptr<container_t> container;

    metadata_t metaData;

    ColumnDescriptor () : refcount (new atomic<ssize_t>), container (new Container), metaData () {
        *refcount = 1;
    }

    /*
    ColumnDescriptor(std::shared_ptr<container_t> & heap) : refcount(new atomic<ssize_t>), container(heap), metaData()
    {
     *refcount = 1;
    }
     */

    ColumnDescriptor (metadata_t & metaData) : refcount (new atomic<ssize_t>), container (new Container), metaData (metaData) {
        *refcount = 1;
    }

    ColumnDescriptor (ColumnDescriptor& cd) : refcount (cd.refcount), container (cd.container), metaData (cd.metaData) {
        ++*refcount;
    }

    ColumnDescriptor (ColumnDescriptor&& cd) : refcount (cd.refcount), container (cd.container), metaData (cd.metaData) {
        ++*refcount;
    }

    virtual
    ~ColumnDescriptor () {
    }

    ColumnDescriptor& operator= (const ColumnDescriptor &copy) {
        this->~ColumnDescriptor();
        new (this) ColumnDescriptor(copy);
        return *this;
    }
};

template<>
class ColumnDescriptor<v2_void_t, void> {

public:
    typedef v2_void_t v2_type_t;
    typedef v2_void_t::type_t type_t;
    typedef ColumnMetaData metadata_t;

    metadata_t metaData;

    ColumnDescriptor () : metaData () {
        static_assert(std::is_base_of<v2_base_t, v2_void_t>::value, "v2_void_t is no derived from v2_base_t, but this class requires this!");
    }

    ColumnDescriptor (oid_t seqbase) : metaData (seqbase) {
        static_assert(std::is_base_of<v2_base_t, v2_void_t>::value, "v2_void_t is no derived from v2_base_t, but this class requires this!");
    }

    ColumnDescriptor (metadata_t & metaData) : metaData (metaData) {
    }

    ColumnDescriptor (ColumnDescriptor& cd) : metaData (cd.metaData) {
    }

    ColumnDescriptor (ColumnDescriptor&& cd) : metaData (cd.metaData) {
    }

    virtual
    ~ColumnDescriptor () {
    }

    ColumnDescriptor& operator= (ColumnDescriptor & copy) {
        this->~ColumnDescriptor();
        new (this) ColumnDescriptor(copy);
        return *this;
    }
};

template<typename V2Type>
class ColumnDescriptor<V2Type, void> {

public:
    typedef V2Type v2_type_t;
    typedef typename V2Type::type_t type_t;
    typedef ColumnMetaData metadata_t;

    metadata_t metaData;

    ColumnDescriptor () : metaData () {
    }

    ColumnDescriptor (metadata_t & metaData) : metaData (metaData) {
    }

    ColumnDescriptor (ColumnDescriptor& cd) : metaData (cd.metaData) {
    }

    ColumnDescriptor (ColumnDescriptor&& cd) : metaData (cd.metaData) {
    }

    virtual
    ~ColumnDescriptor () {
    }

    ColumnDescriptor& operator= (const ColumnDescriptor &copy) {
        this->~ColumnDescriptor();
        new (this) ColumnDescriptor(copy);
        return *this;
    }
};

#endif /* COLUMNDESCRIPTOR_HPP */

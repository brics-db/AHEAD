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
#ifndef TEMPBATITERATOR_H
#define TEMPBATITERATOR_H

#include <vector>

#include <ColumnStore.h>
#include <column_storage/BatIterator.h>
#include <column_storage/TempBat.h>

template<typename Head, typename Tail>
class TempBATIterator : public BATIterator<Head, Tail> {

private:
    using head_t = typename BAT<Head, Tail>::head_t;
    using tail_t = typename BAT<Head, Tail>::tail_t;
    using coldesc_head_t = typename BAT<Head, Tail>::coldesc_head_t;
    using coldesc_tail_t = typename BAT<Head, Tail>::coldesc_tail_t;
    typedef typename coldesc_head_t::container_t container_head_t;
    typedef typename coldesc_tail_t::container_t container_tail_t;
    typedef typename container_head_t::iterator iterator_head_t;
    typedef typename container_tail_t::iterator iterator_tail_t;

    std::shared_ptr<container_head_t> cHead;
    std::shared_ptr<container_tail_t> cTail;
    iterator_head_t iterHead;
    iterator_tail_t iterTail;

public:
    typedef TempBATIterator<Head, Tail> self_t;

    TempBATIterator (coldesc_head_t& head, coldesc_tail_t & tail) : cHead (head.container), cTail (tail.container), iterHead (cHead->begin ()), iterTail (cTail->begin ()) {
    }

    TempBATIterator (const TempBATIterator<Head, Tail> & iter) : cHead (iter.cHead), cTail (iter.cTail), iterHead (iter.iterHead), iterTail (iter.iterTail) {
    }

    virtual
    ~TempBATIterator () {
    }

    TempBATIterator& operator= (const TempBATIterator & copy) {
        new (this) TempBATIterator(copy);
        return *this;
    }

    virtual void
    next () override {
        iterHead++;
        iterTail++;
    }

    virtual TempBATIterator& operator++ () override {
        next();
        return *this;
    }

    virtual TempBATIterator& operator+= (oid_t i)override {
        std::advance(iterHead, i);
        std::advance(iterTail, i);
        return *this;
    }

    virtual void
    position (oid_t index) override {
        iterHead = cHead->begin();
        iterTail = cTail->begin();
        std::advance(iterHead, index);
        std::advance(iterTail, index);
    }

    virtual bool
    hasNext () override {
        return iterHead != cHead->end();
    }

    virtual head_t
    head () override {
        return *iterHead;
    }

    virtual tail_t
    tail () override {
        return *iterTail;
    }

    virtual size_t
    size () override {
        return cHead->size();
    }

    virtual size_t
    consumption () override {
        return cHead->capacity() * sizeof (head_t) + cTail->capacity() * sizeof (tail_t);
    }
};

template<>
class TempBATIterator<v2_void_t, v2_void_t> : public BATIterator<v2_void_t, v2_void_t> {

private:
    typedef v2_void_t Head, Tail;
    using head_t = typename BAT<Head, Tail>::head_t;
    using tail_t = typename BAT<Head, Tail>::tail_t;
    using coldesc_head_t = typename BAT<Head, Tail>::coldesc_head_t;
    using coldesc_tail_t = typename BAT<Head, Tail>::coldesc_tail_t;

    oid_t seqbase_head;
    oid_t position_head;
    oid_t seqbase_tail;
    oid_t position_tail;
    oid_t count;
    oid_t pos;

public:
    typedef TempBATIterator<v2_void_t, v2_void_t> self_t;

    TempBATIterator (coldesc_head_t & head, coldesc_tail_t & tail, oid_t count) : seqbase_head (head.metaData.seqbase), position_head (seqbase_head), seqbase_tail (tail.metaData.seqbase), position_tail (seqbase_tail), count (count), pos (0) {
    }

    TempBATIterator (const TempBATIterator<v2_void_t, v2_void_t> & iter) : seqbase_head (iter.seqbase_head), position_head (iter.position_head), seqbase_tail (iter.seqbase_tail), position_tail (iter.position_tail), count (iter.count), pos (iter.pos) {
    }

    virtual
    ~TempBATIterator () {
    }

    TempBATIterator& operator= (const TempBATIterator & copy) {
        new (this) TempBATIterator(copy);
        return *this;
    }

    virtual void
    next () override {
        ++position_head;
        ++position_tail;
        ++pos;
    }

    virtual TempBATIterator& operator++ () override {
        next();
        return *this;
    }

    virtual TempBATIterator& operator+= (oid_t i)override {
        position_head += i;
        position_tail += i;
        pos += i;
        return *this;
    }

    virtual void
    position (oid_t index) override {
        position_head = seqbase_head + index;
        position_tail = seqbase_tail + index;
    }

    virtual bool
    hasNext () override {
        return pos < count;
    }

    virtual head_t
    head () override {
        return position_head;
    }

    virtual tail_t
    tail () override {
        return position_tail;
    }

    virtual size_t
    size () override {
        return 0;
    }

    virtual size_t
    consumption () override {
        return 0;
    }
};

template<typename Head>
class TempBATIterator<Head, v2_void_t> : public BATIterator<Head, v2_void_t> {

private:
    typedef v2_void_t Tail;
    using head_t = typename BAT<Head, Tail>::head_t;
    using tail_t = typename BAT<Head, Tail>::tail_t;
    using coldesc_head_t = typename BAT<Head, Tail>::coldesc_head_t;
    using coldesc_tail_t = typename BAT<Head, Tail>::coldesc_tail_t;
    typedef typename coldesc_head_t::container_t container_head_t;
    typedef typename container_head_t::iterator iterator_head_t;

    std::shared_ptr<container_head_t> cHead;
    iterator_head_t iterHead;
    oid_t seqbase;
    oid_t position_tail;

public:
    typedef TempBATIterator<Head, v2_void_t> self_t;

    TempBATIterator (coldesc_head_t& head, coldesc_tail_t & tail) : cHead (head.container), iterHead (cHead->begin ()), seqbase (tail.metaData.seqbase), position_tail (seqbase) {
    }

    TempBATIterator (const TempBATIterator<Head, v2_void_t> & iter) : cHead (iter.cHead), iterHead (iter.iterHead), seqbase (iter.seqbase), position_tail (iter.position_tail) {
    }

    virtual
    ~TempBATIterator () {
    }

    TempBATIterator& operator= (const TempBATIterator & copy) {
        new (this) TempBATIterator(copy);
        return *this;
    }

    virtual void
    next () override {
        std::advance(iterHead, 1);
        ++position_tail;
    }

    virtual TempBATIterator& operator++ () override {
        next();
        return *this;
    }

    virtual TempBATIterator& operator+= (oid_t i)override {
        std::advance(iterHead, 1);
        position_tail += i;
        return *this;
    }

    virtual void
    position (oid_t index) override {
        iterHead = cHead->begin();
        std::advance(iterHead, index);
        position_tail = seqbase + index;
    }

    virtual bool
    hasNext () override {
        return iterHead != cHead->end();
    }

    virtual head_t
    head () override {
        return *iterHead;
    }

    virtual tail_t
    tail () override {
        return position_tail;
    }

    virtual size_t
    size () override {
        return cHead->size();
    }

    virtual size_t
    consumption () override {
        return cHead->capacity() * sizeof (head_t);
    }
};

template<typename Tail>
class TempBATIterator<v2_void_t, Tail> : public BATIterator<v2_void_t, Tail> {

private:
    typedef typename v2_void_t::type_t head_t;
    typedef typename Tail::type_t tail_t;
    typedef typename TempBAT<v2_void_t, Tail>::coldesc_tail_t::container_t container_tail_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_head_t;
    typedef ColumnDescriptor<Tail, container_tail_t> coldesc_tail_t;
    typedef typename container_tail_t::iterator iterator_tail_t;

    std::shared_ptr<container_tail_t> cTail;
    iterator_tail_t iterTail;
    oid_t seqbase;
    oid_t position_head;

public:
    typedef TempBATIterator<v2_void_t, Tail> self_t;

    TempBATIterator (coldesc_head_t& head, coldesc_tail_t & tail) : cTail (tail.container), iterTail (cTail->begin ()), seqbase (head.metaData.seqbase), position_head (seqbase) {
    }

    TempBATIterator (const TempBATIterator<v2_void_t, Tail> & iter) : cTail (iter.cTail), iterTail (iter.iterTail), seqbase (iter.seqbase), position_head (iter.position_head) {
    }

    virtual
    ~TempBATIterator () {
    }

    TempBATIterator& operator= (const TempBATIterator & copy) {
        new (this) TempBATIterator(copy);
        return *this;
    }

    virtual void
    next () override {
        iterTail++;
        ++position_head;
    }

    virtual TempBATIterator& operator++ () override {
        next();
        return *this;
    }

    virtual TempBATIterator& operator+= (oid_t i)override {
        position_head += i;
        std::advance(iterTail, 1);
        return *this;
    }

    virtual void
    position (oid_t index) override {
        iterTail = cTail->begin();
        std::advance(iterTail, index);
        position_head = seqbase + index;
    }

    virtual bool
    hasNext () override {
        return iterTail != cTail->end();
    }

    virtual head_t
    head () override {
        return position_head;
    }

    virtual tail_t
    tail () override {
        return *iterTail;
    }

    virtual size_t
    size () override {
        return cTail->size();
    }

    virtual size_t
    consumption () override {
        return cTail->capacity() * sizeof (tail_t);
    }
};

#endif

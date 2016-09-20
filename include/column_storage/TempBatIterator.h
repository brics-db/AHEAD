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
#ifndef TEMPBATITERATOR_H
#define TEMPBATITERATOR_H

#include <vector>

#include "ColumnStore.h"
#include "column_storage/BatIterator.h"
#include "TempBat.h"

using namespace std;

template<typename Head, typename Tail>
class TempBatIterator : public BatIterator<Head, Tail> {
private:
    typedef typename Head::type_t head_t;
    typedef typename Tail::type_t tail_t;
    typedef typename TempBat<Head, Tail>::coldesc_head_t::container_t container_head_t;
    typedef typename TempBat<Head, Tail>::coldesc_tail_t::container_t container_tail_t;
    typedef ColumnDescriptor<Head, container_head_t> coldesc_head_t;
    typedef ColumnDescriptor<Tail, container_tail_t> coldesc_tail_t;
    typedef typename container_head_t::iterator iterator_head_t;
    typedef typename container_tail_t::iterator iterator_tail_t;

    container_head_t* cHead;
    container_tail_t* cTail;
    iterator_head_t iterHead;
    iterator_tail_t iterTail;

public:

    TempBatIterator(coldesc_head_t& head, coldesc_tail_t& tail) : cHead(head.container), cTail(tail.container) {
        iterHead = cHead->begin();
        iterTail = cTail->begin();
    }

    virtual ~TempBatIterator() {
    }

    virtual void next() override {
        iterHead++;
        iterTail++;
    }

    virtual TempBatIterator& operator++() override {
        next();
        return *this;
    }

    virtual void position(oid_t index) override {
        iterHead = cHead->begin();
        iterTail = cTail->begin();
        advance(iterHead, index);
        advance(iterTail, index);
    }

    virtual bool hasNext() override {
        return iterHead != cHead->end();
    }

    virtual head_t&& head() override {
        return move(*iterHead);
    }

    virtual tail_t&& tail() override {
        return move(*iterTail);
    }

    virtual size_t size() override {
        return cHead->size();
    }

    virtual size_t consumption() override {
        return cHead->capacity() * sizeof (head_t) + cTail->capacity() * sizeof (tail_t);
    }
};

template<>
class TempBatIterator<v2_void_t, v2_void_t> : public BatIterator<v2_void_t, v2_void_t> {
private:
    typedef typename v2_void_t::type_t head_t;
    typedef typename v2_void_t::type_t tail_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_t;

    oid_t seqbase_head;
    oid_t position_head;
    oid_t seqbase_tail;
    oid_t position_tail;

public:

    TempBatIterator(coldesc_t& head, coldesc_t& tail) : seqbase_head(head.seqbase), position_head(seqbase_head), seqbase_tail(tail.seqbase), position_tail(seqbase_tail) {
    }

    virtual ~TempBatIterator() {
    }

    virtual void next() override {
        ++position_head;
        ++position_tail;
    }

    virtual TempBatIterator& operator++() override {
        next();
        return *this;
    }

    virtual void position(oid_t index) override {
        position_head = seqbase_head + index;
        position_tail = seqbase_tail + index;
    }

    virtual bool hasNext() override {
        return true;
    }

    virtual head_t&& head() override {
        return move(position_head);
    }

    virtual tail_t&& tail() override {
        return move(position_tail);
    }

    virtual size_t size() override {
        return 0;
    }

    virtual size_t consumption() override {
        return 0;
    }
};

template<typename Head>
class TempBatIterator<Head, v2_void_t> : public BatIterator<Head, v2_void_t> {
private:
    typedef typename Head::type_t head_t;
    typedef typename v2_void_t::type_t tail_t;
    typedef typename TempBat<Head, v2_void_t>::coldesc_head_t::container_t container_head_t;
    typedef ColumnDescriptor<Head, container_head_t> coldesc_head_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_tail_t;
    typedef typename container_head_t::iterator iterator_head_t;

    container_head_t* cHead;
    iterator_head_t iterHead;
    oid_t seqbase;
    oid_t position_tail;

public:

    TempBatIterator(coldesc_head_t& head, coldesc_tail_t& tail) : cHead(head.container), iterHead(cHead->begin()), seqbase(tail.seqbase), position_tail(seqbase) {
    }

    virtual ~TempBatIterator() {
    }

    virtual void next() override {
        iterHead++;
        ++position_tail;
    }

    virtual TempBatIterator& operator++() override {
        next();
        return *this;
    }

    virtual void position(oid_t index) override {
        iterHead = cHead->begin();
        advance(iterHead, index);
        position_tail = seqbase + index;
    }

    virtual bool hasNext() override {
        return iterHead != cHead->end();
    }

    virtual head_t&& head() override {
        return move(*iterHead);
    }

    virtual tail_t&& tail() override {
        return move(position_tail);
    }

    virtual size_t size() override {
        return cHead->size();
    }

    virtual size_t consumption() override {
        return cHead->capacity() * sizeof (head_t);
    }
};

template<typename Tail>
class TempBatIterator<v2_void_t, Tail> : public BatIterator<v2_void_t, Tail> {
private:
    typedef typename v2_void_t::type_t head_t;
    typedef typename Tail::type_t tail_t;
    typedef typename TempBat<v2_void_t, Tail>::coldesc_tail_t::container_t container_tail_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_head_t;
    typedef ColumnDescriptor<Tail, container_tail_t> coldesc_tail_t;
    typedef typename container_tail_t::iterator iterator_tail_t;

    container_tail_t* cTail;
    iterator_tail_t iterTail;
    oid_t seqbase;
    oid_t position_head;

public:

    TempBatIterator(coldesc_head_t& head, coldesc_tail_t& tail) : cTail(tail.container), iterTail(cTail->begin()), seqbase(head.seqbase), position_head(seqbase) {
    }

    virtual ~TempBatIterator() {
    }

    virtual void next() override {
        iterTail++;
        ++position_head;
    }

    virtual TempBatIterator& operator++() override {
        next();
        return *this;
    }

    virtual void position(oid_t index) override {
        iterTail = cTail->begin();
        advance(iterTail, index);
        position_head = seqbase + index;
    }

    virtual bool hasNext() override {
        return iterTail != cTail->end();
    }

    virtual head_t&& head() override {
        return move(position_head);
    }

    virtual tail_t&& tail() override {
        return move(*iterTail);
    }

    virtual size_t size() override {
        return cTail->size();
    }

    virtual size_t consumption() override {
        return cTail->capacity() * sizeof (tail_t);
    }
};

#endif


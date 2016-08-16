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

template<class Head, class Tail>
class TempBatIterator : public BatIterator<Head, Tail> {
private:
    typedef typename Head::type_t head_t;
    typedef typename Tail::type_t tail_t;
    typedef typename TempBat<Head, Tail>::container_t container_t;
    typedef typename container_t::iterator iterator_t;

    container_t* mVector;
    iterator_t iter;

public:

    TempBatIterator(vector<pair<head_t, tail_t> > * _v) : mVector(_v) {
        iter = mVector->begin();
    }

    virtual ~TempBatIterator() {
    }

    virtual pair<head_t, tail_t>&& next() override {
        return move(*(iter++));
    }

    virtual pair<head_t, tail_t>&& get(size_t index) override {
        iter = mVector->begin();
        advance(iter, index);
        return move(next());
    }

    virtual bool hasNext() override {
        return iter != mVector->end();
    }

    virtual size_t size() override {
        return mVector->size();
    }

    virtual size_t consumption() override {
        return mVector->capacity() * sizeof (typename container_t::value_type);
    }
};

#endif


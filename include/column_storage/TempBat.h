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
#ifndef TEMPBAT_H
#define TEMPBAT_H

#include <utility>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBatIterator.h>

template<class Head, class Tail>
class TempBat : public Bat<Head, Tail> {
public:
    typedef typename Head::type_t head_t;
    typedef typename Tail::type_t tail_t;
    typedef vector<std::pair<head_t, tail_t> > container_t;

private:
    container_t items;

public:

    /** default constructor */
    TempBat() {
    }

    virtual ~TempBat() {
    }

    /** constructor for n elements */
    TempBat(size_t n) {
        items.reserve(n);
    }

    /** returns an iterator pointing at the start of the column */
    virtual BatIterator<Head, Tail> * begin() override {
        return new TempBatIterator<Head, Tail>(&items);
    }

    /** append an item */
    virtual void append(pair<head_t, tail_t>& p) override {
        items.emplace_back(std::move(p));
    }

    /** append an item */
    virtual void append(pair<head_t, tail_t>&& p) override {
        items.emplace_back(std::move(p));
    }

    virtual unsigned size() override {
        return items.size();
    }

    virtual size_t consumption() override {
        return size() * sizeof (tail_t);
    }

};

#endif

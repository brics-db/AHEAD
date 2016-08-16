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

#include <column_storage/BatIterator.h>

using namespace std;

template<typename Head, typename Tail>
class Bat {
public:
    typedef typename Head::type_t head_t;
    typedef typename Tail::type_t tail_t;

    virtual ~Bat() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BatIterator<Head, Tail>* begin() = 0;

    /** append an item */
    virtual void append(pair<head_t, tail_t>& p) = 0;
    virtual void append(pair<head_t, tail_t>&& p) = 0;

    /** size of column, obtained through the iterator */
    virtual unsigned size() = 0;

    /** Compute the actual memory consumption of the BAT */
    virtual size_t consumption() = 0;

    virtual boost::typeindex::type_index type_head() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<head_t>();
    }

    virtual boost::typeindex::type_index type_tail() const BOOST_NOEXCEPT {
        return boost::typeindex::type_id<tail_t>();
    }
};

#endif


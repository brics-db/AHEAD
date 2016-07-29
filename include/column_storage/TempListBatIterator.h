// Copyright (c) 2010 Burkhard Rammé
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
 * @author Burkhard Rammé
 */
#ifndef TEMPLISTBATITERATOR_H
#define TEMPLISTBATITERATOR_H

#include <list>

#include "ColumnStore.h"
#include "column_storage/BatIterator.h"

using namespace std;

template<class Head, class Tail>
class TempListBatIterator : public BatIterator<Head, Tail> {
private:
    list<pair<Head, Tail> > * mList;
    typename list<pair<Head, Tail> >::iterator iter;
public:

    TempListBatIterator(list<pair<Head, Tail> > * _l) : mList(_l) {
        iter = mList->begin();
    };

    virtual pair<Head, Tail> next() {
        return *(iter++);
    };

    virtual pair<Head, Tail> get(unsigned index) {
        iter = mList->begin();
        advance(iter, index);
        return next();
    };

    virtual bool hasNext() {
        return iter != mList->end();
    };

    virtual unsigned size() {
        return mList->size();
    };
};

#endif //TEMPLISTBATITERATOR_H

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
#ifndef TEMPFIFOBAT_H
#define TEMPFIFOBAT_H

#include "column_storage/Bat.h"
#include <queue>
//#include "column_storage/TempFIFOBatIterator.h"

template<class Head, class Tail>
class TempFIFOBat : public Bat<Head,Tail> {
private:
    std::queue<std::pair<Head,Tail> > items;
public:
	/** default constructor */
	TempFIFOBat() {};

	/** returns an iterator pointing at the start of the column */
	virtual BatIterator<Head,Tail> * begin() {/*return new TempFIFOBatIterator<Head,Tail>(&items);*/ return 0; };

	/** append an item */
	virtual void append(pair<Head,Tail> p) {items.push(p); };
	virtual int size() {return items.size(); }
//	virtual std::pair<Head,Tail> pop() { std::cout<<"TempFIFOBat::pop() called - is just a dummy for now!"; }
};

#endif //TEMPFIFOBAT_H

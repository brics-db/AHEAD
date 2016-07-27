// Copyleft 2010 Hannes Rauhe


/***
* @author Hannes Rauhe
*/
#ifndef TESTBATITERATOR_H
#define TESTBATITERATOR_H

#include "column_storage/BatIterator.h"
#include <vector>

using namespace std;

template<class Head, class Tail>
class TestBatIterator : public BatIterator<Head,Tail> {
private:
	vector<pair<Head,Tail> > * mVector;
        typename vector<pair<Head,Tail> >::iterator iter;
public:
	TestBatIterator(vector<pair<Head,Tail> > * _v) : mVector(_v) {iter = mVector->begin();};
	virtual pair<Head,Tail> next() {return *(iter++);};
	virtual pair<Head,Tail> get(unsigned index) {
		iter = mVector->begin();
		advance(iter,index);
		return next();
	};
	virtual bool hasNext() {return iter != mVector->end();};
	virtual unsigned size() {return mVector->size();};
};

#endif


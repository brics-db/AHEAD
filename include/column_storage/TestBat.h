// Copyleft 2010 Hannes Rauhe


/***
 * @author Hannes Rauhe
 */
#ifndef TESTBAT_H
#define TESTBAT_H

#include "ColumnStore.h"
#include "column_storage/Bat.h"
#include "column_storage/TestBatIterator.h"

template<class Head, class Tail>
class TestBat : public Bat<Head, Tail> {
private:
    vector<std::pair<Head, Tail> > items;
public:

    /** default constructor */
    TestBat(Head start = 0, Head size = 0) {
        if (size) {
            for (Head i = start; i <= size; i++) {
                append(make_pair((Head) i, Tail(i + 1000)));
            }
        }
    };

    /** returns an iterator pointing at the start of the column */
    virtual BatIterator<Head, Tail> * begin() {
        return new TestBatIterator<Head, Tail>(&items);
    };

    /** append an item */
    virtual void append(pair<Head, Tail> p) {
        items.push_back(p);
    };

    virtual int size() {
        return items.size();
    }
};

#endif

/***
 * @author Hannes Rauhe
 */
#ifndef PARTEDBAT_H
#define PARTEDBAT_H

#include <set>

#include "ColumnStore.h"
#include "column_storage/Bat.h"
#include "column_storage/TempBatIterator.h"
#include "column_storage/PartedBatIterator.h"

template<class Head, class Tail>
class PartedBat : public Bat<Head, Tail> {
private:
    typedef std::list<Head> h_list_t;
    typedef typename h_list_t::iterator h_it_t;
    typedef std::list<Tail> t_list_t;
    typedef typename t_list_t::iterator t_it_t;
    h_list_t heads;
    t_list_t tails;
public:

    PartedBat() {
    }

    ~PartedBat() {
        heads.clear();
        tails.clear();
    }

    virtual BatIterator<Head, Tail> * begin() {
        return new PartedBatIterator<Head, Tail>(heads.begin(), tails.begin(), heads.end(), tails.end());
    }

    BatIterator<Head, Tail> * begin(unsigned start, unsigned end) {
        h_it_t his = heads.begin();
        t_it_t tis = tails.begin();
        advance(his, start);
        advance(tis, start);
        h_it_t hie = his;
        t_it_t tie = tis;
        advance(hie, end - start);
        advance(tie, end - start);
        return new PartedBatIterator<Head, Tail>(his, tis, hie, tie);
    }

    virtual void append(pair<Head, Tail> p) {
        heads.push_back(p.first);
        tails.push_back(p.second);
    }

    virtual int size() {
        return heads.size();
    }
};

#endif

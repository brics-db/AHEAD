/***
 * @author Hannes Rauhe
 */
#ifndef PARTEDBATITERATOR_H
#define PARTEDBATITERATOR_H

#include <list>

#include "ColumnStore.h"
#include "column_storage/BatIterator.h"

template<class Head, class Tail>
class PartedBatIterator : public BatIterator<Head, Tail> {
private:
    typedef std::list<Head> h_list_t;
    typedef typename h_list_t::iterator h_it_t;
    typedef std::list<Tail> t_list_t;
    typedef typename t_list_t::iterator t_it_t;
    const h_it_t h_it_begin;
    const h_it_t h_it_end;
    const t_it_t t_it_begin;
    const t_it_t t_it_end;
    h_it_t h_it;
    t_it_t t_it;
    unsigned index;
public:

    PartedBatIterator(const h_it_t &_h_it_begin, const t_it_t &_t_it_begin, const h_it_t &_h_it_end, const t_it_t &_t_it_end)
    : h_it_begin(_h_it_begin), h_it_end(_h_it_end), t_it_begin(_t_it_begin), t_it_end(_t_it_end), index(0) {
        h_it = h_it_begin;
        t_it = t_it_begin;
    }

    virtual pair<Head, Tail> next() {
        std::pair<Head, Tail> p(*(h_it), *(t_it));
        ++h_it;
        ++t_it;
        ++index;
        return p;
    }

    virtual std::pair<Head, Tail> get(unsigned _index) {
        h_it = h_it_begin;
        t_it = t_it_begin;
        advance(h_it, _index);
        advance(t_it, _index);
        index = _index;
        return next();
    }

    virtual bool hasNext() {
        return (h_it != h_it_end);
    }

    virtual unsigned size() {
        return distance(h_it_begin, h_it_end);
    }
};

#endif


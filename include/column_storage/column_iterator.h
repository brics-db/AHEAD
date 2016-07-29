/**
 * @file column_iterator.h
 * @date Mar 7, 2011
 * @author Hannes Rauhe
 *
 */

#ifndef COLUMN_ITERATOR_H_
#define COLUMN_ITERATOR_H_

#include <vector>
#include <list>
#include <iostream>
#include "boost/shared_ptr.hpp"

template<class T> class column_iterator;

template<class T>
inline bool operator==(const column_iterator<T>& i1, const column_iterator<T>& i2) {
    return i1.part_it == i2.part_it && i1.container_it == i2.container_it;
}

template<class T>
inline bool operator!=(const column_iterator<T>& i1, const column_iterator<T>& i2) {
    return !(i1 == i2);
}

/// basic class to iterate over a column
//TODO: idea: implement partition-wise iterating for the [] of the BAT-class

template<class T>
class column_iterator {
public:
    typedef std::vector<T> partition;
    typedef boost::shared_ptr<partition> partition_pointer;
    typedef std::vector<partition_pointer > part_container;

    typename part_container::const_iterator container_it;
    typename part_container::const_iterator container_it_end;
    typename partition::iterator part_it;
    unsigned part_count;

    column_iterator(typename part_container::iterator _container_it, typename part_container::iterator _container_it_end, typename partition::iterator _part_it) :
    container_it(_container_it),
    container_it_end(_container_it_end),
    part_it(_part_it),
    part_count(0) {
        while (container_it != container_it_end && part_it == (*container_it)->end()) {
            ++container_it;
            if (container_it != container_it_end) {
                part_it = (*container_it)->begin();
                part_count++;
            } else {
                break;
            }
        }
    }

    column_iterator& operator++() {
        ++part_it;
        while (part_it == (*container_it)->end()) {
            ++container_it;
            if (container_it != container_it_end) {
                part_it = (*container_it)->begin();
                part_count++;
            } else {
                break;
            }
        }
        return *this;
    }

    column_iterator operator++(int) {
        column_iterator n_it(container_it, container_it_end, part_it);
        return ++n_it;
    }

    T& operator*() {
        return *(this->part_it);
    }

    friend bool operator== <>(const column_iterator<T>& i1, const column_iterator<T>& i2);
    friend bool operator!= <>(const column_iterator<T>& i1, const column_iterator<T>& i2);
};


/**
 * const iterator - sorry for code-duplication
 */

template<class T> class const_column_iterator;

template<class T>
inline bool operator==(const const_column_iterator<T>& i1, const const_column_iterator<T>& i2) {
    return i1.part_it == i2.part_it && i1.container_it == i2.container_it;
}

template<class T>
inline bool operator!=(const const_column_iterator<T>& i1, const const_column_iterator<T>& i2) {
    return !(i1 == i2);
}

template<class T>
class const_column_iterator {
public:
    typedef std::vector<T> partition;
    typedef boost::shared_ptr<partition> partition_pointer;
    typedef std::vector<partition_pointer > part_container;

    typename part_container::const_iterator container_it;
    typename part_container::const_iterator container_it_end;
    typename partition::const_iterator part_it;
    unsigned part_count;

    const_column_iterator(typename part_container::const_iterator _container_it, typename part_container::const_iterator _container_it_end, typename partition::const_iterator _part_it) :
    container_it(_container_it),
    container_it_end(_container_it_end),
    part_it(_part_it),
    part_count(0) {
        while (container_it != container_it_end && part_it == (*container_it)->end()) {
            ++container_it;
            if (container_it != container_it_end) {
                part_it = (*container_it)->begin();
                part_count++;
            } else {
                break;
            }
        }
    }

    const_column_iterator& operator++() {
        ++part_it;
        while (part_it == (*container_it)->end()) {
            ++container_it;
            if (container_it != container_it_end) {
                part_it = (*container_it)->begin();
                part_count++;
            } else {
                break;
            }
        }
        return *this;
    }

    const_column_iterator operator++(int) {
        const_column_iterator n_it(container_it, container_it_end, part_it);
        return ++n_it;
    }

    const T& operator*() {
        return *(this->part_it);
    }


    friend bool operator== <>(const const_column_iterator<T>& i1, const const_column_iterator<T>& i2);
    friend bool operator!= <>(const const_column_iterator<T>& i1, const const_column_iterator<T>& i2);
};
#endif /* COLUMN_ITERATOR_H_ */

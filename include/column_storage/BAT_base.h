/**
 * @file BAT_base.h
 * @author Hannes Rauhe (hannes.rauhe@sap.com)
 * @version 1.0
 * @date Jan 4, 2011
 *
 * In this file all structures are implemented, that are necessary to work with (the new STL-like) BATs:
 * the BUN- and BAT-datastructures as well as the iterator for BATs and the basic c++-operators to work with these structures
 */

#ifndef BAT_BASE_H_
#define BAT_BASE_H_

#include <vector>
#include <list>
#include <iostream>

#include "boost/shared_ptr.hpp"

/**
 * BinaryUNits hold a head and a tail value - works almost exactly like std::pair
 */
template<class Head, class Tail>
struct BUN {
    Head head;
    Tail tail;

    BUN() : head(), tail() {
    }

    BUN(const Head& h, const Tail& t) : head(h), tail(t) {
    }

    BUN(const BUN& b) : head(b.head), tail(b.tail) {
    }
};

/// Operator that tests two BUNs on equality

template<class Head, class Tail>
inline bool operator==(const BUN<Head, Tail>& __x, const BUN<Head, Tail>& __y) {
    return __x.head == __y.head && __x.tail == __y.tail;
}

/// Operator that tests if two BUNs are unequal

template<class Head, class Tail>
inline bool operator!=(const BUN<Head, Tail>& __x, const BUN<Head, Tail>& __y) {
    return !(__x == __y);
}

/// Stream operator to prints BUNs with cout

template<class Head, class Tail>
std::ostream& operator<<(std::ostream &os, BUN<Head, Tail> &obj) {
    os << obj.head << "|" << obj.tail;
    return os;
}

/// basic class to iterate over a BAT (*-Operator is implemented in child-classes)
//TODO: idea: implement partition-wise iterating for the [] of the BAT-class

template<class Head, class Tail>
class BAT_base_iterator {
public:
    typedef std::vector<BUN<Head, Tail> > partition;
    typedef boost::shared_ptr<partition> partition_pointer;
    typedef std::vector<partition_pointer > part_container;

    typename part_container::iterator container_it;
    typename part_container::iterator container_it_end;
    typename partition::iterator part_it;

    BAT_base_iterator(typename part_container::iterator _container_it, typename part_container::iterator _container_it_end, typename partition::iterator _part_it) :
    container_it(_container_it),
    container_it_end(_container_it_end),
    part_it(_part_it) {
    }

    BAT_base_iterator& operator++() {
        ++part_it;
        if (part_it == (*container_it)->end()) {
            ++container_it;
            if (container_it != container_it_end) {
                part_it = (*container_it)->begin();
            }
        }
        return *this;
    }

    BAT_base_iterator operator++(int) {
        BAT_base_iterator n_it(container_it, container_it_end, part_it);
        return ++n_it;
    }

    template<Head, Tail>
    friend bool operator==(const BAT_base_iterator<Head, Tail>& i1, const BAT_base_iterator<Head, Tail>& i2);
    template<Head, Tail>
    friend bool operator!=(const BAT_base_iterator<Head, Tail>& i1, const BAT_base_iterator<Head, Tail>& i2);
};

template<class Head, class Tail>
inline bool operator==(const BAT_base_iterator<Head, Tail>& i1, const BAT_base_iterator<Head, Tail>& i2) {
    return i1.part_it == i2.part_it && i1.container_it == i2.container_it;
}

template<class Head, class Tail>
inline bool operator!=(const BAT_base_iterator<Head, Tail>& i1, const BAT_base_iterator<Head, Tail>& i2) {
    return !(i1 == i2);
}

template<class Head, class Tail>
class BAT_normal_iterator : public BAT_base_iterator<Head, Tail> {
public:
    typedef std::vector<BUN<Head, Tail> > partition;
    typedef boost::shared_ptr<partition> partition_pointer;
    typedef std::vector<partition_pointer > part_container;

    BAT_normal_iterator(typename part_container::iterator _container_it, typename part_container::iterator _container_it_end, typename partition::iterator _part_it) :
    BAT_base_iterator<Head, Tail>(_container_it, _container_it_end, _part_it) {
    }

    BUN<Head, Tail>& operator*() {
        return *(this->part_it);
    }

    Head& head() {
        return (this->part_it)->head;
    }

    Tail& tail() {
        return (this->part_it)->tail;
    }
};

template<class Head, class Tail>
class BAT_reverse_iterator : public BAT_base_iterator<Head, Tail> {
public:
    typedef std::vector<BUN<Head, Tail> > partition;
    typedef boost::shared_ptr<partition> partition_pointer;
    typedef std::vector<partition_pointer > part_container;

    BAT_reverse_iterator(typename part_container::iterator _container_it, typename part_container::iterator _container_it_end, typename partition::iterator _part_it) :
    BAT_base_iterator<Head, Tail>(_container_it, _container_it_end, _part_it) {
    }

    BUN<Tail, Head> operator*() {
        return BUN<Tail, Head>((*(this->part_it)).tail, (*(this->part_it)).head);
    }

    Tail& head() {
        return (this->part_it)->tail;
    }

    Head& tail() {
        return (this->part_it)->head;
    }
};

template<class Head, class Tail>
class BAT_mirror_iterator : public BAT_base_iterator<Head, Tail> {
public:
    typedef std::vector<BUN<Head, Tail> > partition;
    typedef boost::shared_ptr<partition> partition_pointer;
    typedef std::vector<partition_pointer > part_container;

    BAT_mirror_iterator(typename part_container::iterator _container_it, typename part_container::iterator _container_it_end, typename partition::iterator _part_it) :
    BAT_base_iterator<Head, Tail>(_container_it, _container_it_end, _part_it) {
    }

    BUN<Head, Head> operator*() {
        return BUN<Head, Head>(*(this->part_it).head, *(this->part_it).head);
    }

    Head& head() {
        return (this->part_it)->head;
    }

    Head& tail() {
        return (this->part_it)->head;
    }
};

/// The STL-like BAT_base class implements everything to work with BATs as data structures

/**
 * @section DESCRIPTION
 * @date Jan 4, 2011
 *
 *    This implementation can be used like a STL container. To do that yout only need the members that are named like members of STL containers. That means you can pretend a BAT is a vector of pairs.\n
 *    Inside this is not only a simple container, but a container of (pointers to) containers (in this case a vector of vectors of pairs).
 *    The outter vector is called container and the inner vectors are called partitions. The user of this class can safely ignore that and use it like a normal vector. It can be ignored that the elements are in different partitions.
 *    But it is also possible to access partitions as separate BATs. So you can work with the partitions of one BAT just like if they were a BAT itself. It is possible to let an operation process the whole BAT or just a partition of it.
 *    And of course - and this is the point - you can process the partitions of a BAT independently in parallel with the same operators you would use to process it at once.
 *
 *      - new implementation of the BAT-class without virtual functions and different behaviour (STL-like with iterator)
 *      - can handle partitioned BATs (for parallel processing)
 *      - each partition can be used like a BAT itself a partition can only be accessed as a BAT itself
 *
 *      - copys elements just like std::vector
 *      - whole partitions will be kept as smart pointers - you can use one partition in more than one BAT without copying
 *
 */
template<class Head, class Tail, class iterator_type = BAT_normal_iterator<Head, Tail> >
class BAT_base {
public:
    typedef BUN<Head, Tail> bun;
    typedef std::vector<bun> partition;
    typedef boost::shared_ptr<partition> partition_pointer;
    typedef std::vector<partition_pointer> part_container;
    //typedef const_BAT_base_iterator const_iterator;
    typedef typename part_container::iterator container_iterator;
    typedef typename part_container::const_iterator const_container_iterator;
    typedef iterator_type iterator;


protected:
    part_container content;

public:
    /**
     *
     * simple Vector functionality
     *
     **/

    /**
     * the default constructor pushes the pointer to a new partition
     */
    explicit BAT_base(const partition& p = partition()) {
        partition_pointer p_c(new partition(p));
        content.push_back(p_c);
    }

    BAT_base(part_container& _content) : content(_content) {
    }

    /// kind of copy-constructor - pushs the pointer to the partition given

    BAT_base(const partition_pointer& p) {
        content.push_back(p);
    }


    /// this constructor creates a BAT with a pre-filled partition (similar to std::vector)

    explicit BAT_base(size_t n, const bun& value = bun()) {
        partition_pointer p_c(new partition(n, value));
        content.push_back(p_c);
    }

    /// dtor not needed - default will do

    virtual ~BAT_base() {
    }

    iterator begin() {
        return iterator(content.begin(), content.end(), content.front()->begin());
    }

    iterator end() {
        return iterator(content.end(), content.end(), content.back()->end());
    }

    iterator begin() const {
        return iterator(content.begin(), content.end(), content.front()->begin());
    }

    iterator end() const {
        return iterator(content.end(), content.end(), content.back()->end());
    }

    bun& front() {
        return content.front()->front();
    }

    bun& back() {
        return content.back()->back();
    }

    ///alternative push back that accepts Head and Tail seperately

    void push_back(const Head& h, const Tail& t) {
        content.back()->push_back(bun(h, t));
    }

    void push_back(const bun& e) {
        content.back()->push_back(e);
    }

    /// push_back to a specified partition

    /**
     * @param part_n number of the partition, to add the BUN to (starting with 0)
     * @param e BUN that will be copied
     */
    void push_back(size_t part_n, const bun& e) {
        content[part_n]->push_back(e);
    }

    void pop_back() {
        content.back()->pop_back();
        if (content.back()->empty() && content.back() != content.front()) {
            content.pop_back();
        }
    }

    /**
     * @return true, if only one empty partition is in the container
     */
    bool empty() const {
        return content.front() == content.back() && content.front()->empty();
    }

    size_t size() const {
        size_t size = 0;

        for (const_container_iterator it = content.begin(); it != content.end(); ++it) {
            size += (*it)->size();
        }
        return size;
    }

    //TODO reimplement: won't give back the reversed/mirror pair (isn't using the right iterator)

    bun& operator[](unsigned n) {
        size_t size = 0;

        for (container_iterator it = content.begin(); it != content.end(); ++it) {
            size += (*it)->size();
            if (size > n) {
                return (*it)->at(n - (size - (*it)->size()));
            }
        }
        return content.back()->at(n);
    }

    /*
     *
     * Partition-specific methods
     *
     */

    /// push a partition with specified size (reserved) at the end of the container

    /**
     * @param part_size_reserve number of BUNs can carry without reallocating (default is 0)
     */
    void create_partition(size_t part_size_reserve = 0) {
        partition_pointer p(new partition());
        if (part_size_reserve > 0)
            p->reserve(part_size_reserve);

        content.push_back(p);
    }

    /// push the partitions of another BAT at the end of the container (concatenate)

    void push_back_partition(BAT_base<Head, Tail>& part) {
        container_iterator it = part.content.begin();
        if (empty()) {
            content[0] = *it;
        }
        for (; it != part.content.end(); ++it) {
            content.push_back(*it);
        }
    }

    /// get a partition specified by it's position

    /**
     * This is, what the whole class is about!\n
     * You can access a part of the BAT as BAT itself without copying anything, this allows you to easily process a BAT in parallel (with more than one thread)
     * This way a thread can process a BAT without knowing if it is only a part of a bigger BAT or the complete BAT
     *
     * @param part_n the number of the partition to access
     * @return the partition of this BAT specified by its number as BAT itself
     */
    BAT_base<Head, Tail> get_partition(size_t part_n) {
        return BAT_base(content[part_n]);
    }

    /// return the size of a partition

    size_t partition_count() const {
        return content->size();
    }

    /// return the size of a partition

    size_t size(size_t part_n) const {
        return content[part_n]->size();
    }

    /*
     *
     * essential operators
     *
     */

    ///gives back the BAT with reversed Head & Tail (withour copying)

    BAT_base<Head, Tail, BAT_reverse_iterator<Head, Tail> > reverse() {
        return BAT_base<Head, Tail, BAT_reverse_iterator<Head, Tail> >(content);
    }

    template<Head, Tail>
    friend std::ostream &operator<<(std::ostream &stream, BAT_base<Head, Tail> &obj);
};

template<class Head, class Tail>
std::ostream& operator<<(std::ostream &os, BAT_base<Head, Tail> &obj) {
    for (typename BAT_base<Head, Tail>::iterator it = obj.begin(); it != obj.end(); ++it) {
        os << (*it) << std::endl;
    }
    return os;
}
#endif /* BAT_BASE_H_ */

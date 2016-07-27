/**
 * @file column.h
 * @date Mar 4, 2011
 * @author Hannes Rauhe
 *
 */

#ifndef COLUMN_H_
#define COLUMN_H_
#include <vector>
#include <string>
#include <list>
#include <iostream>
#include "boost/shared_ptr.hpp"
#include "column_storage/column_iterator.h"


/// forward declaration for the ostream << operator
template<class T> class column;

/// the ostream << operator for printing
template<class T>
std::ostream& operator<<(std::ostream &os, const column<T> &obj)    {
    unsigned part_count=1;
    for(typename column<T>::const_iterator it=obj.begin();it!=obj.end();++it) {
        if(part_count<=it.part_count) {
            os<<"-- Partition "<<it.part_count<<std::endl;
            part_count=it.part_count+1;
        }
        os<<" "<<(*it)<<std::endl;
    }
    return os;
}

/// The STL-like column class
/**
 * @section DESCRIPTION
 * @date Jan 4, 2011
 *
 *
 */
template<class T>
class column {
public:
    typedef std::vector<T> partition;
    typedef boost::shared_ptr<partition> partition_pointer;
    typedef std::vector<partition_pointer> part_container;
    typedef typename part_container::iterator container_iterator;
    typedef typename part_container::const_iterator const_container_iterator;
    typedef column_iterator<T> iterator;
    typedef const_column_iterator<T> const_iterator;


protected:
    part_container content;
    bool first_partition_explicitly_empty; //what a hack :-(

public:
    /**
     *
     * simple Vector functionality
     *
     **/

    /**
     * the default constructor pushs the pointer to a new partition
     * pushs the pointer to the partition given or
     */
    explicit column(const partition& p = partition()) : first_partition_explicitly_empty(false) {
        //I don't want to check every time, if there's a partition in the container, so I push one, even if it's empty
        partition_pointer p_c(new partition(p));
        content.push_back(p_c);
    }

    column(part_container& _content) : content(_content), first_partition_explicitly_empty(false) {}

    /// kind of copy-constructor - pushs the pointer to the partition given
    column(const partition_pointer& p)  : first_partition_explicitly_empty(false) {
        content.push_back(p);
    }


    /// this constructor creates a column with a pre-filled partition (similar to std::vector)
    explicit column(size_t n, const T& value = T())  : first_partition_explicitly_empty(false) {
        partition_pointer p_c(new partition(n,value));
        content.push_back(p_c);
    }

    /// dtor not needed - default will do
    ~column() {
    }

    iterator begin() {
        return iterator(content.begin(),content.end(),content.front()->begin());
    }

    iterator end() {
        return iterator(content.end(),content.end(),content.back()->end());
    }

    const_iterator begin() const {
        return const_iterator(content.begin(),content.end(),content.front()->begin());
    }

    const_iterator end() const {
        return const_iterator(content.end(),content.end(),content.back()->end());
    }

    T& front() {
        return content.front()->front();
    }

    T& back() {
        return content.back()->back();
    }

    void push_back(const T& e) {
        content.back()->push_back(e);
    }

    /// push_back to a specified partition
    /**
     * @param part_n number of the partition, to add the element to (starting with 0)
     * @param e element, that will be copied
     */
    void push_back(size_t part_n, const T& e) {
        content[part_n]->push_back(e);
    }

    void pop_back() {
        content.back()->pop_back();
        if(content.back()->empty() && content.back()!=content.front()) {
            content.pop_back();
        }
    }

    /**
     * @return true, if only one empty partition is in the container
     */
    bool empty() const {
        return content.front()==content.back() && content.front()->empty();
    }

    size_t size() const {
        size_t size=0;

        for(const_container_iterator it=content.begin();it!=content.end();++it) {
            size+=(*it)->size();
        }
        return size;
    }

    void clear() {
        content.clear();
        partition_pointer p_c(new partition());
        content.push_back(p_c);
        first_partition_explicitly_empty=false;
    }

    T& operator[](unsigned n) {
        size_t size=0;

        for(container_iterator it=content.begin();it!=content.end();++it) {
            size+=(*it)->size();
            if(size>n) {
                return (*it)->at(n-(size-(*it)->size()));
            }
        }
        return content.back()->at(n);
    }

    const T& operator[](unsigned n) const {
        size_t size=0;

        for(const_container_iterator it=content.begin();it!=content.end();++it) {
            size+=(*it)->size();
            if(size>n) {
                return (*it)->at(n-(size-(*it)->size()));
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
     * @param part_size_reserve number of elements can carry without reallocating (default is 0)
     */
    void create_partition(size_t part_size_reserve=0) {
        partition_pointer p(new partition());
        if(part_size_reserve>0)
            p->reserve(part_size_reserve);

        content.push_back(p);
    }

    /// push the partitions of another column at the end of the container (concatenate)
    void push_back_partition(column<T>& part) {
        container_iterator it=part.content.begin();
        if(empty() && !first_partition_explicitly_empty) {
            content[0]=*it;
            ++it;
            first_partition_explicitly_empty=true;
        }
        for(;it!=part.content.end();++it) {
            content.push_back(*it);
        }
    }

    /**
     * get a partition specified by it's position
     *
     * This is, what the whole class is about!\n
     * You can access a part of the column as column itself without copying anything, this allows you to easily process a column in parallel (with more than one thread)
     * This way a thread can process a column without knowing if it is only a part of a bigger column or the complete column
     *
     * @param part_n the number of the partition to access
     * @return the partition of this column specified by its number as column itself
     */
    column<T> get_partition(size_t part_n) const {
        return column(content[part_n]);
    }

    /// return the size of a partition
    size_t partition_count() const {
        return content.size();
    }

    /// return the size of a partition
    size_t size(size_t part_n) const {
        return content[part_n]->size();
    }

    friend std::ostream &operator<< <>(std::ostream &stream, const column<T> &obj);
};

#endif /* COLUMN_H_ */

/***
 * @author Burkhard Rammé
 */

#ifndef TEMP_QUEUE_H
#define TEMP_QUEUE_H

#include <queue>

#include "ColumnStore.h"

using namespace std;

/**
 * A simple queue with pop() and push()
 */
template<class Head, class Tail>
class TempQueue {
public:
    queue<std::pair<Head, Tail> > *items;

    /** default constructor */
    TempQueue() {
        items = new queue<std::pair<Head, Tail> >();
    };

    TempQueue(const TempQueue& q) {
        items = new queue<std::pair<Head, Tail> >(*(q.items));
    };

    ~TempQueue() {
        /*std::cout << "TempQueue::dtor!" << std::endl;*/delete items;
    };

    virtual std::pair<Head, Tail> pop() {
        std::pair<Head, Tail> el = items->front();
        items->pop();
        return el;
    }

    virtual void push(std::pair<Head, Tail> el) {
        items->push(el);
    }

    virtual void append(std::pair<Head, Tail> el) {
        items->push(el);
    }

    virtual void append(Head h, Tail t) {
        items->push(make_pair(h, t));
    }

    virtual unsigned int size() {
        return items->size();
    }

    /**outputs the items to std::cout*/
    virtual void print() {
        queue<std::pair<Head, Tail> > tmp(*items); //copy
        while (!tmp.empty()) {
            std::pair<Head, Tail> p = tmp.front();
            std::cout << p.first << ", " << p.second << std::endl;
            tmp.pop();
        }
    }
};

#endif //TEMP_QUEUE_H

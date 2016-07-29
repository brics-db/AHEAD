/**
 * @file col_operator.h
 * @date Mar 7, 2011
 * @author Hannes Rauhe
 *
 */

#ifndef COL_OPERATOR_H_
#define COL_OPERATOR_H_

#include "task_scheduler/scheduler.h"
#include "column_storage/column.h"
#include "column_operators/predicate.h"

template<class Head, class Tail>
class BAT {
public:
    typedef unsigned row_id_t;

    BAT() : head(), tail() {
    }

    /**
     *
     * @param _head
     * @param _tail
     * @return
     */
    BAT(const column<Head>& _head, const column<Tail>& _tail) : head(_head), tail(_tail), first_head(0) {
    }

    /**
     *
     * @param _tail
     * @param _first_head the starting row - only used, if the BAT head is empty
     * @return
     */
    BAT(const column<Tail>& _tail, row_id_t _first_head = 0) : head(), tail(_tail), first_head(_first_head) {
    }

    column<Head> head;
    column<Tail> tail;
    row_id_t first_head;
};

template<class Head, class Tail>
class BAT_operator : public BAT<Head, Tail>, public scheduledOp {
public:
    virtual bool execute() = 0;
    typedef unsigned row_id_t;
};

template<class Head, class Tail>
class par_BAT_operator : public BAT<Head, Tail>, public job {
public:

    par_BAT_operator() {
        scheduler *m = scheduler::getInstance();
        this->jobID = m->createJob(this);
    }
};

#endif /* COL_OPERATOR */

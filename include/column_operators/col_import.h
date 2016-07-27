/**
 * @file col_import.h
 * @date Mar 7, 2011
 * @author Hannes Rauhe
 *
 */

#ifndef COL_IMPORT_H_
#define COL_IMPORT_H_

#include "column_storage/column.h"
#include "column_storage/Bat.h"

template<class T>
class col_import : public column<T> {
    template<class Head>
    col_import(const Bat<Head,T>& b) {
        BatIterator<Head,T> * iter = arg->begin();
        while(iter->hasNext()) {
            pair<Head,T> p = iter->next();
            push_back(p.second);
        }
        delete iter;
        return;
    }
};

#endif /* COL_IMPORT_H_ */

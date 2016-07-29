#include <algorithm>
#include <vector>
#include <iostream>
#include <cassert>

#include "column_storage/TempBat.h"
#include "column_storage/ColumnBat.h"
#include "column_storage/ColumnManager.h"
#include "column_storage/TransactionManager.h"
#include "column_operators/operators.h"
#include "column_storage/types.h"

using namespace std;

int main() {

    // load customer-region column
    Bat<unsigned, char20> * bat3 = new ColumnBat<unsigned, char20>(8);

    // load customer mktsegment column
    Bat<unsigned, char20> *bat4 = new ColumnBat<unsigned, char20>(10);

    Bat_Operators *z = new Bat_Operators();
    // restrict to EUROPE region
    char20 yy;
    memcpy(&yy, "EUROPE\0", sizeof (char20));

    Bat<unsigned, char20> *bat5 = z->selection_eq(bat3, yy);

    // mirror result;
    Bat<unsigned, unsigned> *bat6 = z->mirror(bat5);

    // restrict mktsegment column
    Bat<unsigned, char20> *bat7 = z->col_selectjoin(bat6, bat4);

    // reverse result
    Bat<char20, unsigned> *bat8 = z->reverse(bat7);

    // prepare for aggregation
    Bat<char20, unsigned> *bat9 = z->col_fill(bat8, (unsigned) 0);

    // aggreagte
    Bat<char20, unsigned> *bat10 = z->col_aggregate(bat9, (unsigned) 0);

    z->prn_vecOfPair(bat10);

    return 0;
}

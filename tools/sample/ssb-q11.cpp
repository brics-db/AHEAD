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

    // load year-column of sdate table
    Bat<unsigned, unsigned> * bat3 = new ColumnBat<unsigned, unsigned>(14);

    // load datekey-column of sdate table
    Bat<unsigned, unsigned> *bat4 = new ColumnBat<unsigned, unsigned>(11);

    // load orderdate-column of lineorder table
    Bat<unsigned, unsigned> *bat5 = new ColumnBat<unsigned, unsigned>(22);

    // load extendedprice-column of lineorder table
    Bat<unsigned, double> *bat6 = new ColumnBat<unsigned, double>(26);

    // load discount-column of lineorder table
    Bat<unsigned, unsigned> *bat7 = new ColumnBat<unsigned, unsigned>(28);

    // load quantity-column of lineorder table
    Bat<unsigned, unsigned> *bat8 = new ColumnBat<unsigned, unsigned>(25);

    Bat_Operators *z = new Bat_Operators();

    Bat<unsigned, unsigned> *bat9 = z->selection_bt(bat7, (unsigned) 3, unsigned(7));

    Bat<unsigned, unsigned> *bat10 = z->selection_lt(bat8, (unsigned) 25);

    Bat<unsigned, unsigned> *bat11 = z->selection_eq(bat3, (unsigned) 1993);

    Bat<unsigned, unsigned> *bat12 = z->mirror(bat9);
    Bat<unsigned, unsigned> *bat13 = z->mirror(bat10);

    Bat<unsigned, unsigned> *bat14 = z->col_hashjoin_left(bat9, bat10);

    Bat<unsigned, unsigned> *bat15 = z->col_hashjoin_left(bat14, bat5);

    Bat<unsigned, unsigned> *bat16 = z->mirror(bat11);

    Bat<unsigned, unsigned> *bat17 = z->col_hashjoin_left(bat16, bat4);

    Bat<unsigned, unsigned> *bat18 = z->reverse(bat17);

    Bat<unsigned, unsigned> *bat19 = z->reverse(bat15);

    Bat<unsigned, unsigned> *bat20 = z->col_nestedloop_join(bat19, bat18);

    Bat<unsigned, unsigned> *bat21 = z->mirror(bat20);

    Bat<unsigned, double> *bat22 = z->col_nestedloop_join(bat21, bat6);

    Bat<unsigned, unsigned> *bat23 = z->mirror(bat20);

    Bat<unsigned, unsigned> *bat24 = z->col_nestedloop_join(bat23, bat9);

    return 0;
}

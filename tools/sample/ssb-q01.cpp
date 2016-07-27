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

			Bat_Operators *z = new Bat_Operators();

			// selection on year-column

			// join with orderdate-column of lineorder

			Bat<unsigned, unsigned> *bat7 = z->selection_lq(bat3,(unsigned)2999);


			// mirror result;
	   	Bat<unsigned, unsigned> *bat8 = z->mirror(bat7);

			// restr
			Bat<unsigned, unsigned> *bat9 = z->col_selectjoin(bat8, bat4);



				// reverse result
				Bat<unsigned, unsigned> *bat10 = z->reverse(bat9);

				Bat<unsigned, unsigned> *bat11 = z->col_hashjoin(bat5, bat10,0);


				// mirror result
				Bat<unsigned, unsigned> *bat12 = z->mirror(bat11);

				// join with extendedprice
				Bat<unsigned, double> *bat13 = z->col_hashjoin(bat12, bat6,0);

				// reverse result
				Bat<double, unsigned> *bat14 = z->reverse(bat13);

				// prepare for aggregation
				Bat<double, unsigned> *bat15 = z->col_fill(bat14, (unsigned)0);

				Bat<unsigned, double> *bat16 = z->reverse(bat15);

				// aggreagte
				Bat<unsigned, double> *bat17 = z->col_aggregate_sum(bat16, (double)0);

				z->prn_vecOfPair(bat17);
	
	return 0;
}

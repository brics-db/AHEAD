#include <iostream>
#include <vector>
#include <string>
#include <stdlib.h>
//#include <sys/types.h>
//#include <sys/ipc.h>
//#include <sys/shm.h>
#include <time.h>
#include <ext/hash_map>


#include "column_storage/TempBat.h"
#include "column_storage/ColumnBat.h"
#include "column_storage/ColumnManager.h"
#include "column_storage/TransactionManager.h"
#include "column_operators/operators.h"
#include "column_storage/types.h"
#include "query_executor/executor.h"
#include "query_executor/codeHandler.h"

#include <unistd.h>


using namespace std;
using namespace llvm;
using namespace __gnu_cxx;


int
main (int argc, char ** argv)
{

	// create a column manager
	ColumnManager *cm = ColumnManager::getInstance();
	// create a transaction manager
	TransactionManager* tm = TransactionManager::getInstance();

	// import the data	
	TransactionManager::Transaction* t = tm->beginTransaction(true);
	assert(t!=NULL);
	t->load("/Users/habich/v2/database/customer","");
	tm->endTransaction(t);

	t = tm->beginTransaction(true);
	assert(t!=NULL);
	t->load("/Users/habich/v2/database/sdate","");
	tm->endTransaction(t);

	t = tm->beginTransaction(true);
	assert(t!=NULL);
	t->load("/Users/habich/v2/database/lineorder","");
	tm->endTransaction(t);


	const char* file = "tools/sample/ssb-q01.bc";

	Executor *exec = new Executor();
	exec->execute_query(file);

	return 0;

}


#include <string.h>

#include "column_storage/ColumnBat.h"
#include "column_storage/TempBat.h"
#include "column_operators/operators.h"

#ifndef METAREPOSITORYMANAGER_H
#define METAREPOSITORYMANAGER_H

class Bat_Operators;

using namespace std;

/**
* @author Christian Vogel
* @date 22.10.2010
*
* @todo
*/


/**
* @brief Class for managing meta data
*
* This class will manage the whole meta data stuff, where information about the database are stored. Meta data creates
* new entries when somebody creates new tables, operators or something else!
*
* The class implements the Singleton Pattern, this makes clear that only one object exists in runtime.
*/
class MetaRepositoryManager {
public:

	/**
	* @author Christian Vogel
	*
	* @return pointer for only one object of the MetaRepositoryManager class
	*
	* The function returns a pointer of the one and only existing instance of the class. In the case that no instance
	* exists, it will be created and afterwards returned from the function.
	*/
	static MetaRepositoryManager* getInstance();

	/**
	 * @author Christian Vogel
	 *
	 * @return unique id of the created table
	 *
	 * Creates an entry for a table into the meta repository.
	 */
	int createTable(char *name);

	/**
	 * @author Christian Vogel
	 *
	 * Creates an entry for an attribute of a specified table into the meta repository.
	 */
	void createAttribute(char *name, char *datatype, unsigned BATId, unsigned table_id);

	char* getDataTypeForAttribute(char *name);

	unsigned getBatIdOfAttribute(char *nameOfTable, char *attribute);

private:
	static MetaRepositoryManager *instance;

	Bat_Operators *operators;

	// all attributes for the table table :)

	Bat<unsigned, unsigned> *pk_table_id;
	Bat<unsigned, const char*> *table_name;

	// all attributes for the attribute table

	Bat<unsigned, unsigned> *pk_attribute_id;
	Bat<unsigned, const char*> *attribute_name;
	Bat<unsigned, bool> *is_dropped; //unused now
	Bat<unsigned, unsigned> *fk_table_id;
	Bat<unsigned, unsigned> *fk_type_id;
	Bat<unsigned, unsigned> *BAT_number;

	// all attributes for the layout table

	Bat<unsigned, unsigned> *pk_layout_id;
	Bat<unsigned, const char*> *layout_name;
	Bat<unsigned, unsigned> *size;

	// all attributes for the operator table

	Bat<unsigned, unsigned> *pk_operator_id;
	Bat<unsigned, const char*> *operator_name;

	// all attributes for the datatypes table

	Bat<unsigned, unsigned> *pk_datatype_id;
	Bat<unsigned, const char*> *datatype_name;
	Bat<unsigned, unsigned> *length;
	Bat<unsigned, char> *typ_category;
	Bat<unsigned, unsigned> *fk_datatype_id;

	// path where all table files are located
	char* META_PATH;
	char* TEST_DATABASE_PATH;

	void createRepository();

	void createDefaultDataTypes();

	template<class Head, class Tail>
	pair<Head, Tail> getLastValue(Bat<Head,Tail> *bat) {
		BatIterator<unsigned, unsigned> *iter = bat->begin();

		return iter->get(bat->size() - 1);
	}

	template<class Head, class Tail>
	pair<Head, Tail> unique_selection(Bat<Head,Tail> *bat, Tail value) {
		BatIterator<Head, Tail> *iter = bat->begin();

		pair<Head, Tail> nullResult;

		while (iter->hasNext()) {
			pair<Head, Tail> p = iter->next();
			if (p.second == value) {
				return p;
			}
		}
		delete(iter);

		return nullResult;
	}

	template<class Head, class Tail>
	bool isBatEmpty(Bat<Head,Tail> *bat) {
		if(bat->size() == 0) {
			return true;
		}

		return false;
	}

	template<class Head, class Tail>
	int selectBatId(Bat<Head,Tail> *bat, char *value) {
		BatIterator<Head, Tail> *iter = bat->begin();

		while(iter->hasNext()) {
			pair<Head,Tail> p = iter->next();
			if(strcmp(p.second, value) == 0) {
				delete(iter);
				return p.first;
			}
		}
		delete(iter);

		return -1;
	}

	template<class Head, class Tail>
	int selectBatId(Bat<Head,Tail> *bat, int value) {
		BatIterator<Head, Tail> *iter = bat->begin();

		while(iter->hasNext()) {
			pair<Head,Tail> p = iter->next();
			if(p.second == value) {
				delete(iter);
				return p.first;
			}
		}
		delete(iter);

		return -1;
	}

	template<class Head, class Tail>
	Tail selection(Bat<Head,Tail> *bat, Tail value) {
		BatIterator<Head, Tail> *iter = bat->begin();

		while(iter->hasNext()) {
			pair<Head,Tail> p = iter->next();
			if(p.second == value) {
				delete(iter);
				return p.first;
			}
		}
		delete(iter);

		return 0;
	}

	template<class Head, class Tail>
	int selectPKId(Bat<Head,Tail> *bat, int batId) {
		BatIterator<Head, Tail> *iter = bat->begin();

		while(iter->hasNext()) {
			pair<Head,Tail> p = iter->next();
			if(p.first == batId) {
				delete(iter);
				return p.second;
			}
		}
		delete(iter);

		return -1;
	}

	template<class Head, class Tail>
	bool dataAlreadyExists(Bat<Head,Tail> *bat, char* name_value) {
		BatIterator<Head, Tail> *iter = bat->begin();

		while(iter->hasNext()) {
			pair<Head,Tail> p = iter->next();
			if(strcmp(p.second, name_value) == 0) {
				delete(iter);
				return true;
			}
		}
		delete(iter);

		return false;
	}

	MetaRepositoryManager();
	~MetaRepositoryManager();
};

#endif

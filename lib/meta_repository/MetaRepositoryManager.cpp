#include "meta_repository/MetaRepositoryManager.h"


MetaRepositoryManager* MetaRepositoryManager::instance = 0;

MetaRepositoryManager* MetaRepositoryManager::getInstance() {
	if(MetaRepositoryManager::instance == 0) {
		MetaRepositoryManager::instance = new MetaRepositoryManager();
	}

	return MetaRepositoryManager::instance;
}

MetaRepositoryManager::MetaRepositoryManager() {
	META_PATH = "/home/sichvoge/workspace/V2/database/INFORMATION_SCHEMA/";
	TEST_DATABASE_PATH = "/home/sichvoge/workspace/V2/database/";

	// creates the whole repository
	this->createRepository();
	this->createDefaultDataTypes();

	this->operators = new Bat_Operators();

	// test table
	table_name->append(make_pair(0, "tables"));
	pk_table_id->append(make_pair(0,1));
}

void MetaRepositoryManager::createDefaultDataTypes() {
	pk_datatype_id->append(make_pair(0, 1));
	pk_datatype_id->append(make_pair(1, 2));
	pk_datatype_id->append(make_pair(2, 3));
	pk_datatype_id->append(make_pair(3, 4));

	datatype_name->append(make_pair(0, "INTEGER"));
	datatype_name->append(make_pair(1, "STRING"));
	datatype_name->append(make_pair(2, "FIXED"));
	datatype_name->append(make_pair(3, "CHAR"));

	length->append(make_pair(0, 4));
	length->append(make_pair(1, 64));
	length->append(make_pair(2, 4));
	length->append(make_pair(3, 1));

	typ_category->append(make_pair(0, 'N'));
	typ_category->append(make_pair(1, 'S'));
	typ_category->append(make_pair(2, 'N'));
	typ_category->append(make_pair(3, 'S'));

	fk_datatype_id->append(make_pair(0, 0));
	fk_datatype_id->append(make_pair(1, 4));
	fk_datatype_id->append(make_pair(2, 0));
	fk_datatype_id->append(make_pair(2, 0));
}

void MetaRepositoryManager::createRepository() {
	// create bats for table meta
	pk_table_id = new TempBat<unsigned, unsigned>();
	table_name = new TempBat<unsigned, const char*>();

	// create bats for attribute meta
	pk_attribute_id = new TempBat<unsigned, unsigned>();
	attribute_name = new TempBat<unsigned, const char*>();
	fk_table_id = new TempBat<unsigned, unsigned>();
	fk_type_id = new TempBat<unsigned, unsigned>();
	BAT_number = new TempBat<unsigned, unsigned>();

	// create bats for layout meta
	pk_layout_id = new TempBat<unsigned, unsigned>();
	layout_name = new TempBat<unsigned, const char*>();
	size = new TempBat<unsigned, unsigned>();

	// create bats for operator meta
	pk_operator_id = new TempBat<unsigned, unsigned>();
	operator_name = new TempBat<unsigned, const char*>();

	// create bats for datatype meta
	pk_datatype_id = new TempBat<unsigned, unsigned>();
	datatype_name = new TempBat<unsigned, const char*>();
	length = new TempBat<unsigned, unsigned>();
	typ_category = new TempBat<unsigned, char>();
	fk_datatype_id = new TempBat<unsigned, unsigned>();
}

int MetaRepositoryManager::createTable(char *name) {
	int newTableId = -1;

	if(!dataAlreadyExists(table_name, name)) {
		pair<unsigned, unsigned> value = getLastValue(pk_table_id);
		// value.first is the last given head id
		unsigned newId = value.first + 1;

		// value.second is the last given id for a table
		newTableId = (int)value.second + 1;

		pk_table_id->append(make_pair(newId, newTableId));

		table_name->append(make_pair(newId, name));
	} else {
		// table already exists
		cout << "Table already exist!" <<endl;
	}

	return newTableId;
}

unsigned MetaRepositoryManager::getBatIdOfAttribute(char *nameOfTable, char *attribute) {
	pair<unsigned, unsigned> batNrPair;

	int batIdForTableName = this->selectBatId(table_name, nameOfTable);
	int tableId = this->selectPKId(pk_table_id, batIdForTableName);

	if(tableId != -1) {
		Bat<unsigned, unsigned> *batForTableId = operators->selection_eq(fk_table_id, (unsigned)tableId);

		// first make mirror bat, because the joining algorithm will join the tail of the first bat with the head of the second bat
		// reverse will not work here, because we need the bat id, not the table id
		Bat<unsigned, unsigned> *mirrorTableIdBat = operators->mirror(batForTableId);
		Bat<unsigned, const char*> *attributesForTable = operators->col_selectjoin(mirrorTableIdBat, attribute_name);

		int batId = this->selectBatId(attributesForTable, attribute);
		Bat<unsigned, unsigned> *reverse = operators->reverse(BAT_number);

		batNrPair = this->unique_selection(reverse, (unsigned)batId);
	}

	return batNrPair.first;
}

void MetaRepositoryManager::createAttribute(char *name, char *datatype, unsigned BATId, unsigned table_id) {
	int batIdOfDataType = selectBatId(datatype_name, datatype);
	int dataTypeId = selectPKId(pk_datatype_id, batIdOfDataType);

	unsigned newAttributeId;
	unsigned newBatId;

	if(isBatEmpty(pk_attribute_id) == 0) {
		pair<unsigned, unsigned> value = getLastValue(pk_attribute_id);
		newAttributeId = value.second + 1;
		newBatId = value.first + 1;
	}
	else {
		newAttributeId = 1;
		newBatId = 0;
	}

	pk_attribute_id->append(make_pair(newBatId, newAttributeId));
	attribute_name->append(make_pair(newBatId, name));
	fk_table_id->append(make_pair(newBatId, table_id));
	fk_type_id->append(make_pair(newBatId, dataTypeId));
	BAT_number->append(make_pair(newBatId, BATId));
}

char* MetaRepositoryManager::getDataTypeForAttribute(char *name) {
	return "";
}


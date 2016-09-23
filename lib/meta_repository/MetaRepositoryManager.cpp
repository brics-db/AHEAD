#include <cstring>

#include <meta_repository/MetaRepositoryManager.h>
#include <column_operators/operators.h>
#include <util/resilience.hpp>

MetaRepositoryManager* MetaRepositoryManager::instance = nullptr;
char* MetaRepositoryManager::strBaseDir = nullptr;

auto PATH_DATABASE = "/database";
auto PATH_INFORMATION_SCHEMA = "/database/INFORMATION_SCHEMA";

const char* NAME_TINYINT = "TINYINT";
const char* NAME_SHORTINT = "SHORTINT";
const char* NAME_INTEGER = "INTEGER";
const char* NAME_LARGEINT = "LARGEINT";
const char* NAME_STRING = "STRING";
const char* NAME_FIXED = "FIXED";
const char* NAME_CHAR = "CHAR";
const char* NAME_RESTINY = "RESTINY";
const char* NAME_RESSHORT = "RESSHORT";
const char* NAME_RESINT = "RESINT";

const size_t MAXLEN_STRING = 64;

MetaRepositoryManager::MetaRepositoryManager() {
    // creates the whole repository
    this->createRepository();
    this->createDefaultDataTypes();

    // test table
    //    table_name->append(make_pair(0, "tables"));
    pk_table_id->append(make_pair(0, 1));
}

MetaRepositoryManager::~MetaRepositoryManager() {
    delete pk_table_id;
    delete table_name;
    delete pk_attribute_id;
    delete attribute_name;
    delete fk_table_id;
    delete fk_type_id;
    delete BAT_number;
    delete pk_layout_id;
    delete layout_name;
    delete size;
    delete pk_operator_id;
    delete operator_name;
    delete pk_datatype_id;
    delete datatype_name;
    delete datatype_length;
    delete datatype_category;
}

MetaRepositoryManager* MetaRepositoryManager::getInstance() {
    if (MetaRepositoryManager::instance == 0) {
        MetaRepositoryManager::instance = new MetaRepositoryManager();
    }

    return MetaRepositoryManager::instance;
}

void MetaRepositoryManager::destroyInstance() {
    if (MetaRepositoryManager::instance) {
        delete MetaRepositoryManager::instance;
        MetaRepositoryManager::instance = nullptr;
    }
}

void MetaRepositoryManager::init(const char* strBaseDir) {
    if (MetaRepositoryManager::strBaseDir == nullptr) {
        size_t len = strlen(strBaseDir);
        MetaRepositoryManager::strBaseDir = new char[len + 1];
        memcpy(MetaRepositoryManager::strBaseDir, strBaseDir, len + 1); // includes NULL character

        getInstance(); // make sure that an instance exists

        size_t len2 = strlen(PATH_DATABASE);
        instance->TEST_DATABASE_PATH = new char[len + len2 + 1];
        memcpy(instance->TEST_DATABASE_PATH, strBaseDir, len); // excludes NULL character
        memcpy(instance->TEST_DATABASE_PATH + len, PATH_DATABASE, len2 + 1); // includes NULL character

        size_t len3 = strlen(PATH_INFORMATION_SCHEMA);
        instance->META_PATH = new char[len + len3 + 1];
        memcpy(instance->META_PATH, strBaseDir, len); // excludes NULL character
        memcpy(instance->META_PATH + len, PATH_INFORMATION_SCHEMA, len3 + 1); // includes NULL character
    }
}

void MetaRepositoryManager::createDefaultDataTypes() {
    pk_datatype_id->append(make_pair(0, 1));
    pk_datatype_id->append(make_pair(1, 2));
    pk_datatype_id->append(make_pair(2, 3));
    pk_datatype_id->append(make_pair(3, 4));
    pk_datatype_id->append(make_pair(4, 5));
    pk_datatype_id->append(make_pair(5, 6));
    pk_datatype_id->append(make_pair(6, 7));
    pk_datatype_id->append(make_pair(7, 8));
    pk_datatype_id->append(make_pair(8, 9));
    pk_datatype_id->append(make_pair(9, 10));

    datatype_name->append(make_pair(0, NAME_TINYINT));
    datatype_name->append(make_pair(1, NAME_SHORTINT));
    datatype_name->append(make_pair(2, NAME_INTEGER));
    datatype_name->append(make_pair(3, NAME_LARGEINT));
    datatype_name->append(make_pair(4, NAME_STRING));
    datatype_name->append(make_pair(5, NAME_FIXED));
    datatype_name->append(make_pair(6, NAME_CHAR));
    datatype_name->append(make_pair(7, NAME_RESTINY));
    datatype_name->append(make_pair(8, NAME_RESSHORT));
    datatype_name->append(make_pair(9, NAME_RESINT));

    datatype_length->append(make_pair(0, sizeof (tinyint_t)));
    datatype_length->append(make_pair(1, sizeof (shortint_t)));
    datatype_length->append(make_pair(2, sizeof (int_t)));
    datatype_length->append(make_pair(3, sizeof (bigint_t)));
    datatype_length->append(make_pair(4, sizeof (char_t) * MAXLEN_STRING));
    datatype_length->append(make_pair(5, sizeof (fixed_t)));
    datatype_length->append(make_pair(6, sizeof (char_t)));
    datatype_length->append(make_pair(7, sizeof (restiny_t)));
    datatype_length->append(make_pair(8, sizeof (resshort_t)));
    datatype_length->append(make_pair(9, sizeof (resint_t)));

    datatype_category->append(make_pair(0, 'N'));
    datatype_category->append(make_pair(1, 'N'));
    datatype_category->append(make_pair(2, 'N'));
    datatype_category->append(make_pair(3, 'N'));
    datatype_category->append(make_pair(4, 'S'));
    datatype_category->append(make_pair(5, 'N'));
    datatype_category->append(make_pair(6, 'S'));
    datatype_category->append(make_pair(7, 'N'));
    datatype_category->append(make_pair(8, 'N'));
    datatype_category->append(make_pair(9, 'N'));
}

void MetaRepositoryManager::createRepository() {
    // create bats for table meta
    pk_table_id = new id_tmpbat_t;
    table_name = new cstr_tmpbat_t;

    // create bats for attribute meta
    pk_attribute_id = new id_tmpbat_t;
    attribute_name = new cstr_tmpbat_t;
    fk_table_id = new id_tmpbat_t;
    fk_type_id = new id_tmpbat_t;
    BAT_number = new id_tmpbat_t;

    // create bats for layout meta
    pk_layout_id = new id_tmpbat_t;
    layout_name = new cstr_tmpbat_t;
    size = new size_tmpbat_t;

    // create bats for operator meta
    pk_operator_id = new id_tmpbat_t;
    operator_name = new cstr_tmpbat_t;

    // create bats for datatype meta
    pk_datatype_id = new id_tmpbat_t;
    datatype_name = new cstr_tmpbat_t;
    datatype_length = new size_tmpbat_t;
    datatype_category = new char_tmpbat_t;
}

id_t MetaRepositoryManager::createTable(const char *name) {
    id_t newTableId = ID_INVALID;

    if (!dataAlreadyExists(table_name, name)) {
        pair<oid_t, id_t> value = getLastValue(pk_table_id);
        // value.first is the last given head id
        id_t newId = value.first + 1;

        // value.second is the last given id for a table
        newTableId = value.second + 1;

        pk_table_id->append(make_pair(newId, newTableId));
        table_name->append(make_pair(newId, name));
    } else {
        // table already exists
        cerr << "Table already exist!" << endl;
    }

    return newTableId;
}

id_t MetaRepositoryManager::getBatIdOfAttribute(cstr_t nameOfTable, cstr_t attribute) {
    pair<unsigned, unsigned> batNrPair;

    int batIdForTableName = this->selectBatId(table_name, nameOfTable);
    int tableId = this->selectPKId(pk_table_id, static_cast<unsigned> (batIdForTableName));

    if (tableId != -1) {
        auto batForTableId = v2::bat::ops::selection_eq(fk_table_id, (unsigned) tableId);

        // first make mirror bat, because the joining algorithm will join the tail of the first bat with the head of the second bat
        // reverse will not work here, because we need the bat id, not the table id
        auto mirrorTableIdBat = v2::bat::ops::mirrorHead(batForTableId);
        delete batForTableId;
        auto attributesForTable = v2::bat::ops::col_selectjoin(mirrorTableIdBat, attribute_name);
        delete mirrorTableIdBat;
        int batId = this->selectBatId(attributesForTable, attribute);
        delete attributesForTable;
        auto reverseBat = v2::bat::ops::reverse(BAT_number);
        batNrPair = this->unique_selection(reverseBat, (unsigned) batId);
        delete reverseBat;
    }

    return batNrPair.first;
}

void MetaRepositoryManager::createAttribute(char *name, char *datatype, unsigned BATId, unsigned table_id) {
    id_t batIdOfDataType = selectBatId(datatype_name, datatype);
    id_t dataTypeId = selectPKId(pk_datatype_id, batIdOfDataType);
    id_t newAttributeId = getLastValue(pk_attribute_id).second + 1;

    pk_attribute_id->append(newAttributeId);
    attribute_name->append(name);
    fk_table_id->append(table_id);
    fk_type_id->append(dataTypeId);
    BAT_number->append(BATId);
}

char* MetaRepositoryManager::getDataTypeForAttribute(char *name) {
    return nullptr;
}


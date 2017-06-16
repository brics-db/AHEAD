// Copyright (c) 2016-2017 Till Kolditz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstring>

#include <ColumnStore.h>
#include <column_operators/Operators.hpp>
#include "MetaRepositoryManager.h"

namespace ahead {

    std::shared_ptr<MetaRepositoryManager> MetaRepositoryManager::instance;

    auto PATH_INFORMATION_SCHEMA = "INFORMATION_SCHEMA";

    cstr_t NAME_TINYINT = "TINYINT";
    cstr_t NAME_SHORTINT = "SHORTINT";
    cstr_t NAME_INTEGER = "INTEGER";
    cstr_t NAME_LARGEINT = "LARGEINT";
    cstr_t NAME_STRING = "STRING";
    cstr_t NAME_FIXED = "FIXED";
    cstr_t NAME_CHAR = "CHAR";
    cstr_t NAME_RESTINY = "RESTINY";
    cstr_t NAME_RESSHORT = "RESSHORT";
    cstr_t NAME_RESINT = "RESINT";
    cstr_t NAME_RESBIGINT = "RESBIGINT";

    const size_t MAXLEN_STRING = 64;

    MetaRepositoryManager::MetaRepositoryManager()
            : strBaseDir(nullptr),
              tables_id_pk(nullptr),
              tables_name(nullptr),
              attributes_id_pk(nullptr),
              attributes_name(nullptr),
              attributes_table_id_fk(nullptr),
              attributes_type_id_fk(nullptr),
              attributes_column_id(nullptr),
              layout_id_pk(nullptr),
              layout_name(nullptr),
              layout_size(nullptr),
              operator_id_pk(nullptr),
              operator_name(nullptr),
              datatype_id_pk(nullptr),
              datatype_name(nullptr),
              datatype_length(nullptr),
              datatype_category(nullptr),
              META_PATH(nullptr) {
        // creates the whole repository
        this->createRepository();
        this->createDefaultDataTypes();
    }

    MetaRepositoryManager::MetaRepositoryManager(
            const MetaRepositoryManager & copy)
            : strBaseDir(copy.strBaseDir),
              tables_id_pk(copy.tables_id_pk),
              tables_name(copy.tables_name),
              attributes_id_pk(copy.attributes_id_pk),
              attributes_name(copy.attributes_name),
              attributes_table_id_fk(copy.attributes_table_id_fk),
              attributes_type_id_fk(copy.attributes_type_id_fk),
              attributes_column_id(copy.attributes_column_id),
              layout_id_pk(copy.layout_id_pk),
              layout_name(copy.layout_name),
              layout_size(copy.layout_size),
              operator_id_pk(copy.operator_id_pk),
              operator_name(copy.operator_name),
              datatype_id_pk(copy.datatype_id_pk),
              datatype_name(copy.datatype_name),
              datatype_length(copy.datatype_length),
              datatype_category(copy.datatype_category),
              META_PATH(copy.META_PATH) {
    }

    MetaRepositoryManager::~MetaRepositoryManager() {
        delete tables_id_pk;
        delete tables_name;
        delete attributes_id_pk;
        delete attributes_name;
        delete attributes_table_id_fk;
        delete attributes_type_id_fk;
        delete attributes_column_id;
        delete layout_id_pk;
        delete layout_name;
        delete layout_size;
        delete operator_id_pk;
        delete operator_name;
        delete datatype_id_pk;
        delete datatype_name;
        delete datatype_length;
        delete datatype_category;
    }

    std::shared_ptr<MetaRepositoryManager> MetaRepositoryManager::getInstance() {
        if (!MetaRepositoryManager::instance) {
            MetaRepositoryManager::instance.reset(new MetaRepositoryManager());
        }
        return MetaRepositoryManager::instance;
    }

    void MetaRepositoryManager::destroyInstance() {
        MetaRepositoryManager::instance.reset();
    }

    void MetaRepositoryManager::init(
            cstr_t strBaseDir) {
        if (this->strBaseDir == nullptr) {
            size_t len = strlen(strBaseDir);
            this->strBaseDir = new char[len + 1];
            memcpy(this->strBaseDir, strBaseDir, len + 1); // includes NULL character

            size_t len3 = strlen(PATH_INFORMATION_SCHEMA);
            META_PATH = new char[len + len3 + 1];
            memcpy(META_PATH, strBaseDir, len); // excludes NULL character
            memcpy(META_PATH + len, PATH_INFORMATION_SCHEMA, len3 + 1); // includes NULL character
        }
    }

    void MetaRepositoryManager::init(
            const std::string & strBaseDir) {
        init(strBaseDir.c_str());
    }

    void MetaRepositoryManager::createDefaultDataTypes() {
        datatype_id_pk->append(std::make_pair(0u, 1u));
        datatype_id_pk->append(std::make_pair(1u, 2u));
        datatype_id_pk->append(std::make_pair(2u, 3u));
        datatype_id_pk->append(std::make_pair(3u, 4u));
        datatype_id_pk->append(std::make_pair(4u, 5u));
        datatype_id_pk->append(std::make_pair(5u, 6u));
        datatype_id_pk->append(std::make_pair(6u, 7u));
        datatype_id_pk->append(std::make_pair(7u, 8u));
        datatype_id_pk->append(std::make_pair(8u, 9u));
        datatype_id_pk->append(std::make_pair(9u, 10u));
        datatype_id_pk->append(std::make_pair(10u, 11u));

        datatype_name->append(std::make_pair(0u, const_cast<str_t>(NAME_TINYINT)));
        datatype_name->append(std::make_pair(1u, const_cast<str_t>(NAME_SHORTINT)));
        datatype_name->append(std::make_pair(2u, const_cast<str_t>(NAME_INTEGER)));
        datatype_name->append(std::make_pair(3u, const_cast<str_t>(NAME_LARGEINT)));
        datatype_name->append(std::make_pair(4u, const_cast<str_t>(NAME_STRING)));
        datatype_name->append(std::make_pair(5u, const_cast<str_t>(NAME_FIXED)));
        datatype_name->append(std::make_pair(6u, const_cast<str_t>(NAME_CHAR)));
        datatype_name->append(std::make_pair(7u, const_cast<str_t>(NAME_RESTINY)));
        datatype_name->append(std::make_pair(8u, const_cast<str_t>(NAME_RESSHORT)));
        datatype_name->append(std::make_pair(9u, const_cast<str_t>(NAME_RESINT)));
        datatype_name->append(std::make_pair(10u, const_cast<str_t>(NAME_RESBIGINT)));

        datatype_length->append(std::make_pair(0u, sizeof(tinyint_t)));
        datatype_length->append(std::make_pair(1u, sizeof(shortint_t)));
        datatype_length->append(std::make_pair(2u, sizeof(int_t)));
        datatype_length->append(std::make_pair(3u, sizeof(bigint_t)));
        datatype_length->append(std::make_pair(4u, sizeof(char_t) * MAXLEN_STRING));
        datatype_length->append(std::make_pair(5u, sizeof(fixed_t)));
        datatype_length->append(std::make_pair(6u, sizeof(char_t)));
        datatype_length->append(std::make_pair(7u, sizeof(restiny_t)));
        datatype_length->append(std::make_pair(8u, sizeof(resshort_t)));
        datatype_length->append(std::make_pair(9u, sizeof(resint_t)));
        datatype_length->append(std::make_pair(10u, sizeof(resbigint_t)));

        datatype_category->append(std::make_pair(0u, 'N'));
        datatype_category->append(std::make_pair(1u, 'N'));
        datatype_category->append(std::make_pair(2u, 'N'));
        datatype_category->append(std::make_pair(3u, 'N'));
        datatype_category->append(std::make_pair(4u, 'S'));
        datatype_category->append(std::make_pair(5u, 'N'));
        datatype_category->append(std::make_pair(6u, 'S'));
        datatype_category->append(std::make_pair(7u, 'N'));
        datatype_category->append(std::make_pair(8u, 'N'));
        datatype_category->append(std::make_pair(9u, 'N'));
        datatype_category->append(std::make_pair(10u, 'N'));
    }

    void MetaRepositoryManager::createRepository() {
        // create bats for table meta
        tables_id_pk = new id_tmpbat_t;
        tables_name = new str_tmpbat_t;

        // create bats for attribute meta
        attributes_id_pk = new id_tmpbat_t;
        attributes_name = new str_tmpbat_t;
        attributes_table_id_fk = new id_tmpbat_t;
        attributes_type_id_fk = new id_tmpbat_t;
        attributes_column_id = new id_tmpbat_t;

        // create bats for layout meta
        layout_id_pk = new id_tmpbat_t;
        layout_name = new str_tmpbat_t;
        layout_size = new size_tmpbat_t;

        // create bats for operator meta
        operator_id_pk = new id_tmpbat_t;
        operator_name = new str_tmpbat_t;

        // create bats for datatype meta
        datatype_id_pk = new id_tmpbat_t;
        datatype_name = new str_tmpbat_t;
        datatype_length = new size_tmpbat_t;
        datatype_category = new char_tmpbat_t;
    }

    id_t MetaRepositoryManager::createTable(
            cstr_t name) {
        id_t newTableId = ID_INVALID;

        if (!dataAlreadyExists(tables_name, name)) {
            std::pair<oid_t, id_t> value = getLastValue(tables_id_pk);
            // value.first is the last given head id
            id_t newId = value.first + 1;

            // value.second is the last given id for a table
            newTableId = value.second + 1;

            tables_id_pk->append(std::make_pair(newId, newTableId));
            tables_name->append(std::make_pair(newId, const_cast<str_t>(name)));
        } else {
            // table already exists
            throw std::runtime_error("Table already exist!");
        }

        return newTableId;
    }

    id_t MetaRepositoryManager::getBatIdOfAttribute(
            cstr_t nameOfTable,
            cstr_t attribute) {
        std::pair<id_t, id_t> batNrPair;

        id_t batIdForTableName = this->selectBatId(tables_name, nameOfTable);
        id_t tableId = this->selectPKId(tables_id_pk, batIdForTableName);

        if (tableId != ID_INVALID) {
            auto batForTableId = ahead::bat::ops::sse::select<std::equal_to>(attributes_table_id_fk, std::move(tableId));

            // first make mirror bat, because the joining algorithm will join the tail of the first bat with the head of the second bat
            // reverse will not work here, because we need the bat id, not the table id
            auto mirrorTableIdBat = batForTableId->mirror_head();
            delete batForTableId;
            auto attributesForTable = ahead::bat::ops::hashjoin(mirrorTableIdBat, attributes_name);
            delete mirrorTableIdBat;
            id_t batId = this->selectBatId(attributesForTable, attribute);
            delete attributesForTable;
            auto reverseBat = attributes_column_id->reverse();
            batNrPair = this->unique_selection(reverseBat, batId);
            delete reverseBat;
        }

        return batNrPair.first;
    }

    void MetaRepositoryManager::createAttribute(
            cstr_t name,
            cstr_t datatype,
            id_t columnID,
            id_t tableID) {
        id_t batIdOfDataType = selectBatId(datatype_name, datatype);
        id_t dataTypeID = selectPKId(datatype_id_pk, batIdOfDataType);
        id_t newAttributeID = getLastValue(attributes_id_pk).second + 1;

        attributes_id_pk->append(newAttributeID);
        auto nameCopy = new char_t[strlen(name) + 1];
        strcpy(nameCopy, name);
        attributes_name->append(nameCopy);
        attributes_table_id_fk->append(tableID);
        attributes_type_id_fk->append(dataTypeID);
        attributes_column_id->append(columnID);
    }

    char*
    MetaRepositoryManager::getDataTypeForAttribute(
            __attribute__ ((unused)) cstr_t name) {
        return nullptr;
    }

    MetaRepositoryManager::TablesIterator::TablesIterator()
            : pKeyIter(MetaRepositoryManager::instance->tables_id_pk->begin()),
              pNameIter(MetaRepositoryManager::instance->tables_name->begin()) {
    }

    MetaRepositoryManager::TablesIterator::TablesIterator(
            const TablesIterator &iter)
            : pKeyIter(new typename table_key_iter_t::self_t(*iter.pKeyIter)),
              pNameIter(new typename table_name_iter_t::self_t(*iter.pNameIter)) {
    }

    MetaRepositoryManager::TablesIterator::~TablesIterator() {
        delete pKeyIter;
        delete pNameIter;
    }

    MetaRepositoryManager::TablesIterator& MetaRepositoryManager::TablesIterator::operator=(
            const TablesIterator &copy) {
        new (this) TablesIterator(copy);
        return *this;
    }

    void MetaRepositoryManager::TablesIterator::next() {
        pKeyIter->next();
        pNameIter->next();
    }

    MetaRepositoryManager::TablesIterator& MetaRepositoryManager::TablesIterator::operator++() {
        next();
        return *this;
    }

    MetaRepositoryManager::TablesIterator& MetaRepositoryManager::TablesIterator::operator+=(
            oid_t i) {
        (*pKeyIter) += i;
        (*pNameIter) += i;
        return *this;
    }

    void MetaRepositoryManager::TablesIterator::position(
            oid_t index) {
        pKeyIter->position(index);
        pNameIter->position(index);
    }

    bool MetaRepositoryManager::TablesIterator::hasNext() {
        return pKeyIter->hasNext();
    }

    id_t MetaRepositoryManager::TablesIterator::head() {
        return pKeyIter->tail();
    }

    str_t MetaRepositoryManager::TablesIterator::tail() {
        return pNameIter->tail();
    }

    size_t MetaRepositoryManager::TablesIterator::size() {
        return pKeyIter->size();
    }

    size_t MetaRepositoryManager::TablesIterator::consumption() {
        return pKeyIter->consumption() + pNameIter->consumption();
    }

}

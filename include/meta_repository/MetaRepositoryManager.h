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

/* 
 * File:   MetaRepositoryManager.h
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 27. Juli 2016, 18:42
 */

#ifndef METAREPOSITORYMANAGER_H
#define METAREPOSITORYMANAGER_H

#include <cstring>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBat.h>

extern const char* NAME_TINYINT;
extern const char* NAME_SHORTINT;
extern const char* NAME_INTEGER;
extern const char* NAME_LARGEINT;
extern const char* NAME_STRING;
extern const char* NAME_FIXED;
extern const char* NAME_CHAR;
extern const char* NAME_RESTINY;
extern const char* NAME_RESSHORT;
extern const char* NAME_RESINT;

extern const size_t MAXLEN_STRING;

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
    friend class TransactionManager;

    /**
     * @author Christian Vogel
     *
     * @return pointer for only one object of the MetaRepositoryManager class
     *
     * The function returns a pointer of the one and only existing instance of the class. In the case that no instance
     * exists, it will be created and afterwards returned from the function.
     */
    static MetaRepositoryManager* getInstance ();

    static void init (const char* strBaseDir);

    static void init (const std::string & strBaseDir);

    /**
     * @author Christian Vogel
     *
     * @return unique id of the created table
     *
     * Creates an entry for a table into the meta repository.
     */
    id_t createTable (const char *name);

    /**
     * @author Christian Vogel
     *
     * Creates an entry for an attribute of a specified table into the meta repository.
     */
    void createAttribute (char *name, char *datatype, unsigned BATId, unsigned table_id);

    char* getDataTypeForAttribute (char *name);

    unsigned getBatIdOfAttribute (const char *nameOfTable, const char *attribute);

private:
    static MetaRepositoryManager *instance;
    static char* strBaseDir;

    static void destroyInstance ();

    // all BATs for the "tables" table
    id_tmpbat_t *tables_id_pk;
    str_tmpbat_t *tables_name;

    // all BATs for the "attributes" table
    id_tmpbat_t *attributes_id_pk;
    str_tmpbat_t *attributes_name;
    id_tmpbat_t *attributes_table_id_fk;
    id_tmpbat_t *attributes_type_id_fk;
    id_tmpbat_t *attributes_column_id;

    // all BATs for the "layout" table
    id_tmpbat_t *layout_id_pk;
    str_tmpbat_t *layout_name;
    size_tmpbat_t *layout_size;

    // all BATs for the "operator" table
    id_tmpbat_t *operator_id_pk;
    str_tmpbat_t *operator_name;

    // all BATs for the "datatypes" table
    id_tmpbat_t *datatype_id_pk;
    str_tmpbat_t *datatype_name;
    size_tmpbat_t *datatype_length;
    char_tmpbat_t *datatype_category;

    // path where all table files are located
    char* META_PATH;
    // char* TEST_DATABASE_PATH;

    void createRepository ();

    void createDefaultDataTypes ();

    template<class Head, class Tail>
    std::pair<typename Head::type_t, typename Tail::type_t> getLastValue (BAT<Head, Tail> *bat);

    template<class Head, class Tail>
    std::pair<typename Head::type_t, typename Tail::type_t> unique_selection (BAT<Head, Tail> *bat, typename Tail::type_t value);

    template<class Head, class Tail>
    bool isBatEmpty (BAT<Head, Tail> *bat);

    template<class Head, class Tail>
    id_t selectBatId (BAT<Head, Tail> *bat, cstr_t value);

    template<class Head, class Tail>
    id_t selectBatId (BAT<Head, Tail> *bat, typename Tail::type_t value);

    template<class Head, class Tail>
    typename Head::type_t selection (BAT<Head, Tail> *bat, typename Tail::type_t value);

    template<class Head, class Tail>
    id_t selectPKId (BAT<Head, Tail> *bat, typename Head::type_t batId);

    template<class Head, class Tail>
    bool dataAlreadyExists (BAT<Head, Tail> *bat, cstr_t name_value);

    MetaRepositoryManager ();
    MetaRepositoryManager (const MetaRepositoryManager &copy);
    virtual
    ~MetaRepositoryManager ();
    MetaRepositoryManager& operator= (const MetaRepositoryManager &copy);

public:

    class TablesIterator : public BATIterator<v2_id_t, v2_str_t> {

        typedef TempBATIterator<v2_void_t, v2_id_t> table_key_iter_t;
        typedef TempBATIterator<v2_void_t, v2_str_t> table_name_iter_t;

        table_key_iter_t *pKeyIter;
        table_name_iter_t *pNameIter;

    public:

        TablesIterator ();

        TablesIterator (const TablesIterator &iter);

        virtual ~TablesIterator ();

        TablesIterator& operator= (const TablesIterator &copy);

        virtual void next () override;

        virtual TablesIterator& operator++ () override;

        virtual TablesIterator& operator+= (oid_t i) override;

        virtual void position (oid_t index) override;

        virtual bool hasNext () override;

        virtual id_t head () override;

        virtual str_t tail () override;

        virtual size_t size () override;

        virtual size_t consumption () override;

        friend class MetaRepositoryManager;
    };

    TablesIterator*
    listTables () {
        return new TablesIterator;
    }
};

#include "meta_repository/MetaRepositoryManager.tcc"

#endif

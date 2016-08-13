#ifndef METAREPOSITORYMANAGER_H
#define METAREPOSITORYMANAGER_H

#include <cstring>

#include <ColumnStore.h>
#include <column_storage/Bat.h>

using namespace std;

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
    static MetaRepositoryManager* getInstance();

    static void init(const char* strBaseDir);

    /**
     * @author Christian Vogel
     *
     * @return unique id of the created table
     *
     * Creates an entry for a table into the meta repository.
     */
    id_t createTable(const char *name);

    /**
     * @author Christian Vogel
     *
     * Creates an entry for an attribute of a specified table into the meta repository.
     */
    void createAttribute(char *name, char *datatype, unsigned BATId, unsigned table_id);

    char* getDataTypeForAttribute(char *name);

    unsigned getBatIdOfAttribute(const char *nameOfTable, const char *attribute);

private:
    static MetaRepositoryManager *instance;
    static char* strBaseDir;

    static void destroyInstance();

    // all attributes for the table table :)
    Bat<oid_t, id_t> *pk_table_id;
    Bat<oid_t, const char*> *table_name;

    // all attributes for the attribute table
    Bat<oid_t, id_t> *pk_attribute_id;
    Bat<oid_t, const char*> *attribute_name;
    Bat<oid_t, id_t> *fk_table_id;
    Bat<oid_t, unsigned> *fk_type_id;
    Bat<oid_t, unsigned> *BAT_number;

    // all attributes for the layout table
    Bat<oid_t, id_t> *pk_layout_id;
    Bat<oid_t, const char*> *layout_name;
    Bat<oid_t, unsigned> *size;

    // all attributes for the operator table
    Bat<oid_t, id_t> *pk_operator_id;
    Bat<oid_t, const char*> *operator_name;

    // all attributes for the datatypes table
    Bat<oid_t, id_t> *pk_datatype_id;
    Bat<oid_t, const char*> *datatype_name;
    Bat<oid_t, unsigned> *datatype_length;
    Bat<oid_t, char> *datatype_category;

    // path where all table files are located
    char* META_PATH;
    char* TEST_DATABASE_PATH;

    void createRepository();

    void createDefaultDataTypes();

    template<class Head, class Tail>
    pair<Head, Tail> getLastValue(Bat<Head, Tail> *bat);

    template<class Head, class Tail>
    pair<Head, Tail> unique_selection(Bat<Head, Tail> *bat, Tail value);

    template<class Head, class Tail>
    bool isBatEmpty(Bat<Head, Tail> *bat);

    template<class Head, class Tail>
    int selectBatId(Bat<Head, Tail> *bat, const char *value);

    template<class Head, class Tail>
    int selectBatId(Bat<Head, Tail> *bat, int value);

    template<class Head, class Tail>
    Tail selection(Bat<Head, Tail> *bat, Tail value);

    template<class Head, class Tail>
    int selectPKId(Bat<Head, Tail> *bat, Head batId);

    template<class Head, class Tail>
    bool dataAlreadyExists(Bat<Head, Tail> *bat, const char* name_value);

    MetaRepositoryManager();
    virtual ~MetaRepositoryManager();

public:

    class TablesIterator : public BatIterator<oid_t, const char*> {
        typedef BatIterator<oid_t, unsigned> table_key_iter_t;
        typedef BatIterator<oid_t, const char*> table_name_iter_t;

        table_key_iter_t *pKeyIter;
        table_name_iter_t *pNameIter;

    public:

        TablesIterator() {
            pKeyIter = MetaRepositoryManager::instance->pk_table_id->begin();
            pNameIter = MetaRepositoryManager::instance->table_name->begin();
        }

        ~TablesIterator() {
            delete pKeyIter;
            delete pNameIter;
        }

        virtual pair<unsigned, const char*> next() override {
            return make_pair(pKeyIter->next().second, pNameIter->next().second);
        }

        virtual pair<unsigned, const char*> get(unsigned index) override {
            return make_pair(pKeyIter->get(index).second, pNameIter->get(index).second);
        }

        virtual bool hasNext() override {
            return pKeyIter->hasNext();
        }

        virtual size_t size() override {
            return pKeyIter->size();
        }

        virtual size_t consumption() override {
            return pKeyIter->consumption() + pNameIter->consumption();
        }

        friend class MetaRepositoryManager;
    };

    TablesIterator* listTables() {
        return new TablesIterator;
    }
};

#include "meta_repository/MetaRepositoryManager.tcc"

#endif

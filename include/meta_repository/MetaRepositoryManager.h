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
    id_bat_t *pk_table_id;
    cstr_bat_t *table_name;

    // all attributes for the attribute table
    id_bat_t *pk_attribute_id;
    cstr_bat_t *attribute_name;
    id_bat_t *fk_table_id;
    id_bat_t *fk_type_id;
    id_bat_t *BAT_number;

    // all attributes for the layout table
    id_bat_t *pk_layout_id;
    cstr_bat_t *layout_name;
    size_bat_t *size;

    // all attributes for the operator table
    id_bat_t *pk_operator_id;
    cstr_bat_t *operator_name;

    // all attributes for the datatypes table
    id_bat_t *pk_datatype_id;
    cstr_bat_t *datatype_name;
    size_bat_t *datatype_length;
    char_bat_t *datatype_category;

    // path where all table files are located
    char* META_PATH;
    char* TEST_DATABASE_PATH;

    void createRepository();

    void createDefaultDataTypes();

    template<class Head, class Tail>
    pair<typename Head::type_t, typename Tail::type_t> getLastValue(Bat<Head, Tail> *bat);

    template<class Head, class Tail>
    pair<typename Head::type_t, typename Tail::type_t> unique_selection(Bat<Head, Tail> *bat, typename Tail::type_t value);

    template<class Head, class Tail>
    bool isBatEmpty(Bat<Head, Tail> *bat);

    template<class Head, class Tail>
    id_t selectBatId(Bat<Head, Tail> *bat, cstr_t value);

    template<class Head, class Tail>
    id_t selectBatId(Bat<Head, Tail> *bat, typename Tail::type_t value);

    template<class Head, class Tail>
    typename Head::type_t selection(Bat<Head, Tail> *bat, typename Tail::type_t value);

    template<class Head, class Tail>
    id_t selectPKId(Bat<Head, Tail> *bat, typename Head::type_t batId);

    template<class Head, class Tail>
    bool dataAlreadyExists(Bat<Head, Tail> *bat, cstr_t name_value);

    MetaRepositoryManager();
    virtual ~MetaRepositoryManager();

public:

    class TablesIterator : public BatIterator<v2_id_t, v2_cstr_t> {
        typedef BatIterator<v2_void_t, v2_id_t> table_key_iter_t;
        typedef BatIterator<v2_void_t, v2_cstr_t> table_name_iter_t;

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

        virtual void next() override {
            pKeyIter->next();
            pNameIter->next();
        }

        virtual TablesIterator& operator++() override {
            next();
            return *this;
        }

        virtual TablesIterator& operator++(int) override {
            next();
            return *this;
        }

        virtual void position(oid_t index) override {
            pKeyIter->position(index);
            pNameIter->position(index);
        }

        virtual bool hasNext() override {
            return pKeyIter->hasNext();
        }

        virtual id_t head() override {
            return pKeyIter->tail();
        }

        virtual cstr_t tail() override {
            return pNameIter->tail();
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

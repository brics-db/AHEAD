#include <cstring>

#include "ColumnStore.h"
#include "column_storage/ColumnBat.h"
#include "column_storage/TempBat.h"
#include "column_operators/operators.h"

#ifndef METAREPOSITORYMANAGER_H
#define METAREPOSITORYMANAGER_H

using namespace std;

enum type_t {
    type_int = 0, type_str, type_fxd, type_chr, type_resint
};

typedef int int_t;
typedef char char_t, *str_t;
typedef double fxd_t;
typedef uint64_t resint_t;

extern const char* NAME_INTEGER;
extern const char* NAME_STRING;
extern const char* NAME_FIXED;
extern const char* NAME_CHAR;
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
    int createTable(const char *name);

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
    int selectPKId(Bat<Head, Tail> *bat, int batId);

    template<class Head, class Tail>
    bool dataAlreadyExists(Bat<Head, Tail> *bat, const char* name_value);

    MetaRepositoryManager();
    virtual ~MetaRepositoryManager();

public:

    class TablesIterator : public BatIterator<unsigned, const char*> {
        typedef BatIterator<unsigned, unsigned> table_key_iter_t;
        typedef BatIterator<unsigned, const char*> table_name_iter_t;

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

        virtual unsigned size() override {
            return pKeyIter->size();
        }


        friend class MetaRepositoryManager;
    };

    TablesIterator listTables() {
        TablesIterator iter;
        return iter;
    }
};

#include "meta_repository/MetaRepositoryManager.tcc"

#endif

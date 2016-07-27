/**
 * @file column_table_manager.h
 * @date Mar 16, 2011
 * @author Hannes Rauhe
 *
 */

#ifndef COLUMN_TABLE_MANAGER_H_
#define COLUMN_TABLE_MANAGER_H_

#include "column_storage/column_table.h"
#include <map>

/**
 * NOT threadsafe (singleton)!
 */
class column_table_manager {
public:
    std::string database_path;
    static column_table_manager* getInstance();

    void loadTable(const std::string & name);
    column_table& getTable(std::string name);
private:
    static column_table_manager *instance;

    std::map<std::string,column_table*> tables;
    column_table_manager() : database_path("/home/c5148079/workspace/V2/database/") {};
    ~column_table_manager();
};

#endif /* COLUMN_TABLE_MANAGER_H_ */

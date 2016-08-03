#include "column_storage/TransactionManager.h"

using namespace std;

TransactionManager* TransactionManager::instance = 0;

const unsigned TransactionManager::Transaction::ID_BAT_COLNAMES = 0;
const unsigned TransactionManager::Transaction::ID_BAT_COLTYPES = 1;
const unsigned TransactionManager::Transaction::ID_BAT_COLIDENT = 2;
const unsigned TransactionManager::Transaction::ID_BAT_FIRST_USER = 3;

TransactionManager::TransactionManager() {
    this->currentVersion = 0;
}

TransactionManager::~TransactionManager() {
}

TransactionManager* TransactionManager::getInstance() {
    if (TransactionManager::instance == 0) {
        TransactionManager::instance = new TransactionManager();
    }

    return TransactionManager::instance;
}

TransactionManager::Transaction* TransactionManager::beginTransaction(bool isUpdater) {
    if (isUpdater) {
        // Atomic Block Start
        set<Transaction*>::iterator it;

        for (it = this->transactions.begin(); it != this->transactions.end(); it++) {
            if ((*it)->isUpdater) {
                // Problem : Another Active Updater
                return nullptr;
            }
        }
        // Atomic Block Ende
    }

    auto result = this->transactions.insert(new TransactionManager::Transaction(isUpdater, this->currentVersion));
    return result.second ? *result.first : nullptr;
}

void TransactionManager::endTransaction(TransactionManager::Transaction *transaction) {
    if (transaction->isUpdater) {
        *transaction->eotVersion = this->currentVersion + 1;
        this->currentVersion++;
    }

    // Atomic Block Start
    this->transactions.erase(this->transactions.find(transaction));
    // Atomic Block Ende

    delete transaction;
}

void TransactionManager::rollbackTransaction(TransactionManager::Transaction *transaction) {
    if (transaction->isUpdater) {
        transaction->rollback();
    }

    // Atomic Block Start
    this->transactions.erase(this->transactions.find(transaction));
    // Atomic Block Ende

    delete transaction;
}

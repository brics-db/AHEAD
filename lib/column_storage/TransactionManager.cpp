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

#include "column_storage/TransactionManager.h"

using namespace std;

TransactionManager* TransactionManager::instance = 0;

const id_t TransactionManager::Transaction::ID_BAT_COLNAMES = 0;
const id_t TransactionManager::Transaction::ID_BAT_COLTYPES = 1;
const id_t TransactionManager::Transaction::ID_BAT_COLIDENT = 2;
const id_t TransactionManager::Transaction::ID_BAT_FIRST_USER = 3;

TransactionManager*
TransactionManager::getInstance() {
    if (TransactionManager::instance == 0) {
        TransactionManager::instance = new TransactionManager();
    }

    return TransactionManager::instance;
}

void TransactionManager::destroyInstance() {
    if (TransactionManager::instance) {
        delete TransactionManager::instance;
        TransactionManager::instance = nullptr;
    }
}

TransactionManager::TransactionManager()
        : currentVersion(0), transactions() {
}

TransactionManager::TransactionManager(const TransactionManager& copy)
        : currentVersion(copy.currentVersion), transactions(copy.transactions) {
}

TransactionManager::~TransactionManager() {
    for (auto pT : transactions) {
        delete pT;
    }
    transactions.clear();
    BucketManager::destroyInstance();
    ColumnManager::destroyInstance();
    MetaRepositoryManager::destroyInstance();
}

TransactionManager::Transaction*
TransactionManager::beginTransaction(bool isUpdater) {
    if (isUpdater) {
        // Atomic Block Start
        for (auto pT : this->transactions) {
            if (pT->isUpdater) {
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

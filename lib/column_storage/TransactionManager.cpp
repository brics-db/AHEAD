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

#include <column_storage/TransactionManager.h>
#include "../meta_repository/MetaRepositoryManager.h"

namespace ahead {

    std::shared_ptr<TransactionManager> TransactionManager::instance;

    std::shared_ptr<TransactionManager> TransactionManager::getInstance() {
        if (!TransactionManager::instance) {
            TransactionManager::instance.reset(new TransactionManager());
        }

        return TransactionManager::instance;
    }

    void TransactionManager::destroyInstance() {
        if (TransactionManager::instance) {
            TransactionManager::instance.reset();
        }
    }

    TransactionManager::TransactionManager()
            : currentVersion(0),
              transactions() {
    }

    TransactionManager::TransactionManager(
            const TransactionManager& copy)
            : currentVersion(copy.currentVersion),
              transactions(copy.transactions) {
    }

    TransactionManager::~TransactionManager() {
        for (auto pT : transactions) {
            pT.reset();
        }
        transactions.clear();
        BucketManager::destroyInstance();
        ColumnManager::destroyInstance();
        MetaRepositoryManager::destroyInstance();
    }

    std::shared_ptr<TransactionManager::Transaction> TransactionManager::beginTransaction(
            bool isUpdater) {
        if (isUpdater) {
            // Atomic Block Start
            for (auto pT : this->transactions) {
                if (pT->isUpdater) {
                    // Problem : Another Active Updater
                    return std::shared_ptr<TransactionManager::Transaction>(nullptr);
                }
            }
            // Atomic Block Ende
        }
        auto result = this->transactions.insert(std::shared_ptr<TransactionManager::Transaction>(new TransactionManager::Transaction(isUpdater, this->currentVersion)));
        return result.second ? (*result.first) : (std::shared_ptr<TransactionManager::Transaction>(nullptr));
    }

    void TransactionManager::endTransaction(
            std::shared_ptr<TransactionManager::Transaction> && transaction) {
        if (transaction->isUpdater) {
            transaction->eotVersion = this->currentVersion + 1;
            this->currentVersion++;
        }

        // Atomic Block Start
        this->transactions.erase(this->transactions.find(transaction));
        // Atomic Block Ende
    }

    void TransactionManager::rollbackTransaction(
            std::shared_ptr<TransactionManager::Transaction> && transaction) {
        if (transaction->isUpdater) {
            transaction->rollback();
        }

        // Atomic Block Start
        this->transactions.erase(this->transactions.find(transaction));
        // Atomic Block Ende
    }

}

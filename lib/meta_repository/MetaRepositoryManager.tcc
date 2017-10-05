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
 * File:   MetaRepositoryManager.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 27. Juli 2016, 18:42
 */

#ifndef METAREPOSITORYMANAGER_H
#error "This file must only be included by MetaRepositoryManager.h!"
#endif

namespace ahead {

    template<class Head, class Tail>
    std::pair<typename Head::type_t, typename Tail::type_t> MetaRepositoryManager::getLastValue(
            BAT<Head, Tail> *bat) {
        auto *iter = bat->begin();
        if (iter->hasNext() && bat->size()) {
            iter->position(bat->size() - 1);
            auto h = iter->head();
            auto t = iter->tail();
            delete iter;
            return std::make_pair(h, t);
        } else {
            delete iter;
            return std::make_pair(-1, -1);
        }
    }

    template<class Head, class Tail>
    std::pair<typename Head::type_t, typename Tail::type_t> MetaRepositoryManager::unique_selection(
            BAT<Head, Tail> *bat,
            typename Tail::type_t value) {
        auto iter = bat->begin();
        for (; iter->hasNext(); ++*iter) {
            auto t = iter->tail();
            if (t == value) {
                auto h = iter->head();
                delete iter;
                return std::make_pair(h, t);
            }
        }
        delete iter;
        return std::make_pair(0, 0);
    }

    template<class Head, class Tail>
    bool MetaRepositoryManager::isBatEmpty(
            BAT<Head, Tail> *bat) {
        return bat->size() == 0;
    }

    template<class Head, class Tail>
    id_t MetaRepositoryManager::selectBatId(
            BAT<Head, Tail> *bat,
            cstr_t value) {
        auto iter = bat->begin();
        for (; iter->hasNext(); ++*iter) {
            if (strcmp(iter->tail(), value) == 0) {
                auto id = iter->head();
                delete iter;
                return id;
            }
        }
        delete iter;
        return ID_INVALID;
    }

    template<class Head, class Tail>
    id_t MetaRepositoryManager::selectBatId(
            BAT<Head, Tail> *bat,
            typename Tail::type_t value) {
        auto iter = bat->begin();
        for (; iter->hasNext(); ++*iter) {
            if (iter->tail() == value) {
                auto id = iter->head();
                delete iter;
                return id;
            }
        }
        delete iter;
        return ID_INVALID;
    }

    template<class Head, class Tail>
    typename Head::type_t MetaRepositoryManager::selection(
            BAT<Head, Tail> *bat,
            typename Tail::type_t value) {
        auto iter = bat->begin();
        for (; iter->hasNext(); ++*iter) {
            if (iter->tail() == value) {
                auto h = iter->head();
                delete iter;
                return h;
            }
        }
        delete iter;
        return typename Head::type_t(0);
    }

    template<class Head, class Tail>
    id_t MetaRepositoryManager::selectPKId(
            BAT<Head, Tail> *bat,
            typename Head::type_t batId) {
        auto iter = bat->begin();
        for (; iter->hasNext(); ++*iter) {
            if (iter->head() == batId) {
                auto t = iter->tail();
                delete iter;
                return t;
            }
        }
        delete iter;
        return ID_INVALID;
    }

    template<class Head, class Tail>
    bool MetaRepositoryManager::dataAlreadyExists(
            BAT<Head, Tail> *bat,
            cstr_t name_value) {
        auto iter = bat->begin();
        for (; iter->hasNext(); ++*iter) {
            if (strcmp(iter->tail(), name_value) == 0) {
                delete iter;
                return true;
            }
        }
        delete iter;
        return false;
    }

    template<typename TargetHead, typename TargetTail, typename Head1, typename Tail1, typename Head2, typename Tail2>
    TempBAT<TargetHead, TargetTail> *
    skeletonJoin(
            BAT<Head1, Tail1> * arg1,
            BAT<Head2, Tail2> * arg2) {
        typedef TempBAT<TargetHead, TargetTail> bat_t;
        typedef typename bat_t::coldesc_head_t coldesc_head_t;
        typedef typename bat_t::coldesc_tail_t coldesc_tail_t;
        auto * result = new bat_t(coldesc_head_t(arg1->head.metaData), coldesc_tail_t(arg2->tail.metaData));
        result->head.metaData.width = size_bytes<typename TargetHead::type_t>;
        result->tail.metaData.width = size_bytes<typename TargetTail::type_t>;
        return result;
    }

    template<typename Head1, typename Tail1, typename Head2, typename Tail2>
    TempBAT<Head1, Tail2> * MetaRepositoryManager::nestedLoopJoin(
            BAT<Head1, Tail1> * bat1,
            BAT<Head2, Tail2> * bat2) {
        auto * result = skeletonJoin<typename Head1::v2_select_t, typename Tail2::v2_select_t>(bat1, bat2);
        auto * iter1 = bat1->begin();
        auto eq_op = ahead::eq<>();
        for (; iter1->hasNext(); ++*iter1) {
            auto * iter2 = bat2->begin();
            for (; iter2->hasNext(); ++*iter2) {
                if (eq_op(iter1->tail(), iter2->head())) {
                    result->append(std::make_pair(iter1->head(), iter2->tail()));
                }
            }
            delete iter2;
        }
        delete iter1;
        return result;
    }

}


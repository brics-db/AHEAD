/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   MetaRepositoryManager.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 27. Juli 2016, 18:42
 */

#ifndef METAREPOSITORYMANAGER_TCC
#define METAREPOSITORYMANAGER_TCC

template<class Head, class Tail>
pair<typename Head::type_t, typename Tail::type_t> MetaRepositoryManager::getLastValue(Bat<Head, Tail> *bat) {
    auto *iter = bat->begin();
    if (iter->hasNext() && bat->size()) {
        iter->position(bat->size() - 1);
        auto h = iter->head();
        auto t = iter->tail();
        delete iter;
        return make_pair(h, t);
    } else {
        return make_pair(-1, -1);
    }
}

template<class Head, class Tail>
pair<typename Head::type_t, typename Tail::type_t> MetaRepositoryManager::unique_selection(Bat<Head, Tail> *bat, typename Tail::type_t value) {
    auto iter = bat->begin();
    for (; iter->hasNext(); ++*iter) {
        if (iter->tail() == value) {
            auto h = iter->head();
            auto t = iter->tail();
            delete iter;
            return make_pair(h, t);
        }
    }
    delete iter;
    return make_pair(0, 0);
}

template<class Head, class Tail>
bool MetaRepositoryManager::isBatEmpty(Bat<Head, Tail> *bat) {
    return bat->size() == 0;
}

template<class Head, class Tail>
id_t MetaRepositoryManager::selectBatId(Bat<Head, Tail> *bat, cstr_t value) {
    auto iter = bat->begin();
    size_t len = strnlen(value, 256);
    for (; iter->hasNext(); ++*iter) {
        if (strncmp(iter->tail(), value, len) == 0) {
            auto id = iter->head();
            delete iter;
            return id;
        }
    }
    delete iter;
    return ID_INVALID;
}

template<class Head, class Tail>
id_t MetaRepositoryManager::selectBatId(Bat<Head, Tail> *bat, typename Tail::type_t value) {
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
typename Head::type_t MetaRepositoryManager::selection(Bat<Head, Tail> *bat, typename Tail::type_t value) {
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
id_t MetaRepositoryManager::selectPKId(Bat<Head, Tail> *bat, typename Head::type_t batId) {
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
bool MetaRepositoryManager::dataAlreadyExists(Bat<Head, Tail> *bat, cstr_t name_value) {
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

#endif /* METAREPOSITORYMANAGER_TCC */


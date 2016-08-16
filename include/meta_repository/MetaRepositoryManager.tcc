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
    auto value = iter->get(bat->size() - 1);
    delete iter;
    return value;
}

template<class Head, class Tail>
pair<typename Head::type_t, typename Tail::type_t> MetaRepositoryManager::unique_selection(Bat<Head, Tail> *bat, typename Tail::type_t value) {
    auto iter = bat->begin();
    while (iter->hasNext()) {
        auto p = iter->next();
        if (p.second == value) {
            delete iter;
            return p;
        }
    }
    delete iter;
    return pair<typename Head::type_t, typename Tail::type_t > (0, 0);
}

template<class Head, class Tail>
bool MetaRepositoryManager::isBatEmpty(Bat<Head, Tail> *bat) {
    if (bat->size() == 0) {
        return true;
    }
    return false;
}

template<class Head, class Tail>
id_t MetaRepositoryManager::selectBatId(Bat<Head, Tail> *bat, cstr_t value) {
    auto iter = bat->begin();
    size_t len = strnlen(value, 256);
    while (iter->hasNext()) {
        auto p = iter->next();
        if (strncmp(p.second, value, len) == 0) {
            delete iter;
            return p.first;
        }
    }
    delete iter;
    return ID_INVALID;
}

template<class Head, class Tail>
id_t MetaRepositoryManager::selectBatId(Bat<Head, Tail> *bat, typename Tail::type_t value) {
    auto iter = bat->begin();
    while (iter->hasNext()) {
        auto p = iter->next();
        if (p.second == value) {
            delete iter;
            return p.first;
        }
    }
    delete iter;
    return ID_INVALID;
}

template<class Head, class Tail>
typename Head::type_t MetaRepositoryManager::selection(Bat<Head, Tail> *bat, typename Tail::type_t value) {
    auto iter = bat->begin();
    while (iter->hasNext()) {
        auto p = iter->next();
        if (p.second == value) {
            delete iter;
            return p.first;
        }
    }
    delete iter;
    return typename Head::type_t(0);
}

template<class Head, class Tail>
id_t MetaRepositoryManager::selectPKId(Bat<Head, Tail> *bat, typename Head::type_t batId) {
    auto iter = bat->begin();
    while (iter->hasNext()) {
        auto p = iter->next();
        if (p.first == batId) {
            delete iter;
            return p.second;
        }
    }
    delete iter;
    return ID_INVALID;
}

template<class Head, class Tail>
bool MetaRepositoryManager::dataAlreadyExists(Bat<Head, Tail> *bat, cstr_t name_value) {
    auto iter = bat->begin();
    while (iter->hasNext()) {
        auto p = iter->next();
        if (strcmp(p.second, name_value) == 0) {
            delete iter;
            return true;
        }
    }
    delete iter;
    return false;
}

#endif /* METAREPOSITORYMANAGER_TCC */


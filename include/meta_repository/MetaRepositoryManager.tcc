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
    iter->position(bat->size() - 1);
    auto value = make_pair(iter->head(), iter->tail());
    delete iter;
    return value;
}

template<class Head, class Tail>
pair<typename Head::type_t, typename Tail::type_t> MetaRepositoryManager::unique_selection(Bat<Head, Tail> *bat, typename Tail::type_t value) {
    auto iter = bat->begin();
    while (iter->hasNext()) {
        iter->next();
        if (iter->tail() == value) {
            auto p = make_pair(iter->head(), iter->tail());
            delete iter;
            return p;
        }
    }
    delete iter;
    return pair<typename Head::type_t, typename Tail::type_t > (0, 0);
}

template<class Head, class Tail>
bool MetaRepositoryManager::isBatEmpty(Bat<Head, Tail> *bat) {
    return bat->size() == 0;
}

template<class Head, class Tail>
id_t MetaRepositoryManager::selectBatId(Bat<Head, Tail> *bat, cstr_t value) {
    auto iter = bat->begin();
    size_t len = strnlen(value, 256);
    while (iter->hasNext()) {
        iter->next();
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
    while (iter->hasNext()) {
        iter->next();
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
    while (iter->hasNext()) {
        iter->next();
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
    while (iter->hasNext()) {
        iter->next();
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
    while (iter->hasNext()) {
        iter->next();
        if (strcmp(iter->tail(), name_value) == 0) {
            delete iter;
            return true;
        }
    }
    delete iter;
    return false;
}

#endif /* METAREPOSITORYMANAGER_TCC */


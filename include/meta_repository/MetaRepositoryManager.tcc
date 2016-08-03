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
pair<Head, Tail> MetaRepositoryManager::getLastValue(Bat<Head, Tail> *bat) {
    BatIterator<unsigned, unsigned> *iter = bat->begin();

    return iter->get(bat->size() - 1);
}

template<class Head, class Tail>
pair<Head, Tail> MetaRepositoryManager::unique_selection(Bat<Head, Tail> *bat, Tail value) {
    BatIterator<Head, Tail> *iter = bat->begin();

    pair<Head, Tail> nullResult;

    while (iter->hasNext()) {
        pair<Head, Tail> p = iter->next();
        if (p.second == value) {
            return p;
        }
    }
    delete(iter);

    return nullResult;
}

template<class Head, class Tail>
bool MetaRepositoryManager::isBatEmpty(Bat<Head, Tail> *bat) {
    if (bat->size() == 0) {
        return true;
    }

    return false;
}

template<class Head, class Tail>
int MetaRepositoryManager::selectBatId(Bat<Head, Tail> *bat, const char *value) {
    BatIterator<Head, Tail> *iter = bat->begin();

    while (iter->hasNext()) {
        pair<Head, Tail> p = iter->next();
        if (strcmp(p.second, value) == 0) {
            delete(iter);
            return p.first;
        }
    }
    delete(iter);

    return -1;
}

template<class Head, class Tail>
int MetaRepositoryManager::selectBatId(Bat<Head, Tail> *bat, int value) {
    BatIterator<Head, Tail> *iter = bat->begin();

    while (iter->hasNext()) {
        pair<Head, Tail> p = iter->next();
        if (p.second == value) {
            delete(iter);
            return p.first;
        }
    }
    delete(iter);

    return -1;
}

template<class Head, class Tail>
Tail MetaRepositoryManager::selection(Bat<Head, Tail> *bat, Tail value) {
    BatIterator<Head, Tail> *iter = bat->begin();

    while (iter->hasNext()) {
        pair<Head, Tail> p = iter->next();
        if (p.second == value) {
            delete(iter);
            return p.first;
        }
    }
    delete(iter);

    return 0;
}

template<class Head, class Tail>
int MetaRepositoryManager::selectPKId(Bat<Head, Tail> *bat, Head batId) {
    BatIterator<Head, Tail> *iter = bat->begin();

    while (iter->hasNext()) {
        pair<Head, Tail> p = iter->next();
        if (p.first == batId) {
            delete(iter);
            return p.second;
        }
    }
    delete(iter);

    return -1;
}

template<class Head, class Tail>
bool MetaRepositoryManager::dataAlreadyExists(Bat<Head, Tail> *bat, const char* name_value) {
    BatIterator<Head, Tail> *iter = bat->begin();

    while (iter->hasNext()) {
        pair<Head, Tail> p = iter->next();
        if (strcmp(p.second, name_value) == 0) {
            delete(iter);
            return true;
        }
    }
    delete(iter);

    return false;
}

#endif /* METAREPOSITORYMANAGER_TCC */


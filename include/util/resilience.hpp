// Copyright (c) 2016 Till Kolditz
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

/* 
 * File:   resilience.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 29. Juli 2016, 12:08
 */

#ifndef RESILIENCE_HPP
#define RESILIENCE_HPP

#include <ColumnStore.h>
#include <column_storage/ColumnBat.h>

typedef ColumnBat<resoid_t, resoid_t> resoid_col_t;
typedef ColumnBat<resoid_t, restiny_t> restiny_col_t;
typedef ColumnBat<resoid_t, resshort_t> resshort_col_t;
typedef ColumnBat<resoid_t, resint_t> resint_col_t;

typedef Bat<resoid_t, resoid_t> resoid_bat_t;
typedef Bat<resoid_t, restiny_t> restiny_bat_t;
typedef Bat<resoid_t, resshort_t> resshort_bat_t;
typedef Bat<resoid_t, resint_t> resint_bat_t;

extern const restiny_t A_TINY_UNENC_MAX;
extern const restiny_t A_TINY_UNENC_MIN;
extern const restiny_t A_TINY_UNENC_MAX_U;
extern const restiny_t A_TINY_UNENC_MIN_U;
extern const restiny_t A_TINY;
extern const restiny_t A_TINY_INV;
// extern const restiny_t A2_TINY;
// extern const restiny_t A2_TINY_INV;

extern const resshort_t A_SHORT_UNENC_MAX;
extern const resshort_t A_SHORT_UNENC_MIN;
extern const resshort_t A_SHORT_UNENC_MAX_U;
extern const resshort_t A_SHORT_UNENC_MIN_U;
extern const resshort_t A_SHORT;
extern const resshort_t A_SHORT_INV;
// extern const resshort_t A2_SHORT;
// extern const resshort_t A2_SHORT_INV;

extern const resint_t A_INT_UNENC_MAX;
extern const resint_t A_INT_UNENC_MIN;
extern const resint_t A_INT_UNENC_MAX_U;
extern const resint_t A_INT_UNENC_MIN_U;
extern const resint_t A_INT;
extern const resint_t A_INT_INV;
// extern const resint_t A2_INT;
// extern const resint_t A2_INT_INV;

template<typename T>
struct TypeSelector;

#define DEFINE_TYPE_SELECTOR_ALIASES(v2Type, resType, colType, resColType)    \
template<>                                                                    \
struct TypeSelector<resType> : public TypeSelector<v2Type> {                  \
};                                                                            \
                                                                              \
template<>                                                                    \
struct TypeSelector<colType> : public TypeSelector<v2Type> {                  \
};                                                                            \
                                                                              \
template<>                                                                    \
struct TypeSelector<resColType> : public TypeSelector<v2Type> {               \
};

template<>
struct TypeSelector<v2_tinyint_t> {
    typedef tinyint_t base_t;
    typedef tinyint_col_t col_t;
    typedef tinyint_bat_t bat_t;
    typedef restiny_t res_t;
    typedef restiny_bat_t res_bat_t;
    typedef restiny_col_t res_col_t;

    static const char* BaseTypeName;
    static const res_t A;
    static const res_t A_INV;
    static const res_t A_UNENC_MAX;
    static const res_t A_UNENC_MAX_U;
};



template<>
struct TypeSelector<v2_shortint_t> {
    typedef shortint_t base_t;
    typedef shortint_bat_t bat_t;
    typedef shortint_col_t col_t;
    typedef resshort_t res_t;
    typedef resshort_bat_t res_bat_t;
    typedef resshort_col_t res_col_t;

    static const char* BaseTypeName;
    static const res_t A;
    static const res_t A_INV;
    static const res_t A_UNENC_MAX;
    static const res_t A_UNENC_MAX_U;
};

template<>
struct TypeSelector<v2_resshort_t> : public TypeSelector<v2_shortint_t> {
};

template<>
struct TypeSelector<v2_int_t> {
    typedef int_t base_t;
    typedef int_bat_t bat_t;
    typedef int_col_t col_t;
    typedef resint_t res_t;
    typedef resint_bat_t res_bat_t;
    typedef resint_col_t res_col_t;

    static const char* BaseTypeName;
    static const res_t A;
    static const res_t A_INV;
    static const res_t A_UNENC_MAX;
    static const res_t A_UNENC_MAX_U;
};

template<>
struct TypeSelector<v2_resint_t> : public TypeSelector<v2_int_t> {
};

template<typename T>
struct TypeName;

template<>
struct TypeName<uint8_t> {
    static const char* NAME;
};

template<>
struct TypeName<uint16_t> {
    static const char* NAME;
};

template<>
struct TypeName<uint32_t> {
    static const char* NAME;
};

template<>
struct TypeName<uint64_t> {
    static const char* NAME;
};

template<typename Tail>
class ColumnBatIterator<resoid_t, Tail> : public ColumnBatIteratorBase<resoid_t, Tail> {
public:
    using ColumnBatIteratorBase<resoid_t, Tail>::ColumnBatIteratorBase;

    virtual ~ColumnBatIterator() {
    }

    virtual pair<resoid_t, Tail> get(unsigned index) override {
        this->bu = this->ta->get(this->mColumnId, index);
        pair<resoid_t, Tail> p = make_pair(std::move(*reinterpret_cast<resoid_t*> (&this->bu->head)), std::move(static_cast<const char*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return p;
    }

    virtual pair<resoid_t, Tail> next() override {
        pair<resoid_t, Tail> p = make_pair(std::move(*reinterpret_cast<resoid_t*> (&this->bu->head)), std::move(static_cast<const char*> (this->bu->tail)));
        delete this->bu;
        this->bu = this->ta->next(this->mColumnId);
        return p;
    }
};

template<typename Tail>
class ColumnBat<resoid_t, Tail> : public Bat<resoid_t, Tail> {
    id_t mColumnId;

public:

    ColumnBat(id_t columnId) : mColumnId(columnId) {
    }

    ColumnBat(const char *table_name, const char *attribute) {
        MetaRepositoryManager *mrm = MetaRepositoryManager::getInstance();
        mColumnId = mrm->getBatIdOfAttribute(table_name, attribute);
    }

    virtual ~ColumnBat() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual BatIterator<resoid_t, Tail> * begin() override {
        return new ColumnBatIterator<resoid_t, Tail>(mColumnId);
    }

    /** append an item */
    virtual void append(pair<resoid_t, Tail>& p) override {
    }

    virtual void append(pair<resoid_t, Tail>&& p) override {
    }

    virtual unsigned size() override {
        auto iter = begin();
        unsigned size = iter->size();
        delete iter;
        return size;
    }

    virtual size_t consumption() override {
        auto iter = begin();
        unsigned size = iter->consumption();
        delete iter;
        return size;
    }
};

#endif /* RESILIENCE_HPP */

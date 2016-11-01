// Copyright (c) 2010 Benjamin Schlegel
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


/***
 * @author Benjamin Schlegel
 */
#ifndef TEMPBAT_H
#define TEMPBAT_H

#include <utility>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBatIterator.h>

template<class Head, class Tail>
class TempBat : public Bat<Head, Tail> {
public:
    typedef Head v2_head_t;
    typedef Tail v2_tail_t;
    typedef typename v2_head_t::type_t head_t;
    typedef typename v2_tail_t::type_t tail_t;
    typedef ColumnDescriptor<v2_head_t, vector<head_t>> coldesc_head_t;
    typedef ColumnDescriptor<v2_tail_t, vector<tail_t>> coldesc_tail_t;

    //    friend class TempBat<Head, v2_void_t>;
    //    friend class TempBat<v2_void_t, Tail>;
    //    friend class TempBat<v2_void_t, v2_void_t>;
    //    template<typename H, typename T> friend class ColumnBat;

private:
    coldesc_head_t head;
    coldesc_tail_t tail;

public:

    /** default constructor */
    TempBat() : head(), tail() {
    }

    /** constructor for n elements */
    TempBat(size_t n) : head(), tail() {
        head.container->reserve(n);
        tail.container->reserve(n);
    }

    TempBat(coldesc_head_t& head, coldesc_tail_t& tail) : head(head), tail(tail) {
    }

    TempBat(coldesc_head_t&& head, coldesc_tail_t&& tail) : head(head), tail(tail) {
    }

    virtual ~TempBat() {
    }

    /** returns an iterator pointing at the start of the column */
    virtual TempBatIterator<Head, Tail> * begin() override {
        return new TempBatIterator<Head, Tail>(head, tail);
    }

    /** append an item */
    virtual void append(pair<head_t, tail_t>& p) override {
        head.container->emplace_back(move(p.first));
        tail.container->emplace_back(move(p.second));
    }

    /** append an item */
    virtual void append(pair<head_t, tail_t>&& p) override {
        head.container->emplace_back(move(p.first));
        tail.container->emplace_back(move(p.second));
    }

    virtual Bat<Tail, Head>* reverse() override {
        return new TempBat<Tail, Head>(this->tail, this->head);
    }

    virtual Bat<Head, Head>* mirror_head() override {
        return new TempBat<Head, Head>(this->head, this->head);
    }

    virtual Bat<Tail, Tail>* mirror_tail() override {
        return new TempBat<Tail, Tail>(this->tail, this->tail);
    }

    virtual unsigned size() override {
        return head.container->size();
    }

    virtual size_t consumption() override {
        return size() * (sizeof (head_t) + sizeof (tail_t));
    }
};

template<>
class TempBat<v2_void_t, v2_void_t> : public Bat<v2_void_t, v2_void_t> {
public:
    typedef v2_void_t v2_head_t;
    typedef v2_void_t v2_tail_t;
    typedef typename v2_void_t::type_t head_t;
    typedef typename v2_void_t::type_t tail_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_head_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_tail_t;

    //    template<typename H, typename T> friend class TempBat;
    //    template<typename H, typename T> friend class ColumnBat;

private:
    coldesc_head_t head;
    coldesc_tail_t tail;
    oid_t count;

public:

    /** default constructor */
    TempBat() : head(), tail(), count(0) {
    }

    virtual ~TempBat() {
    }

    TempBat(coldesc_head_t& head, coldesc_tail_t& tail) : head(head), tail(tail), count(0) {
    }

    TempBat(coldesc_head_t&& head, coldesc_tail_t&& tail) : head(head), tail(tail), count(0) {
    }

    /** constructor for n elements */
    TempBat(size_t n) : head(), tail(), count(n) {
    }

    /** returns an iterator pointing at the start of the column */
    virtual TempBatIterator<v2_void_t, v2_void_t> * begin() override {
        return new TempBatIterator<v2_void_t, v2_void_t>(head, tail, count);
    }

    /** append an item */
    virtual void append(__attribute__((unused)) pair<head_t, tail_t>& p) override {
        ++count;
    }

    /** append an item */
    virtual void append(__attribute__((unused)) pair<head_t, tail_t>&& p) override {
        ++count;
    }

    virtual void append(__attribute__((unused)) tail_t& t) override {
        ++count;
    }

    virtual void append(__attribute__((unused)) tail_t&& t) override {
        ++count;
    }

    virtual Bat<v2_void_t, v2_void_t>* reverse() override {
        return new TempBat<v2_void_t, v2_void_t>(this->tail, this->head);
    }

    virtual Bat<v2_void_t, v2_void_t>* mirror_head() override {
        return new TempBat<v2_void_t, v2_void_t>(this->head, this->head);
    }

    virtual Bat<v2_void_t, v2_void_t>* mirror_tail() override {
        return new TempBat<v2_void_t, v2_void_t>(this->tail, this->tail);
    }

    virtual unsigned size() override {
        return count;
    }

    virtual size_t consumption() override {
        return 0;
    }
};

template<class Head>
class TempBat<Head, v2_void_t> : public Bat<Head, v2_void_t> {
public:
    typedef Head v2_head_t;
    typedef v2_void_t v2_tail_t;
    typedef typename Head::type_t head_t;
    typedef typename v2_void_t::type_t tail_t;
    typedef vector<head_t> containerhead_t;
    typedef ColumnDescriptor<Head, containerhead_t> coldesc_head_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_tail_t;

    template<typename H, typename T> friend class TempBat;
    template<typename H, typename T> friend class ColumnBat;

private:
    coldesc_head_t head;
    coldesc_tail_t tail;

public:

    /** default constructor */
    TempBat() {
    }

    virtual ~TempBat() {
    }

    TempBat(coldesc_head_t& head, coldesc_tail_t& tail) : head(head), tail(tail) {
    }

    TempBat(coldesc_head_t&& head, coldesc_tail_t&& tail) : head(head), tail(tail) {
    }

    /** constructor for n elements */
    TempBat(size_t n) {
        head.container.reserve(n);
    }

    /** returns an iterator pointing at the start of the column */
    virtual TempBatIterator<Head, v2_void_t> * begin() override {
        return new TempBatIterator<Head, v2_void_t>(head, tail);
    }

    /** append an item */
    virtual void append(pair<head_t, tail_t>& p) override {
        if (head.container)
            head.container->emplace_back(std::move(p.first));
    }

    /** append an item */
    virtual void append(pair<head_t, tail_t>&& p) override {
        if (head.container)
            head.container->emplace_back(std::move(p.first));
    }

    virtual void append(head_t& h) {
        head.container->emplace_back(move(h));
    }

    virtual void append(head_t&& h) {
        head.container->emplace_back(move(h));
    }

    virtual Bat<v2_void_t, Head>* reverse() override {
        return new TempBat<v2_void_t, Head>(this->tail, this->head);
    }

    virtual Bat<Head, Head>* mirror_head() override {
        return new TempBat<Head, Head>(this->head, this->head);
    }

    virtual Bat<v2_void_t, v2_void_t>* mirror_tail() override {
        return new TempBat<v2_void_t, v2_void_t>(this->tail, this->tail);
    }

    virtual unsigned size() override {
        return head.container->size();
    }

    virtual size_t consumption() override {
        return size() * sizeof (head_t);
    }
};

template<class Tail>
class TempBat<v2_void_t, Tail> : public Bat<v2_void_t, Tail> {
public:
    typedef v2_void_t v2_head_t;
    typedef Tail v2_tail_t;
    typedef typename v2_void_t::type_t head_t;
    typedef typename Tail::type_t tail_t;
    typedef ColumnDescriptor<v2_void_t, void> coldesc_head_t;
    typedef ColumnDescriptor<Tail, vector<tail_t>> coldesc_tail_t;

    template<typename H, typename T> friend class TempBat;
    template<typename H, typename T> friend class ColumnBat;

private:
    coldesc_head_t head;
    coldesc_tail_t tail;

public:

    /** default constructor */
    TempBat() : head(), tail() {
    }

    virtual ~TempBat() {
    }

    TempBat(coldesc_head_t& head, coldesc_tail_t& tail) : head(head), tail(tail) {
    }

    TempBat(coldesc_head_t&& head, coldesc_tail_t&& tail) : head(head), tail(tail) {
    }

    /** constructor for n elements */
    TempBat(size_t n) : head(), tail() {
        tail.container->reserve(n);
    }

    /** returns an iterator pointing at the start of the column */
    virtual TempBatIterator<v2_void_t, Tail> * begin() override {
        return new TempBatIterator<v2_void_t, Tail>(head, tail);
    }

    /** append an item */
    virtual void append(pair<head_t, tail_t>& p) override {
        tail.container->emplace_back(move(p.second));
    }

    /** append an item */
    virtual void append(pair<head_t, tail_t>&& p) override {
        tail.container->emplace_back(move(p.second));
    }

    virtual void append(tail_t& t) {
        tail.container->emplace_back(move(t));
    }

    virtual void append(tail_t&& t) {
        tail.container->emplace_back(move(t));
    }

    virtual Bat<Tail, v2_void_t>* reverse() override {
        return new TempBat<Tail, v2_void_t>(this->tail, this->head);
    }

    virtual Bat<v2_void_t, v2_void_t>* mirror_head() override {
        return new TempBat<v2_void_t, v2_void_t>(this->head, this->head);
    }

    virtual Bat<Tail, Tail>* mirror_tail() override {
        return new TempBat<Tail, Tail>(this->tail, this->tail);
    }

    virtual unsigned size() override {
        return tail.container->size();
    }

    virtual size_t consumption() override {
        return size() * sizeof (Tail);
    }
};

#endif

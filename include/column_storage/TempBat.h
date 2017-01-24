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


/***
 * @author Benjamin Schlegel
 * 
 */
#ifndef TEMPBAT_H
#define TEMPBAT_H

#include <vector>
#include <utility>

#include <ColumnStore.h>
#include <column_storage/Bat.h>
#include <column_storage/TempBatIterator.h>

template<class Head, class Tail>
class TempBAT : public BAT<Head, Tail> {

public:
    using v2_head_t = typename BAT<Head, Tail>::v2_head_t;
    using v2_tail_t = typename BAT<Head, Tail>::v2_tail_t;
    using head_t = typename BAT<Head, Tail>::head_t;
    using tail_t = typename BAT<Head, Tail>::tail_t;
    using coldesc_head_t = typename BAT<Head, Tail>::coldesc_head_t;
    using coldesc_tail_t = typename BAT<Head, Tail>::coldesc_tail_t;

    /** default constructor */
    TempBAT () : BAT<Head, Tail>() {
    }

    TempBAT (coldesc_head_t head, coldesc_tail_t tail) : BAT<Head, Tail>(std::move (head), std::move (tail)) {
    }

    virtual
    ~TempBAT () {
    }

    virtual void
    reserve (size_t n) {
        this->head.container->reserve(n);
        this->tail.container->reserve(n);
    }

    /** returns an iterator pointing at the start of the column */
    virtual TempBATIterator<Head, Tail> *
    begin () override {
        return new TempBATIterator<Head, Tail>(this->head, this->tail);
    }

    /** append an item */
    virtual void
    append (std::pair<head_t, tail_t>& p) override {
        this->head.container->push_back(p.first);
        this->tail.container->push_back(p.second);
    }

    /** append an item */
    virtual void
    append (std::pair<head_t, tail_t>&& p) override {
        this->head.container->push_back(p.first);
        this->tail.container->push_back(p.second);
    }

    virtual BAT<Tail, Head>*
    reverse () override {
        return new TempBAT<Tail, Head>(this->tail, this->head);
    }

    virtual BAT<Head, Head>*
    mirror_head () override {
        return new TempBAT<Head, Head>(this->head, this->head);
    }

    virtual BAT<Tail, Tail>*
    mirror_tail () override {
        return new TempBAT<Tail, Tail>(this->tail, this->tail);
    }

    virtual unsigned
    size () override {
        return this->head.container->size();
    }

    virtual size_t
    consumption () override {
        return size() * (sizeof (head_t) + sizeof (tail_t));
    }
};

template<>
class TempBAT<v2_void_t, v2_void_t> : public BAT<v2_void_t, v2_void_t> {

public:
    typedef v2_void_t Head, Tail;
    using v2_head_t = typename BAT<Head, Tail>::v2_head_t;
    using v2_tail_t = typename BAT<Head, Tail>::v2_tail_t;
    using head_t = typename BAT<Head, Tail>::head_t;
    using tail_t = typename BAT<Head, Tail>::tail_t;
    using coldesc_head_t = typename BAT<Head, Tail>::coldesc_head_t;
    using coldesc_tail_t = typename BAT<Head, Tail>::coldesc_tail_t;

    oid_t count;

    /** default constructor */
    TempBAT () : BAT<v2_void_t, v2_void_t>(), count (0) {
    }

    TempBAT (coldesc_head_t& head, coldesc_tail_t& tail) : BAT<Head, Tail>(head, tail), count (0) {
    }

    TempBAT (coldesc_head_t&& head, coldesc_tail_t&& tail) : BAT<Head, Tail>(std::forward<coldesc_head_t>(head), std::forward<coldesc_tail_t>(tail)), count (0) {
    }

    virtual
    ~TempBAT () {
    }

    virtual void
    reserve (size_t n) {
        (void)n;
    }

    /** returns an iterator pointing at the start of the column */
    virtual TempBATIterator<Head, Tail> *
    begin () override {
        return new TempBATIterator<Head, Tail>(this->head, this->tail, count);
    }

    /** append an item */
    virtual void
    append (__attribute__ ((unused)) std::pair<head_t, tail_t>& p) override {
        ++count;
    }

    /** append an item */
    virtual void
    append (__attribute__ ((unused)) std::pair<head_t, tail_t>&& p) override {
        ++count;
    }

    virtual BAT<Tail, Head>*
    reverse () override {
        return new TempBAT<Tail, Head>(this->tail, this->head);
    }

    virtual BAT<Head, Head>*
    mirror_head () override {
        return new TempBAT<Head, Head>(this->head, this->head);
    }

    virtual BAT<Tail, Tail>*
    mirror_tail () override {
        return new TempBAT<Tail, Tail>(this->tail, this->tail);
    }

    virtual unsigned
    size () override {
        return count;
    }

    virtual size_t
    consumption () override {
        return 0;
    }
};

template<class Head>
class TempBAT<Head, v2_void_t> : public BAT<Head, v2_void_t> {

public:
    typedef v2_void_t Tail;
    using v2_head_t = typename BAT<Head, Tail>::v2_head_t;
    using v2_tail_t = typename BAT<Head, Tail>::v2_tail_t;
    using head_t = typename BAT<Head, Tail>::head_t;
    using tail_t = typename BAT<Head, Tail>::tail_t;
    using coldesc_head_t = typename BAT<Head, Tail>::coldesc_head_t;
    using coldesc_tail_t = typename BAT<Head, Tail>::coldesc_tail_t;

    /** default constructor */
    TempBAT () {
    }

    TempBAT (coldesc_head_t& head, coldesc_tail_t& tail) : BAT<Head, v2_void_t>(head, tail) {
    }

    TempBAT (coldesc_head_t&& head, coldesc_tail_t&& tail) : BAT<Head, v2_void_t>(std::forward<coldesc_head_t>(head), std::forward<coldesc_tail_t>(tail)) {
    }

    virtual
    ~TempBAT () {
    }

    virtual void
    reserve (size_t n) {
        this->head.container->reserve(n);
    }

    /** returns an iterator pointing at the start of the column */
    virtual TempBATIterator<Head, v2_void_t> *
    begin () override {
        return new TempBATIterator<Head, v2_void_t>(this->head, this->tail);
    }

    /** append an item */
    virtual void
    append (std::pair<head_t, tail_t>& p) override {
        if (this->head.container)
            this->head.container->push_back(p.first);
    }

    /** append an item */
    virtual void
    append (std::pair<head_t, tail_t>&& p) override {
        if (this->head.container)
            this->head.container->push_back(std::move(p.first));
    }

    virtual void
    append (head_t& h) {
        this->head.container->push_back(h);
    }

    virtual void
    append (head_t&& h) {
        this->head.container->push_back(std::move(h));
    }

    virtual BAT<Tail, Head>*
    reverse () override {
        return new TempBAT<Tail, Head>(this->tail, this->head);
    }

    virtual BAT<Head, Head>*
    mirror_head () override {
        return new TempBAT<Head, Head>(this->head, this->head);
    }

    virtual BAT<Tail, Tail>*
    mirror_tail () override {
        return new TempBAT<Tail, Tail>(this->tail, this->tail);
    }

    virtual unsigned
    size () override {
        return this->head.container->size();
    }

    virtual size_t
    consumption () override {
        return size() * sizeof (head_t);
    }
};

template<class Tail>
class TempBAT<v2_void_t, Tail> : public BAT<v2_void_t, Tail> {

public:
    typedef v2_void_t Head;
    using v2_head_t = typename BAT<Head, Tail>::v2_head_t;
    using v2_tail_t = typename BAT<Head, Tail>::v2_tail_t;
    using head_t = typename BAT<Head, Tail>::head_t;
    using tail_t = typename BAT<Head, Tail>::tail_t;
    using coldesc_head_t = typename BAT<Head, Tail>::coldesc_head_t;
    using coldesc_tail_t = typename BAT<Head, Tail>::coldesc_tail_t;

    /** default constructor */
    TempBAT () : BAT<v2_void_t, Tail>() {
    }

    TempBAT (coldesc_head_t& head, coldesc_tail_t& tail) : BAT<v2_void_t, Tail>(head, tail) {
    }

    TempBAT (coldesc_head_t&& head, coldesc_tail_t&& tail) : BAT<v2_void_t, Tail>(std::forward<coldesc_head_t>(head), std::forward<coldesc_tail_t>(tail)) {
    }

    virtual
    ~TempBAT () {
    }

    virtual void
    reserve (size_t n) {
        this->tail.container->reserve(n);
    }

    /** returns an iterator pointing at the start of the column */
    virtual TempBATIterator<v2_void_t, Tail> *
    begin () override {
        return new TempBATIterator<v2_void_t, Tail>(this->head, this->tail);
    }

    /** append an item */
    virtual void
    append (std::pair<head_t, tail_t>& p) override {
        this->tail.container->push_back(p.second);
    }

    /** append an item */
    virtual void
    append (std::pair<head_t, tail_t>&& p) override {
        this->tail.container->push_back(std::move(p.second));
    }

    virtual void
    append (tail_t& t) {
        this->tail.container->push_back(t);
    }

    virtual void
    append (tail_t&& t) {
        this->tail.container->push_back(std::move(t));
    }

    virtual BAT<Tail, Head>*
    reverse () override {
        return new TempBAT<Tail, v2_void_t>(this->tail, this->head);
    }

    virtual BAT<Head, Head>*
    mirror_head () override {
        return new TempBAT<v2_void_t, v2_void_t>(this->head, this->head);
    }

    virtual BAT<Tail, Tail>*
    mirror_tail () override {
        return new TempBAT<Tail, Tail>(this->tail, this->tail);
    }

    virtual unsigned
    size () override {
        return this->tail.container->size();
    }

    virtual size_t
    consumption () override {
        return size() * sizeof (tail_t);
    }
};

#endif

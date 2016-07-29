/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   ColumnStore.h
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 27. Juli 2016, 18:14
 */

#ifndef COLUMNSTORE_H
#define COLUMNSTORE_H

// Fast-Forward Declarations

// column_operators
template<class Head, class Tail>
class BAT;
template<class Head, class Tail>
class BAT_selection;
template<class T>
class col_import;
class Bat_Operators;
template<class T>
class predicate;

// column_storage
template<class Head, class Tail>
struct BUN;
template<class Head, class Tail>
class BAT_base_iterator;
template<class Head, class Tail>
class BAT_normal_iterator;
template<class Head, class Tail>
class BAT_reverse_iterator;
template<class Head, class Tail>
class BAT_mirror_iterator;
template<class Head, class Tail, class iterator_type>
class BAT_base;
template<class Head, class Tail>
class Bat;
template<class Head, class Tail>
class BatIterator;
class BucketManager;
template<class Head, class Tail>
class ColumnBat;
template<class Head, class Tail>
class ColumnBatIterator;
class ColumnManager;
template<class Head, class Tail>
class PartedBat;
template<class Head, class Tail>
class PartedBatIterator;
template<class Head, class Tail>
class TempBat;
template<class Head, class Tail>
class TempBatIterator;
template<class Head, class Tail>
class TempFIFOBat;
template<class Head, class Tail>
class TempListBat;
template<class Head, class Tail>
class TempListBatIterator;
template<class Head, class Tail>
class TempQueue;
template<class Head, class Tail>
class TestBat;
template<class Head, class Tail>
class TestBatIterator;
class TransactionManager;
template<class T>
class Adapter;
template<class T>
class CircularBuffer;
template<class T>
class column;

// meta_repository
class MetaRepositoryManager;

#endif /* COLUMNSTORE_H */


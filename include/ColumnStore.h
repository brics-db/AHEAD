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

// global type definitions

#include <cinttypes>

/***********************************************************/
/* Definition of meta-macro for variable-length macros     */
/***********************************************************/
// get number of arguments with __NARG__
#define __NARG__(...)  __NARG_I_(__VA_ARGS__,__RSEQ_N())
#define __NARG_I_(...) __ARG_N(__VA_ARGS__)
#define __ARG_N( \
      _1, _2, _3, _4, _5, _6, _7, _8, _9,_10, \
     _11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
     _21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
     _31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
     _41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
     _51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
     _61,_62,_63,N,...) N
#define __RSEQ_N() \
     63,62,61,60,                   \
     59,58,57,56,55,54,53,52,51,50, \
     49,48,47,46,45,44,43,42,41,40, \
     39,38,37,36,35,34,33,32,31,30, \
     29,28,27,26,25,24,23,22,21,20, \
     19,18,17,16,15,14,13,12,11,10, \
     9,8,7,6,5,4,3,2,1,0

// general definition for any function name
#define _VFUNC_(name, n) name##n
#define _VFUNC(name, n) _VFUNC_(name, n)
#define VFUNC(func, ...) _VFUNC(func, __NARG__(__VA_ARGS__)) (__VA_ARGS__)

// Example definition for FOO
// #define FOO(...) VFUNC(FOO, __VA_ARGS__)
// #define FOO2(x, y) ((x) + (y))
// #define FOO3(x, y, z) ((x) + (y) + (z))
// it also works with C functions:
// int FOO4(int a, int b, int c, int d) { return a + b + c + d; }

/***********************************************************/


enum type_t {
    type_tinyint = 0, type_shortint, type_int, type_largeint, type_string, type_fixed, type_char, type_restiny, type_resshort, type_resint
};

typedef uint8_t tinyint_t;
typedef uint16_t shortint_t;
typedef uint32_t int_t;
typedef uint64_t largeint_t;
typedef char char_t, *str_t;
typedef double fixed_t;

typedef uint32_t restiny_t;
typedef uint32_t resshort_t;
typedef uint64_t resint_t;

typedef unsigned oid_t;

typedef int_t id_t;

#define OID_INVALID (static_cast<oid_t>(-1))

// Fast-Forward Declarations

// column_operators
template<class Head, class Tail>
class BAT;
template<class Head, class Tail>
class BAT_selection;
template<class T>
class col_import;
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
template<class Tail>
class ColumnBat;
template<class Tail>
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


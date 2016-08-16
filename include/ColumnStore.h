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
#include <utility>
#include <functional>

using namespace std;

#define TOSTRING0(str) #str
#define TOSTRING(str) TOSTRING0(str)

/*
 * Concatenate preprocessor tokens A and B without expanding macro definitions
 * (however, if invoked from a macro, macro arguments are expanded).
 */
#define CONCAT0(A, B) A ## B

/*
 * Concatenate preprocessor tokens A and B after macro-expanding them.
 */
#define CONCAT(A, B) CONCAT0(A, B)

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

/* META MACRO intended for generating type names */
#define MKT(...) VFUNC(MKT, __VA_ARGS__)
#define MKT1(BASE) CONCAT(BASE, _t)
#define MKT2(BASE, SUFFIX) MKT1(CONCAT(BASE, SUFFIX))
#define MKT3(PREFIX, BASE, SUFFIX) CONCAT(PREFIX, CONCAT(BASE, SUFFIX))

enum type_t {
    type_tinyint = 0, type_shortint, type_int, type_largeint, type_string, type_fixed, type_char, type_restiny, type_resshort, type_resint
};

typedef uint8_t tinyint_t;
typedef uint16_t shortint_t;
typedef uint32_t int_t;
typedef uint64_t bigint_t;
typedef char char_t, *str_t;
typedef const char_t *cstr_t;
typedef double fixed_t;
typedef uint16_t restiny_t;
typedef uint32_t resshort_t;
typedef uint64_t resint_t;

typedef unsigned id_t;
typedef unsigned oid_t;
typedef unsigned version_t;

// enforce real different types. The typedef's above result in type-clashes!

struct v2_tinyint_t {
    typedef tinyint_t type_t;
    type_t value;

    v2_tinyint_t(type_t& x) : value(x) {
    }

    v2_tinyint_t(type_t&& x) : value(x) {
    }
};

struct v2_shortint_t {
    typedef shortint_t type_t;
    type_t value;

    v2_shortint_t(type_t& x) : value(x) {
    }

    v2_shortint_t(type_t&& x) : value(x) {
    }
};

struct v2_int_t {
    typedef int_t type_t;
    type_t value;

    v2_int_t(type_t& x) : value(x) {
    }

    v2_int_t(type_t&& x) : value(x) {
    }
};

struct v2_bigint_t {
    typedef bigint_t type_t;
    type_t value;

    v2_bigint_t(type_t& x) : value(x) {
    }

    v2_bigint_t(type_t&& x) : value(x) {
    }
};

struct v2_char_t {
    typedef char_t type_t;
    type_t value;

    v2_char_t(type_t& x) : value(x) {
    }

    v2_char_t(type_t&& x) : value(x) {
    }
};

struct v2_str_t {
    typedef str_t type_t;
    type_t value;

    v2_str_t(type_t& x) : value(x) {
    }

    v2_str_t(type_t&& x) : value(x) {
    }
};

struct v2_cstr_t {
    typedef cstr_t type_t;
    type_t value;

    v2_cstr_t(type_t& x) : value(x) {
    }

    v2_cstr_t(type_t&& x) : value(x) {
    }
};

struct v2_fixed_t {
    typedef fixed_t type_t;
    type_t value;

    v2_fixed_t(type_t& x) : value(x) {
    }

    v2_fixed_t(type_t&& x) : value(x) {
    }
};

struct v2_id_t {
    typedef id_t type_t;
    type_t value;

    v2_id_t(type_t& x) : value(x) {
    }

    v2_id_t(type_t&& x) : value(x) {
    }
};

struct v2_oid_t {
    typedef oid_t type_t;
    type_t value;

    v2_oid_t(type_t& x) : value(x) {
    }

    v2_oid_t(type_t&& x) : value(x) {
    }
};

struct v2_version_t {
    typedef version_t type_t;
    type_t value;

    v2_version_t(type_t& x) : value(x) {
    }

    v2_version_t(type_t&& x) : value(x) {
    }
};

struct v2_size_t {
    typedef size_t type_t;
    type_t value;

    v2_size_t(type_t& x) : value(x) {
    }

    v2_size_t(type_t&& x) : value(x) {
    }
};

#define ID_INVALID (static_cast<id_t>(-1))
#define OID_INVALID (static_cast<oid_t>(-1))
#define VERSION_INVALID (static_cast<version_t>(-1))

// Fast-Forward Declarations

// column_storage
template<typename Head, typename Tail>
class Bat;
template<typename Head, typename Tail>
class BatIterator;
class BucketManager;
template<typename Head, typename Tail>
class ColumnBat;
template<typename Head, typename Tail>
class ColumnBatIterator;
class ColumnManager;
template<class Head, class Tail>
class TempBat;
template<class Head, class Tail>
class TempBatIterator;
class TransactionManager;

// meta_repository
class MetaRepositoryManager;

// Fast-Forward declare Bat, ColumnBat, TempBat types
typedef Bat<v2_oid_t, v2_tinyint_t> tinyint_bat_t;
typedef Bat<v2_oid_t, v2_shortint_t> shortint_bat_t;
typedef Bat<v2_oid_t, v2_int_t> int_bat_t;
typedef Bat<v2_oid_t, v2_bigint_t> bigint_bat_t;
typedef Bat<v2_oid_t, v2_char_t> char_bat_t;
typedef Bat<v2_oid_t, v2_str_t> str_bat_t;
typedef Bat<v2_oid_t, v2_cstr_t> cstr_bat_t;
typedef Bat<v2_oid_t, v2_fixed_t> fixed_bat_t;
typedef Bat<v2_oid_t, v2_oid_t> oid_bat_t;
typedef Bat<v2_oid_t, v2_id_t> id_bat_t;
typedef Bat<v2_oid_t, v2_version_t> version_bat_t;
typedef Bat<v2_oid_t, v2_size_t> size_bat_t;

typedef ColumnBat<v2_oid_t, v2_tinyint_t> tinyint_colbat_t;
typedef ColumnBat<v2_oid_t, v2_shortint_t> shortint_colbat_t;
typedef ColumnBat<v2_oid_t, v2_int_t> int_colbat_t;
typedef ColumnBat<v2_oid_t, v2_bigint_t> bigint_colbat_t;
typedef ColumnBat<v2_oid_t, v2_char_t> char_colbat_t;
typedef ColumnBat<v2_oid_t, v2_str_t> str_colbat_t;
typedef ColumnBat<v2_oid_t, v2_cstr_t> cstr_colbat_t;
typedef ColumnBat<v2_oid_t, v2_fixed_t> fixed_colbat_t;
typedef ColumnBat<v2_oid_t, v2_oid_t> oid_colbat_t;
typedef ColumnBat<v2_oid_t, v2_id_t> id_colbat_t;
typedef ColumnBat<v2_oid_t, v2_version_t> version_colbat_t;
typedef ColumnBat<v2_oid_t, v2_size_t> size_colbat_t;

typedef TempBat<v2_oid_t, v2_tinyint_t> tinyint_tmp_t;
typedef TempBat<v2_oid_t, v2_shortint_t> shortint_tmpbat_t;
typedef TempBat<v2_oid_t, v2_int_t> int_tmpbat_t;
typedef TempBat<v2_oid_t, v2_bigint_t> bigint_tmpbat_t;
typedef TempBat<v2_oid_t, v2_char_t> char_tmpbat_t;
typedef TempBat<v2_oid_t, v2_str_t> str_tmpbat_t;
typedef TempBat<v2_oid_t, v2_cstr_t> cstr_tmpbat_t;
typedef TempBat<v2_oid_t, v2_fixed_t> fixed_tmpbat_t;
typedef TempBat<v2_oid_t, v2_oid_t> oid_tmpbat_t;
typedef TempBat<v2_oid_t, v2_id_t> id_tmpbat_t;
typedef TempBat<v2_oid_t, v2_version_t> version_tmpbat_t;
typedef TempBat<v2_oid_t, v2_size_t> size_tmpbat_t;

#endif /* COLUMNSTORE_H */


/**
 * @file generic_column.h
 * @date Mar 9, 2011
 * @author Hannes Rauhe
 *
 */

#ifndef GENERIC_COLUMN_H_
#define GENERIC_COLUMN_H_

#include <stdexcept>
#include <sstream>
#include "column_storage/column.h"

enum COL_DATA_TYPE {
    INTEGER = 0,
    UINT = 1,
    STRING = 2,
    DOUBLE = 3
};

class generic_column {
private:
    COL_DATA_TYPE c_type;
    std::string c_name;
    column<int> d_int;
    column<unsigned> d_uint;
    column<double> d_double;
    column<std::string> d_string;

public:

    generic_column(std::string t) {
        if (t == "INTEGER") {
            c_type = INTEGER;
        } else if (t == "STRING") {
            c_type = STRING;
        } else if (t == "FIXED:10:0") {
            c_type = DOUBLE;
        } else {
            throw std::invalid_argument((std::string("cannot create column: type ") + t + std::string(" not known")).c_str());
        }
    }

    generic_column(COL_DATA_TYPE t) : c_type(t) {
    }

    column<int>& getIntCol() {
        if (c_type != INTEGER)
            throw std::invalid_argument("Requested column is not of type INTEGER");
        return d_int;
    }

    column<std::string>& getStringCol() {
        if (c_type != STRING)
            throw std::invalid_argument("Requested column is not of type INTEGER");
        return d_string;
    }

    void setName(std::string newname) {
        c_name = newname;
    }

    std::string getName() const {
        return c_name;
    }

    void push_back(std::string& cell) {
        std::stringstream cellStream(cell, std::ios_base::in);
        switch (c_type) {
            case INTEGER:
            {
                int n = 0;
                cellStream>>n;
                d_int.push_back(n);
                break;
            }
            case UINT:
            {
                unsigned n = 0;
                cellStream>>n;
                d_uint.push_back(n);
                break;
            }
            case DOUBLE:
            {
                double n = 0;
                cellStream>>n;
                d_double.push_back(n);
                break;
            }
            case STRING:
            {
                d_string.push_back(cell);
                break;
            }
        }
    }

    void create_partition(unsigned part_size_reserve = 0) {
        switch (c_type) {
            case INTEGER:
            {
                d_int.create_partition(part_size_reserve);
                break;
            }
            case UINT:
            {
                d_uint.create_partition(part_size_reserve);
                break;
            }
            case DOUBLE:
            {
                d_double.create_partition(part_size_reserve);
                break;
            }
            case STRING:
            {
                d_string.create_partition(part_size_reserve);
                break;
            }
        }
    }

    void print_val(std::ostream &os, unsigned index) {
        switch (c_type) {
            case INTEGER:
            {
                os << d_int[index];
                break;
            }
            case UINT:
            {
                os << d_uint[index];
                break;
            }
            case DOUBLE:
            {
                os << d_double[index];
                break;
            }
            case STRING:
            {
                os << d_string[index];
                break;
            }
        }
    }

};

#endif /* GENERIC_COLUMN_H_ */

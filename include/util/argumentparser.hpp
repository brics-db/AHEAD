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
 * File:   argumentparser.hpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 23. November 2016, 02:16
 */

#ifndef ARGUMENTPARSER_HPP
#define ARGUMENTPARSER_HPP

#include <utility>
#include <unordered_map>
#include <sstream>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>

class ArgumentParser {

public:
    typedef std::vector<std::string> alias_list_t;
    typedef std::vector<std::tuple<std::string, alias_list_t, size_t>> uint_args_t;
    typedef std::vector<std::tuple<std::string, alias_list_t, std::string>> str_args_t;
    typedef std::vector<std::tuple<std::string, alias_list_t, bool>> bool_args_t;

private:

    enum argtype_t {

        argint, argstr, argbool
    };

    uint_args_t uintArgs;
    str_args_t strArgs;
    bool_args_t boolArgs;
    std::unordered_map<std::string, argtype_t> argTypes; // we know what we do

public:

    ArgumentParser ();

    ArgumentParser (const uint_args_t & uintArgs, const str_args_t & strArgs, const bool_args_t & boolArgs);

    ArgumentParser (const uint_args_t && uintArgs, const str_args_t && strArgs, const bool_args_t && boolArgs);

    virtual ~ArgumentParser ();

    ArgumentParser& operator= (const ArgumentParser & other);

private:

    size_t parseint (const std::string& name, char* arg);

    size_t parsestr (const std::string& name, char* arg);

    size_t parsebool (const std::string& name, __attribute__ ((unused)) char* arg);

public:

    void parse (int argc, char** argv, size_t offset); // no C++17 (array_view), yet :-(

    size_t get_uint (const std::string & name);

    const std::string & get_str (const std::string & name);

    bool get_bool (const std::string & name);
};

#endif /* ARGUMENTPARSER_HPP */

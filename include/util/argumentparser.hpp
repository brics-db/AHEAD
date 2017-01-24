// Copyright (c) 2016 Till Kolditz
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

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
 * File:   argumentparser.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 23. November 2016, 02:16
 */

#ifdef DEBUG
#include <iostream>
#endif

#include <util/argumentparser.hpp>

ArgumentParser::ArgumentParser () : uintArgs (), strArgs (), boolArgs (), argTypes () {
#ifdef DEBUG
    std::cout << "ArgumentParser()" << std::endl;
#endif
}

ArgumentParser::ArgumentParser (const uint_args_t & uintArgs, const str_args_t & strArgs, const bool_args_t & boolArgs) : uintArgs (uintArgs), strArgs (strArgs), boolArgs (boolArgs), argTypes (uintArgs.size () + strArgs.size () + boolArgs.size ()) {
#ifdef DEBUG
    std::cout << "ArgumentParser(const &, const &, const &)" << std::endl;
#endif
    for (auto a : this->uintArgs) {
        for (auto s : std::get<1>(a)) {
            argTypes[s] = argint;
        }
    }
    for (auto a : this->strArgs) {
        for (auto s : std::get<1>(a)) {
            argTypes[s] = argstr;
        }
    }
    for (auto a : this->boolArgs) {
        for (auto s : std::get<1>(a)) {
            argTypes[s] = argbool;
            if (boost::starts_with(s, "--")) {
                argTypes["--no-" + s.substr(2)] = argbool;
            } else if (boost::starts_with(s, "-")) {
                argTypes["-no-" + s.substr(1)] = argbool;
            }
        }
    }
}

ArgumentParser::ArgumentParser (const uint_args_t && uintArgs, const str_args_t && strArgs, const bool_args_t && boolArgs) : uintArgs (uintArgs), strArgs (strArgs), boolArgs (boolArgs), argTypes (uintArgs.size () + strArgs.size () + boolArgs.size ()) {
#ifdef DEBUG
    std::cout << "ArgumentParser(const &&, const &&, const &&)" << std::endl;
#endif
    for (auto a : this->uintArgs) {
        for (auto s : std::get<1>(a)) {
            argTypes[s] = argint;
        }
    }
    for (auto a : this->strArgs) {
        for (auto s : std::get<1>(a)) {
            argTypes[s] = argstr;
        }
    }
    for (auto a : this->boolArgs) {
        for (auto s : std::get<1>(a)) {
            argTypes[s] = argbool;
            if (boost::starts_with(s, "--")) {
                argTypes["--no-" + s.substr(2)] = argbool;
            } else if (boost::starts_with(s, "-")) {
                argTypes["-no-" + s.substr(1)] = argbool;
            }
        }
    }
}

ArgumentParser::~ArgumentParser () {
}

ArgumentParser& ArgumentParser::operator= (const ArgumentParser & other) {
#ifdef DEBUG
    std::cout << "ArgumentParser& operator=(const ArgumentParser & other)" << std::endl;
#endif
    uintArgs.clear();
    strArgs.clear();
    boolArgs.clear();
    argTypes.clear();
    uintArgs.insert(uintArgs.begin(), other.uintArgs.begin(), other.uintArgs.end());
    strArgs.insert(strArgs.begin(), other.strArgs.begin(), other.strArgs.end());
    boolArgs.insert(boolArgs.begin(), other.boolArgs.begin(), other.boolArgs.end());
    argTypes.insert(other.argTypes.begin(), other.argTypes.end());
    return *this;
}

size_t
ArgumentParser::parseint (const std::string& name, char* arg) {
    if (arg == nullptr) {
        std::stringstream ss;
        ss << "Required value for parameter \"" << name << "\" missing! (on line " << __LINE__ << ')';
        throw std::runtime_error(ss.str());
    }
    std::string str(arg);
    size_t idx = std::string::npos;
    size_t value;
    try {
        value = std::stoul(str, &idx);
    } catch (std::invalid_argument& exc) {
        std::stringstream ss;
        ss << "Value for parameter \"" << name << "\" is not an integer (is \"" << str << "\")! (on line " << __LINE__ << ')';
        throw std::runtime_error(ss.str());
    }
    if (idx < str.length()) {
        std::stringstream ss;
        ss << "Value for parameter \"" << name << "\" is not an integer (is \"" << str << "\")! (on line " << __LINE__ << ')';
        throw std::runtime_error(ss.str());
    }
    for (auto & tup : uintArgs) {
        for (auto & alias : std::get<1>(tup)) {
            if (name.compare(alias) == 0) {
                std::get<2>(tup) = value;
                return 1;
            }
        }
    }
    return 1;
}

size_t
ArgumentParser::parsestr (const std::string& name, char* arg) {
    if (arg == nullptr) {
        std::stringstream ss;
        ss << "Required value for parameter \"" << name << "\" missing! (on line " << __LINE__ << ')';
        throw std::runtime_error(ss.str());
    }
    for (auto & tup : strArgs) {
        for (auto & alias : std::get<1>(tup)) {
            if (name.compare(alias) == 0) {
                std::get<2>(tup) = arg;
                return 1;
            }
        }
    }
    return 1;
}

size_t
ArgumentParser::parsebool (const std::string& name, __attribute__ ((unused)) char* arg) {
    size_t start = 0;
    if (boost::starts_with(name, "no-")) {
        start = 3;
    }
    for (auto & tup : boolArgs) {
        for (auto & alias : std::get<1>(tup)) {
            if (name.compare(start, std::string::npos, alias) == 0) {
                std::get<2>(tup) = (start == 0); // "no-..." -> false
                return 0;
            }
        }
    }
    return 0;
}

void
ArgumentParser::parse (int argc, char** argv, size_t offset) { // no C++17 (array_view), yet :-(
#ifdef DEBUG
    std::cout << "parse(int argc, char** argv, size_t offset)" << std::endl;
#endif
    if (argc > 1) {
        for (int nArg = offset; nArg < argc; ++nArg) { // always advance at least one step
            bool recognized = false;
            for (auto & p : argTypes) {
                if (p.first.compare(argv[nArg]) == 0) {
                    recognized = true;
                    char* arg = (nArg + 1) < argc ? argv[nArg + 1] : nullptr;
                    switch (p.second) {
                        case argint:
                            nArg += parseint(p.first, arg);
                            break;

                        case argstr:
                            nArg += parsestr(p.first, arg);
                            break;

                        case argbool:
                            nArg += parsebool(p.first, arg);
                            break;
                    }
                    break;
                }
            }
            if (!recognized) {
                std::stringstream ss;
                ss << "Parameter \"" << argv[nArg] << "\" is unknown! (on line " << __LINE__ << ')';
                throw std::runtime_error(ss.str());
            }
        }
    }
}

size_t
ArgumentParser::get_uint (const std::string & name) {
    for (auto & tup : uintArgs) {
        if (std::get<0>(tup).compare(name) == 0) {
            return std::get<2>(tup);
        }
    }
    std::stringstream ss;
    ss << "UINT parameter \"" << name << "\" is unknown! (on line " << __LINE__ << ')';
    throw std::runtime_error(ss.str());
}

const std::string &
ArgumentParser::get_str (const std::string & name) {
    for (auto & tup : strArgs) {
        if (std::get<0>(tup).compare(name) == 0) {
            return std::get<2>(tup);
        }
    }
    std::stringstream ss;
    ss << "String parameter \"" << name << "\" is unknown! (on line " << __LINE__ << ')';
    throw std::runtime_error(ss.str());
}

bool
ArgumentParser::get_bool (const std::string & name) {
    for (auto & tup : boolArgs) {
        if (std::get<0>(tup).compare(name) == 0) {
            return std::get<2>(tup);
        }
    }
    std::stringstream ss;
    ss << "Boolean parameter \"" << name << "\" is unknown! (on line " << __LINE__ << ')';
    throw std::runtime_error(ss.str());
}

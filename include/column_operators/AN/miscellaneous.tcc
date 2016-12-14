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
 * File:   miscellaneous.tcc
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 8. Dezember 2016, 17:21
 */

#ifndef AN_MISCELLANEOUS_TCC
#define AN_MISCELLANEOUS_TCC

namespace v2 {
    namespace bat {
        namespace ops {
            namespace Private {

                template<typename Type, typename Container, bool>
                struct Selector {

                    static uint16_t
                    A (ColumnDescriptor<Type, Container> & arg) {
                        return 1;
                    }

                    static uint16_t
                    Ainv (ColumnDescriptor<Type, Container> & arg) {
                        return 1;
                    }
                };

                template<typename Type, typename Container>
                struct Selector<Type, Container, false> {

                    static uint16_t
                    A (ColumnDescriptor<Type, Container> & arg) {
                        return arg->head.metaData.A;
                    }

                    static uint16_t
                    Ainv (ColumnDescriptor<Type, Container> & arg) {
                        return arg->tail.metaData.A;
                    }
                };
            }
        }
    }
}

#endif /* AN_MISCELLANEOUS_TCC */

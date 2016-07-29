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
 * File:   Transaction.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 28. Juli 2016, 15:05
 */

#include <stdexcept>
#include <cstring>
#include <cinttypes>

#include "column_storage/TransactionManager.h"
#include "util/resilience.hpp"

TransactionManager::Transaction::Transaction(bool isUpdater, unsigned int currentVersion) {
    this->botVersion = currentVersion;
    this->isUpdater = isUpdater;

    if (this->isUpdater) {
        this->eotVersion = new unsigned int;
        *this->eotVersion = UINT_MAX;
    } else {
        this->eotVersion = &this->botVersion;
    }
}

TransactionManager::Transaction::~Transaction() {
    for (unsigned int id = 0; id < this->iterators.size(); id++) {
        if (this->iterators[id] != 0) {
            delete this->iterators[id];
        }
    }
}

size_t TransactionManager::Transaction::load(const char *path, const char* tableName, const char *prefix, size_t size, const char* delim) {
    static const size_t MAX_PATH_LEN = 1024;
    if (path == nullptr) {
        throw runtime_error("[TransactionManager::load] You must provide a path!");
    }
    size_t pathLen = strnlen(path, MAX_PATH_LEN);
    if (pathLen == 0 || pathLen == MAX_PATH_LEN) {
        throw runtime_error("[TransactionManager::load] path is too long (>1024)");
    }
    const char* actDelim = delim == nullptr ? "|" : delim;

    MetaRepositoryManager *mrm = MetaRepositoryManager::getInstance();
    ColumnManager * cm = ColumnManager::getInstance();

    std::list<ColumnManager::ColumnIterator*> iterators;
    std::list<ColumnManager::ColumnIterator*>::iterator iteratorsIterator;
    std::list<type_t> types;
    std::list<type_t>::iterator typesIterator;

    ColumnManager::ColumnIterator *ci;
    ColumnManager::Record *record;

    set<unsigned int> columns;
    string valuesPath(path);
    valuesPath.append(".tbl");
    string headerPath(path);
    headerPath.append("_header.csv");
    FILE *valuesFile, *headerFile;
    static const size_t LEN_LINE = 1024;
    char *line = new char[LEN_LINE]();
    char *value = new char[256]();
    char *buffer = new char[1024]();
    TransactionManager::BinaryUnit *bun;
    unsigned int offset, column;
    type_t type;
    bool firstAppend = true;
    size_t n = 0; // line counter
    size_t lenPrefix = prefix ? strlen(prefix) : 0;

    unsigned newTableId; // unique id of the created table
    unsigned BATId;
    char *datatype = new char[256];
    vector<char*> attribute_names;

    if (this->isUpdater) {
        valuesFile = fopen(valuesPath.c_str(), "r");
        headerFile = fopen(headerPath.c_str(), "r");

        if (valuesFile && headerFile) {
            columns = cm->listColumns();

            if (columns.find(0) == columns.end()) {
                cm->createColumn(0, sizeof (char)*256);
                cm->createColumn(1, sizeof (unsigned int));
                cm->createColumn(2, sizeof (unsigned int));
            } else {
                // Zurückspulen der Iteratoren
                close(0);
                close(1);
                close(2);
            }

            open(0);
            open(1);
            open(2);

            // create table
            if (tableName) {
                newTableId = mrm->createTable(tableName);
            }

            // Zeile mit Spaltennamen einlesen aus Header-Datei
            fgets(line, LEN_LINE, headerFile);

            if (strchr(line, '\n') != 0) {
                *strchr(line, '\n') = 0;
            }

            // Zeile durch Zeichen actDelim in Einzelwerte trennen
            buffer = strtok(line, actDelim);

            while (buffer != 0) {
                size_t lenBuf = strlen(buffer);
                if (lenPrefix + lenBuf > 255) {
                    // Problem : Name für Spalte (inkl. Prefix) zu lang
                    throw runtime_error("[TransactionManager::load] Name of column is too long (>255)!");
                }

                bun = append(0);

                // Spaltenname = Prefix + Spaltenname aus Header-Datei
                if (prefix) {
                    strncpy(value, prefix, 256);
                    strncat(value, buffer, 256 - lenPrefix);
                } else {
                    strncpy(value, buffer, 256);
                }

                // prevents the referencing of value
                char* attribute_name = new char[256];
                strncpy(attribute_name, value, lenPrefix + lenBuf + 1);
                attribute_names.push_back(attribute_name);

                strncpy((char*) bun->tail, value, lenPrefix + lenBuf + 1);

                // Berechnung der Anzahl bisher vorhandener Spalten
                if (firstAppend) {
                    offset = *((unsigned int*) bun->head);
                    firstAppend = false;
                }

                delete bun->head;
                delete bun;

                buffer = strtok(nullptr, actDelim);
            }

            // Zeile mit Spaltentypen einlesen aus Header-Datei
            fgets(line, LEN_LINE, headerFile);

            if (strchr(line, '\n') != 0) {
                *strchr(line, '\n') = 0;
            }

            // Zeile durch Zeichen actDelim in Einzelwerte trennen
            buffer = strtok(line, actDelim);

            int attributeNamesIndex = 0;

            while (buffer != 0) {
                // freie Spalte suchen
                columns = cm->listColumns();
                column = 3;

                while (columns.find(column) != columns.end()) {
                    column++;
                }

                // Spaltentyp einpflegen
                bun = append(1);

                if (strncmp(buffer, "INTEGER", 7) == 0) {
                    *((type_t*) bun->tail) = type_int;
                    cm->createColumn(column, sizeof (int_t));
                    //cerr << "Type integer" << column << endl;
                } else if (strncmp(buffer, "STRING", 6) == 0) {
                    *((type_t*) bun->tail) = type_str;
                    cm->createColumn(column, sizeof (char_t) * MAXLEN_STRING); // TODO warum genau 45 zeichen?!
                    //cerr << "Type string" << column << endl;
                } else if (strncmp(buffer, "FIXED", 5) == 0) {
                    // data type fixed
                    *((type_t*) bun->tail) = type_fxd;
                    cm->createColumn(column, sizeof (double_t));
                    //cerr << "Type double" << column << endl;
                } else if (strncmp(buffer, "CHAR", 4) == 0) {
                    *((type_t*) bun->tail) = type_chr;
                    cm->createColumn(column, sizeof (char_t));
                } else if (strncmp(buffer, "RESINT", 6) == 0) {
                    *((type_t*) bun->tail) = type_resint;
                    cm->createColumn(column, sizeof (resint_t));
                } else {
                    // data type unknown
                    cerr << "TransactionManager::Transaction::load() data type " << buffer << " in header unknown" << endl;
                    abort();
                }


                types.push_back(*((type_t*) bun->tail));

                delete bun->head;
                delete bun;

                // Spaltenidentifikation einpflegen
                bun = append(2);

                *((unsigned int*) bun->tail) = column;

                delete bun->head;
                delete bun;

                open(column);

                iterators.push_back(this->iterators[column]);

                buffer = strtok(nullptr, actDelim);

                BATId = column;

                // create attribute for specified table
                if (tableName) {
                    mrm->createAttribute(attribute_names.at(attributeNamesIndex), datatype, BATId, newTableId);
                }

                attributeNamesIndex++;
            }

            // Spaltenwerte zeilenweise aus Datei einlesen		
            while (fgets(line, LEN_LINE, valuesFile) != 0 && n < size) {
                if (strchr(line, '\n') != 0) {
                    *strchr(line, '\n') = 0;
                }
                n++; // increase line counter

                // Iteratoren f?r Spaltentypen und Spalteniteratoren zur?cksetzen
                iteratorsIterator = iterators.begin();
                typesIterator = types.begin();

                // Zeile durch Zeichen actDelim in Einzelwerte trennen
                buffer = strtok(line, actDelim);

                while (buffer != 0) {
                    // Spaltentyp und Spaltenidentifikation bestimmen
                    type = *typesIterator;
                    ci = *iteratorsIterator;

                    record = ci->append();
                    size_t bufLen;

                    switch (type) {
                        case type_int:
                            *(static_cast<int_t*> (record->content)) = atoi(buffer);
                            break;

                        case type_str:
                            bufLen = strlen(buffer); // strtok already replaced the token separator with a null char
                            // TODO make sure the string is at most MAXLEN_STRING bytes long (incl. null byte)
                            strncpy(static_cast<str_t> (record->content), buffer, bufLen + 1);
                            break;

                        case type_fxd:
                            *(static_cast<fxd_t*> (record->content)) = atof(buffer);
                            break;

                        case type_chr:
                            *(static_cast<char_t*> (record->content)) = buffer[0];
                            break;

                        case type_resint:
                            *(static_cast<resint_t*> (record->content)) = atoll(buffer) * A;
                            break;

                        default:
                            cerr << "TransactionManager::Transaction::load() data type unknown" << endl;
                            abort();
                    }

                    delete record;

                    typesIterator++;
                    iteratorsIterator++;

                    buffer = strtok(nullptr, actDelim);
                }
            }

            // Spalten schließen
            close(0);
            close(1);
            close(2);

            open(2);

            if (offset > 0) {
                bun = get(2, offset);
            } else {
                bun = next(2);
            }

            while (bun != 0) {
                close(*((unsigned int*) bun->tail));

                delete bun->head;
                delete bun;

                bun = next(2);
            }

            close(2);

            // Dateien schließen
            fclose(valuesFile);
            fclose(headerFile);
        } else {
            // Problem : Dateien konnten nicht geöffnet werden
            cerr << "Problem : Dateien konnten nicht geöffnet werden:\n\tvaluesPath = " << valuesPath << "\n\theaderPath = " << headerPath << endl;
        }
    } else {
        // Problem : Transaktion darf keine Änderungen vornehmen
        cerr << "Problem : Transaktion darf keine Änderungen vornehmen" << endl;
    }
    return n;
}

set<unsigned int> TransactionManager::Transaction::list() {
    return ColumnManager::getInstance()->listColumns();
}

unsigned TransactionManager::Transaction::open(unsigned int id) {
    if (id >= this->iterators.size()) {
        this->iterators.resize(id + 1);
        this->iteratorPositions.resize(id + 1);

        ColumnManager::ColumnIterator* cm = ColumnManager::getInstance()->openColumn(id, this->eotVersion);

        if (cm != 0) {
            this->iterators[id] = cm;
            this->iteratorPositions[id] = 0;
            return this->iterators[id]->size();
        } else {
            // Problem : Spalte nicht existent
            return 0;
        }
    } else if (id < this->iterators.size() && this->iterators[id] != nullptr) {
        if (this->iteratorPositions[id] != -1) {
            ColumnManager::ColumnIterator* cm = ColumnManager::getInstance()->openColumn(id, this->eotVersion);

            if (cm != 0) {
                this->iterators[id] = cm;
                this->iteratorPositions[id] = 0;
                return this->iterators[id]->size();
            } else {
                // Problem : Spalte nicht existent
                return 0;
            }
        } else {
            this->iterators[id]->rewind();
            this->iteratorPositions[id] = 0;
            return this->iterators[id]->size();
        }
    } else {
        // Problem : Spalte bereits geoffnet
        return 0;
    }
}

void TransactionManager::Transaction::close(unsigned int id) {
    if (id >= this->iterators.size() || (id < this->iterators.size() && this->iterators[id] == nullptr)) {
        // Problem : Spalte nicht geoeffnet
    } else {
        this->iteratorPositions[id] = -1;
    }
}

TransactionManager::BinaryUnit* TransactionManager::Transaction::next(unsigned int id) {
    if (id < this->iterators.size() && this->iterators[id] != nullptr && this->iteratorPositions[id] != -1) {
        ColumnManager::Record *record = this->iterators[id]->next();

        if (record != 0) {
            TransactionManager::BinaryUnit *bun = new TransactionManager::BinaryUnit;
            unsigned int *position = new unsigned int;

            *position = this->iteratorPositions[id]++;
            bun->head = position;
            bun->tail = record->content;

            delete record;

            return bun;
        } else {
            // Problem : Ende der Spalte
            return 0;
        }
    } else {
        // Problem : Spalte nicht geoffnet
        return 0;
    }
}

TransactionManager::BinaryUnit* TransactionManager::Transaction::get(unsigned int id, unsigned int index) {
    if (id < this->iterators.size() && this->iterators[id] != 0 && this->iteratorPositions[id] != -1) {
        ColumnManager::Record *record = this->iterators[id]->seek(index);

        if (record != 0) {
            TransactionManager::BinaryUnit *bun = new TransactionManager::BinaryUnit;
            unsigned int *position = new unsigned int;

            *position = index++;
            this->iteratorPositions[id] = index;
            bun->head = position;
            bun->tail = record->content;

            delete record;

            return bun;
        } else {
            // Problem : Falscher Index
            this->iteratorPositions[id] = 0;
            return 0;
        }
    } else {
        // Problem : Spalte nicht geoeffnet
        return 0;
    }
}

TransactionManager::BinaryUnit* TransactionManager::Transaction::edit(unsigned int id) {
    if (this->isUpdater) {
        if (id < this->iterators.size() && this->iterators[id] != 0 && this->iteratorPositions[id] != -1) {
            ColumnManager::Record *record = this->iterators[id]->edit();

            if (record != 0) {
                TransactionManager::BinaryUnit *bun = new TransactionManager::BinaryUnit;
                unsigned int *position = new unsigned int;

                *position = this->iteratorPositions[id] - 1;
                bun->head = position;
                bun->tail = record->content;

                delete record;

                return bun;
            } else {
                // Problem : Ende der Spalte
                return 0;
            }
        } else {
            // Problem : Spalte nicht geoffnet
            return 0;
        }
    } else {
        // Problem : Transaktion darf keine ?nderungen vornehmen
        return 0;
    }
}

TransactionManager::BinaryUnit* TransactionManager::Transaction::append(unsigned int id) {
    if (this->isUpdater) {
        if (id < this->iterators.size() && this->iterators[id] != 0 && this->iteratorPositions[id] != -1) {
            ColumnManager::Record *record = this->iterators[id]->append();

            if (record != 0) {
                TransactionManager::BinaryUnit *bun = new TransactionManager::BinaryUnit;
                unsigned int *position = new unsigned int;

                this->iteratorPositions[id] = this->iterators[id]->size();
                *position = this->iteratorPositions[id] - 1;

                bun->head = position;
                bun->tail = record->content;

                delete record;

                return bun;
            } else {
                // Problem : Record konnte nicht an Spalte angehängt werden
                return nullptr;
            }
        } else {
            // Problem : Spalte nicht geoffnet
            return nullptr;
        }
    } else {
        // Problem : Transaktion darf keine Änderungen vornehmen
        return nullptr;
    }
}

void TransactionManager::Transaction::rollback() {
    for (unsigned int id = 0; id < this->iterators.size(); id++) {
        if (this->iterators[id] != 0) {
            this->iterators[id]->undo();
            delete this->iterators[id];
        }
    }
}

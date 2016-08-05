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
#include <sstream>

#include "column_storage/TransactionManager.h"
#include "util/resilience.hpp"

using namespace std;

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

size_t TransactionManager::Transaction::load(const char *path, const char* tableName, const char *prefix, size_t size, const char* delim, bool ignoreMoreData) {
    static const size_t LEN_PATH = 1024;
    static const size_t LEN_LINE = 1024;
    static const size_t LEN_VALUE = 256;

    if (path == nullptr) {
        throw runtime_error("[TransactionManager::load] You must provide a path!");
    }
    size_t pathLen = strnlen(path, LEN_PATH);
    if (pathLen == 0 || pathLen == LEN_PATH) {
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
    char line[LEN_LINE];
    memset(line, 0, LEN_LINE);
    char value[LEN_VALUE];
    memset(value, 0, LEN_VALUE);
    char *buffer;
    TransactionManager::BinaryUnit *bun;
    unsigned int offset, column;
    type_t type;
    bool firstAppend = true;
    size_t n = 0; // line counter
    size_t lenPrefix = prefix ? strlen(prefix) : 0;

    unsigned newTableId; // unique id of the created table
    unsigned BATId;
    char datatype[LEN_VALUE];
    vector<char*> attribute_names;

    if (this->isUpdater) {
        valuesFile = fopen(valuesPath.c_str(), "r");
        headerFile = fopen(headerPath.c_str(), "r");

        if (valuesFile && headerFile) {
            columns = cm->listColumns();

            if (columns.find(ID_BAT_COLNAMES) == columns.end()) {
                cm->createColumn(ID_BAT_COLNAMES, sizeof (char)*LEN_VALUE);
                cm->createColumn(ID_BAT_COLTYPES, sizeof (unsigned int));
                cm->createColumn(ID_BAT_COLIDENT, sizeof (unsigned int));
            } else {
                // Zurückspulen der Iteratoren
                close(ID_BAT_COLNAMES);
                close(ID_BAT_COLTYPES);
                close(ID_BAT_COLIDENT);
            }

            open(ID_BAT_COLNAMES);
            open(ID_BAT_COLTYPES);
            open(ID_BAT_COLIDENT);

            // create table
            if (tableName) {
                newTableId = mrm->createTable(tableName);
            }

            // Zeile mit Spaltennamen einlesen aus Header-Datei
            memset(line, 0, LEN_LINE);
            fgets(line, LEN_LINE, headerFile);

            char* pPos;
            if ((pPos = strchr(line, '\n')) != nullptr) {
                *pPos = 0;
            }
            if (*(pPos - 1) == '\r') {
                *(pPos - 1) = 0;
            }

            // Zeile durch Zeichen actDelim in Einzelwerte trennen
            buffer = strtok(line, actDelim);

            while (buffer != nullptr) {
                size_t lenBuf = strlen(buffer);
                if (lenPrefix + lenBuf > (LEN_VALUE - 1)) {
                    // Problem : Name für Spalte (inkl. Prefix) zu lang
                    stringstream ss;
                    ss << "[TransactionManager::load] Name of column is too long (>" << (LEN_VALUE - 1) << ")!";
                    throw runtime_error(ss.str());
                }

                bun = append(ID_BAT_COLNAMES);

                // Spaltenname = Prefix + Spaltenname aus Header-Datei
                if (prefix) {
                    strncpy(value, prefix, LEN_VALUE);
                    strncat(value, buffer, LEN_VALUE - lenPrefix);
                } else {
                    strncpy(value, buffer, LEN_VALUE);
                }

                // prevents the referencing of value
                const size_t attr_name_len = lenPrefix + lenBuf + 1;
                char* attribute_name = new char[attr_name_len];
                strncpy(attribute_name, value, attr_name_len);
                attribute_names.push_back(attribute_name);

                strncpy(static_cast<char*> (bun->tail), value, attr_name_len);

                // Berechnung der Anzahl bisher vorhandener Spalten
                if (firstAppend) {
                    offset = *reinterpret_cast<unsigned*> (&bun->head);
                    firstAppend = false;
                }

                // delete static_cast<unsigned int*> (bun->head);
                delete bun;

                buffer = strtok(nullptr, actDelim);
            }

            // Zeile mit Spaltentypen einlesen aus Header-Datei
            memset(line, 0, LEN_LINE);
            fgets(line, LEN_LINE, headerFile);

            if ((pPos = strchr(line, '\n')) != nullptr) {
                *pPos = 0;
            }
            if (*(pPos - 1) == '\r') {
                *(pPos - 1) = 0;
            }

            // Zeile durch Zeichen actDelim in Einzelwerte trennen
            buffer = strtok(line, actDelim);

            int attributeNamesIndex = 0;

            while (buffer != nullptr) {
                // freie Spalte suchen
                columns = cm->listColumns();
                column = ID_BAT_FIRST_USER;

                while (columns.find(column) != columns.end()) {
                    column++;
                }

                // Spaltentyp einpflegen
                bun = append(ID_BAT_COLTYPES);

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
                    cm->createColumn(column, sizeof (fxd_t));
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

                // delete static_cast<unsigned int*> (bun->head);
                delete bun;

                // Spaltenidentifikation einpflegen
                bun = append(ID_BAT_COLIDENT);
                *static_cast<unsigned*> (bun->tail) = column;
                // delete static_cast<unsigned int*> (bun->head);
                delete bun;

                open(column);
                iterators.push_back(this->iterators[column]);

                // create attribute for specified table
                if (tableName) {
                    BATId = column;
                    mrm->createAttribute(attribute_names.at(attributeNamesIndex), datatype, BATId, newTableId);
                }

                buffer = strtok(nullptr, actDelim);
                attributeNamesIndex++;
            }

            // Spaltenwerte zeilenweise aus Datei einlesen
            memset(line, 0, LEN_LINE);
            while (fgets(line, LEN_LINE, valuesFile) != 0 && n < size) {
                if ((pPos = strchr(line, '\n')) != nullptr) {
                    *pPos = 0;
                }
                if (*(pPos - 1) == '\r') {
                    *(pPos - 1) = 0;
                }
                n++; // increase line counter

                // Iteratoren f?r Spaltentypen und Spalteniteratoren zur?cksetzen
                iteratorsIterator = iterators.begin();
                typesIterator = types.begin();

                // Zeile durch Zeichen actDelim in Einzelwerte trennen
                buffer = strtok(line, actDelim);
                size_t numVal = 1;

                while (buffer != nullptr) {
                    if (typesIterator == types.end()) {
                        if (ignoreMoreData) {
                            buffer = nullptr;
                        } else {
                            stringstream ss;
                            ss << "TransactionManager::Transaction::load() more values than types registered (#" << numVal << ")!";
                            throw runtime_error(ss.str());
                        }
                    } else {
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
                                *(static_cast<resint_t*> (record->content)) = atoll(buffer) * ::A;
                                break;

                            default:
                                cerr << "TransactionManager::Transaction::load() data type unknown" << endl;
                                abort();
                        }

                        delete record;

                        typesIterator++;
                        iteratorsIterator++;

                        buffer = strtok(nullptr, actDelim);
                        ++numVal;
                    }
                }
                memset(line, 0, LEN_LINE);
            }
            cout << "Transaction::load() imported " << n << " records." << endl;

            // Spalten schließen
            close(ID_BAT_COLNAMES);
            close(ID_BAT_COLTYPES);
            close(ID_BAT_COLIDENT);

            open(ID_BAT_COLIDENT);

            if (offset > 0) {
                bun = get(ID_BAT_COLIDENT, offset);
            } else {
                bun = next(ID_BAT_COLIDENT);
            }

            while (bun != 0) {
                close(*((unsigned int*) bun->tail));

                // delete static_cast<unsigned int*> (bun->head);
                delete bun;

                bun = next(ID_BAT_COLIDENT);
            }

            close(ID_BAT_COLIDENT);

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

pair<size_t, size_t> TransactionManager::Transaction::open(unsigned int id) {
    bool isOK = false;
    if (id >= this->iterators.size()) {
        this->iterators.resize(id + 1);
        this->iteratorPositions.resize(id + 1);
        ColumnManager::ColumnIterator* cm = ColumnManager::getInstance()->openColumn(id, this->eotVersion);
        if (cm != 0) {
            this->iterators[id] = cm;
            this->iteratorPositions[id] = 0;
            isOK = true;
        } else {
            // Problem : Spalte nicht existent
        }
    } else if (id < this->iterators.size() && this->iterators[id] != nullptr) {
        if (this->iteratorPositions[id] != -1) {
            ColumnManager::ColumnIterator* cm = ColumnManager::getInstance()->openColumn(id, this->eotVersion);
            if (cm != 0) {
                this->iterators[id] = cm;
                this->iteratorPositions[id] = 0;
                isOK = true;
            } else {
                // Problem : Spalte nicht existent
            }
        } else {
            this->iterators[id]->rewind();
            this->iteratorPositions[id] = 0;
            isOK = true;
        }
    } else {
        // Problem : Spalte bereits geoffnet
    }
    return isOK ? make_pair<size_t, size_t>(this->iterators[id]->size(), this->iterators[id]->consumption()) : make_pair<size_t, size_t>(0, 0);
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
            *reinterpret_cast<unsigned*> (&bun->head) = this->iteratorPositions[id]++;
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
            *reinterpret_cast<unsigned*> (&bun->head) = index++;
            bun->tail = record->content;
            this->iteratorPositions[id] = index;
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
                *reinterpret_cast<unsigned*> (&bun->head) = this->iteratorPositions[id] - 1;
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
                // this->iteratorPositions[id] = this->iterators[id]->size();
                *reinterpret_cast<unsigned*> (&bun->head) = this->iteratorPositions[id]++;
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

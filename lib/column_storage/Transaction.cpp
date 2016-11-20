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
#include <sstream>

#include <ColumnStore.h>
#include <column_storage/TransactionManager.h>
#include <util/resilience.hpp>

using namespace std;

TransactionManager::Transaction::Transaction(bool isUpdater, unsigned int currentVersion) : botVersion(currentVersion), eotVersion(isUpdater ? (new unsigned int(UINT_MAX)) : (&this->botVersion)), isUpdater(isUpdater), iterators(), iteratorPositions() {
}

TransactionManager::Transaction::Transaction(const Transaction &copy) : Transaction(copy.isUpdater, copy.botVersion) {
}

TransactionManager::Transaction::~Transaction() {
    for (auto pIter : iterators) {
        if (pIter) {
            delete pIter;
        }
    }
}

TransactionManager::Transaction& TransactionManager::Transaction::operator=(const Transaction &copy) {
    new (this) Transaction(copy);
    return *this;
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

    set<id_t> columns;
    string valuesPath(path);
    valuesPath.append(".tbl");
    string headerPath(path);
    headerPath.append("_header.csv");
    FILE *valuesFile, *headerFile;
    char line[LEN_LINE];
    memset(line, 0, LEN_LINE);
    char value[LEN_VALUE];
    memset(value, 0, LEN_VALUE);
    char *buffer(nullptr);
    TransactionManager::BinaryUnit bun;
    oid_t offset(0);
    id_t column(0);
    type_t type(type_void);
    bool firstAppend = true;
    size_t n(0); // line counter
    size_t lenPrefix = prefix ? strlen(prefix) : 0;

    id_t newTableId(0); // unique id of the created table
    id_t BATId(0);
    char datatype[LEN_VALUE];
    vector<char*> attribute_names;
    vector<size_t> column_widths(32); // initialize with 32 zeros
    size_t column_width(0);

    if (this->isUpdater) {
        valuesFile = std::fopen(valuesPath.c_str(), "r");
        headerFile = std::fopen(headerPath.c_str(), "r");

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
            std::memset(line, 0, LEN_LINE);
            if (std::fgets(line, LEN_LINE, headerFile) != line) {
                throw std::runtime_error("Error reading line");
            }

            char* pPos;
            if ((pPos = std::strchr(line, '\n')) != nullptr) {
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
                    std::strncpy(value, prefix, LEN_VALUE);
                    std::strncat(value, buffer, LEN_VALUE - lenPrefix);
                } else {
                    std::strncpy(value, buffer, LEN_VALUE);
                }

                // prevents the referencing of value
                const size_t attr_name_len = lenPrefix + lenBuf + 1;
                char* attribute_name = new char[attr_name_len];
                std::strncpy(attribute_name, value, attr_name_len);
                attribute_names.push_back(attribute_name);

                std::strncpy(static_cast<str_t> (bun.tail), value, attr_name_len);

                // Berechnung der Anzahl bisher vorhandener Spalten
                if (firstAppend) {
                    offset = bun.head.oid;
                    firstAppend = false;
                }

                buffer = std::strtok(nullptr, actDelim);
            }

            // Zeile mit Spaltentypen einlesen aus Header-Datei
            std::memset(line, 0, LEN_LINE);
            if (std::fgets(line, LEN_LINE, headerFile) != line) {
                throw std::runtime_error("Error reading line");
            }

            if ((pPos = std::strchr(line, '\n')) != nullptr) {
                *pPos = 0;
            }
            if (*(pPos - 1) == '\r') {
                *(pPos - 1) = 0;
            }

            // Zeile durch Zeichen actDelim in Einzelwerte trennen
            buffer = std::strtok(line, actDelim);

            int attributeNamesIndex = 0;

            size_t colIdx = 0;
            while (buffer != nullptr) {
                size_t bufSlen = strlen(buffer);
                // freie Spalte suchen
                columns = cm->listColumns();
                column = ID_BAT_FIRST_USER;

                while (columns.find(column) != columns.end()) {
                    column++;
                }

                // Spaltentyp einpflegen
                bun = append(ID_BAT_COLTYPES);

                if (std::strncmp(buffer, "INTEGER", 7) == 0 || std::strncmp(buffer, "INT", 3) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_int;
                    column_width = sizeof (int_t);
                    std::strncpy(datatype, NAME_INTEGER, LEN_VALUE);
                } else if (std::strncmp(buffer, "TINYINT", 7) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_tinyint;
                    column_width = sizeof (tinyint_t);
                    std::strncpy(datatype, NAME_TINYINT, LEN_VALUE);
                } else if (std::strncmp(buffer, "SHORTINT", 8) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_shortint;
                    column_width = sizeof (shortint_t);
                    std::strncpy(datatype, NAME_SHORTINT, LEN_VALUE);
                } else if (std::strncmp(buffer, "LARGEINT", 8) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_largeint;
                    column_width = sizeof (bigint_t);
                    std::strncpy(datatype, NAME_LARGEINT, LEN_VALUE);
                } else if (std::strncmp(buffer, "STRING", 6) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_string;
                    size_t maxlen = MAXLEN_STRING;
                    if (bufSlen > 6 && buffer[6] == ':') {
                        maxlen = atoi(&buffer[7]);
                    }
                    column_width = sizeof (char_t) * maxlen + 1;
                    std::strncpy(datatype, NAME_STRING, LEN_VALUE);
                } else if (std::strncmp(buffer, "FIXED", 5) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_fixed;
                    column_width = sizeof (fixed_t);
                    std::strncpy(datatype, NAME_FIXED, LEN_VALUE);
                } else if (std::strncmp(buffer, "CHAR", 4) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_char;
                    column_width = sizeof (char_t);
                    std::strncpy(datatype, NAME_CHAR, LEN_VALUE);
                } else if (std::strncmp(buffer, "RESTINY", 7) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_restiny;
                    column_width = sizeof (restiny_t);
                    std::strncpy(datatype, NAME_RESTINY, LEN_VALUE);
                } else if (std::strncmp(buffer, "RESSHORT", 8) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_resshort;
                    column_width = sizeof (resshort_t);
                    std::strncpy(datatype, NAME_RESSHORT, LEN_VALUE);
                } else if (std::strncmp(buffer, "RESINT", 6) == 0) {
                    *static_cast<type_t*> (bun.tail) = type_resint;
                    column_width = sizeof (resint_t);
                    std::strncpy(datatype, NAME_RESINT, LEN_VALUE);
                } else {
                    cerr << "TransactionManager::Transaction::load() data type " << buffer << " in header unknown" << endl;
                    abort();
                }

                column_widths[colIdx] = column_width;
                ++colIdx;
                cm->createColumn(column, column_width);
                types.push_back(*static_cast<type_t*> (bun.tail));

                // Spaltenidentifikation einpflegen
                bun = append(ID_BAT_COLIDENT);
                *static_cast<unsigned*> (bun.tail) = column;

                open(column);
                iterators.push_back(this->iterators[column]);

                // create attribute for specified table
                if (tableName) {
                    BATId = column;
                    mrm->createAttribute(attribute_names.at(attributeNamesIndex), datatype, BATId, newTableId);
                }

                buffer = std::strtok(nullptr, actDelim);
                attributeNamesIndex++;
            }

            // Spaltenwerte zeilenweise aus Datei einlesen
            std::memset(line, 0, LEN_LINE);
            while (std::fgets(line, LEN_LINE, valuesFile) != 0 && n < size) {
                if ((pPos = std::strchr(line, '\n')) != nullptr) {
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
                buffer = std::strtok(line, actDelim);
                size_t numVal = 1;
                colIdx = 0;
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

                        auto record = ci->append();

                        switch (type) {
                            case type_tinyint:
                                *(static_cast<tinyint_t*> (record.content)) = static_cast<tinyint_t> (std::atoi(buffer));
                                break;

                            case type_shortint:
                                *(static_cast<shortint_t*> (record.content)) = static_cast<shortint_t> (std::atoi(buffer));
                                break;

                            case type_int:
                                *(static_cast<int_t*> (record.content)) = std::atol(buffer);
                                break;

                            case type_largeint:
                                *(static_cast<bigint_t*> (record.content)) = static_cast<bigint_t> (std::atoll(buffer));
                                break;

                            case type_string:
                                std::strncpy(static_cast<str_t> (record.content), buffer, column_widths[colIdx]);
                                break;

                            case type_fixed:
                                *(static_cast<fixed_t*> (record.content)) = std::atof(buffer);
                                break;

                            case type_char:
                                *(static_cast<char_t*> (record.content)) = buffer[0];
                                break;

                            case type_restiny:
                                *(static_cast<restiny_t*> (record.content)) = std::atol(buffer) * v2_restiny_t::A;
                                break;

                            case type_resshort:
                                *(static_cast<resshort_t*> (record.content)) = std::atol(buffer) * v2_resshort_t::A;
                                break;

                            case type_resint:
                                *(static_cast<resint_t*> (record.content)) = std::atoll(buffer) * v2_resint_t::A;
                                break;

                            default:
                                cerr << "TransactionManager::Transaction::load() data type unknown" << endl;
                                abort();
                        }

                        ++colIdx;
                        ++numVal;
                        typesIterator++;
                        iteratorsIterator++;

                        buffer = std::strtok(nullptr, actDelim);
                    }
                }
                std::memset(line, 0, LEN_LINE);
            }

            // Spalten schließen
            close(ID_BAT_COLNAMES);
            close(ID_BAT_COLTYPES);
            close(ID_BAT_COLIDENT);

            open(ID_BAT_COLIDENT);
            bun = get(ID_BAT_COLIDENT, offset);
            while (bun.tail != nullptr) {
                close(*static_cast<oid_t*> (bun.tail));
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

pair<size_t, size_t> TransactionManager::Transaction::open(id_t id) {
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

void TransactionManager::Transaction::close(id_t id) {
    if (id >= this->iterators.size() || (id < this->iterators.size() && this->iterators[id] == nullptr)) {
        // Problem : Spalte nicht geoeffnet
    } else {
        this->iteratorPositions[id] = -1;
    }
}

TransactionManager::BinaryUnit TransactionManager::Transaction::next(id_t id) {
    if (id < this->iterators.size() && this->iterators[id] != nullptr && this->iteratorPositions[id] != -1) {
        const ColumnManager::Record &record = this->iterators[id]->next();

        if (record.content != nullptr) {
            TransactionManager::BinaryUnit bun;
            bun.head.oid = this->iteratorPositions[id]++;
            bun.tail = record.content;
            return bun;
        } else {
            // Problem : Ende der Spalte
            return TransactionManager::BinaryUnit();
        }
    } else {
        // Problem : Spalte nicht geoffnet
        return TransactionManager::BinaryUnit();
    }
}

TransactionManager::BinaryUnit TransactionManager::Transaction::get(id_t id, oid_t index) {
    if (id < this->iterators.size() && this->iterators[id] != 0 && this->iteratorPositions[id] != -1) {
        const ColumnManager::Record &record = this->iterators[id]->seek(index);

        if (record.content != nullptr) {
            TransactionManager::BinaryUnit bun;
            bun.head.oid = this->iteratorPositions[id]++;
            bun.tail = record.content;
            return bun;
        } else {
            // Problem : Falscher Index
            this->iteratorPositions[id] = 0;
            return TransactionManager::BinaryUnit();
        }
    } else {
        // Problem : Spalte nicht geoeffnet
        return TransactionManager::BinaryUnit();
    }
}

TransactionManager::BinaryUnit TransactionManager::Transaction::edit(id_t id) {
    if (this->isUpdater) {
        if (id < this->iterators.size() && this->iterators[id] != 0 && this->iteratorPositions[id] != -1) {
            const ColumnManager::Record &record = this->iterators[id]->edit();

            if (record.content != nullptr) {
                TransactionManager::BinaryUnit bun;
                bun.head.oid = this->iteratorPositions[id] - 1;
                bun.tail = record.content;
                return bun;
            } else {
                // Problem : Ende der Spalte
                return TransactionManager::BinaryUnit();
            }
        } else {
            // Problem : Spalte nicht geoffnet
            return TransactionManager::BinaryUnit();
        }
    } else {
        // Problem : Transaktion darf keine ?nderungen vornehmen
        return TransactionManager::BinaryUnit();
    }
}

TransactionManager::BinaryUnit TransactionManager::Transaction::append(id_t id) {
    if (this->isUpdater) {
        if (id < this->iterators.size() && this->iterators[id] != 0 && this->iteratorPositions[id] != -1) {
            const ColumnManager::Record &record = this->iterators[id]->append();

            if (record.content != nullptr) {
                TransactionManager::BinaryUnit bun;
                bun.head.oid = this->iteratorPositions[id]++;
                bun.tail = record.content;
                return move(bun);
            } else {
                // Problem : Record konnte nicht an Spalte angehängt werden
                return TransactionManager::BinaryUnit();
            }
        } else {
            // Problem : Spalte nicht geoffnet
            return TransactionManager::BinaryUnit();
        }
    } else {
        // Problem : Transaktion darf keine Änderungen vornehmen
        return TransactionManager::BinaryUnit();
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

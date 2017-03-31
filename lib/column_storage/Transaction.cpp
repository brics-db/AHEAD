// Copyright (c) 2016-2017 Till Kolditz
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
 * File:   Transaction.cpp
 * Author: Till Kolditz <till.kolditz@gmail.com>
 *
 * Created on 28. Juli 2016, 15:05
 */

#include <cstdlib>
#include <cstring>
#include <sstream>
#include <limits>

#include <ColumnStore.h>
#include <column_storage/TransactionManager.h>
#include <util/resilience.hpp>

namespace v2 {

    TransactionManager::Transaction::Transaction(bool isUpdater, version_t currentVersion)
            : botVersion(currentVersion), eotVersion(isUpdater ? (new id_t(std::numeric_limits<id_t>::max())) : (&this->botVersion)), isUpdater(isUpdater), iterators(), iteratorPositions() {
    }

    TransactionManager::Transaction::Transaction(const Transaction &copy)
            : Transaction(copy.isUpdater, copy.botVersion) {
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
        const size_t LEN_PATH = 1024;
        const size_t LEN_LINE = 1024;
        const size_t LEN_VALUE = 256;

        if (path == nullptr) {
            throw std::runtime_error("[TransactionManager::load] You must provide a path!");
        }
        size_t pathLen = strnlen(path, LEN_PATH);
        if (pathLen == 0 || pathLen > LEN_PATH) {
            throw std::runtime_error("[TransactionManager::load] path is too long (>1024)");
        }
        const char* actDelim = delim == nullptr ? "|" : delim;

        MetaRepositoryManager *mrm = MetaRepositoryManager::getInstance();
        ColumnManager * cm = ColumnManager::getInstance();

        std::list<ColumnManager::ColumnIterator*> iterators;
        std::list<ColumnManager::ColumnIterator*>::iterator iteratorsIterator;
        std::list<type_t> types;
        std::list<type_t>::iterator typesIterator;

        ColumnManager::ColumnIterator *ci;

        std::string valuesPath(path);
        valuesPath.append(".tbl");
        std::string headerPath(path);
        headerPath.append("_header.csv");
        FILE *valuesFile, *headerFile;
        char line[LEN_LINE];
        std::memset(line, 0, LEN_LINE);
        char value[LEN_VALUE];
        std::memset(value, 0, LEN_VALUE);
        char *buffer(nullptr);
        TransactionManager::BinaryUnit bun;
        oid_t offset(0);
        id_t columnId(0);
        type_t type(type_void);
        bool firstAppend = true;
        size_t n(0); // line counter
        size_t lenPrefix = prefix ? strlen(prefix) : 0;

        id_t newTableId(0); // unique id of the created table
        char datatype[LEN_VALUE];
        std::vector<char*> attribute_names;
        std::vector<size_t> columnWidths;
        columnWidths.reserve(32);
        size_t columnWidth(0);
        std::stringstream sserr;
        std::vector<id_t> columnIDs;
        columnIDs.reserve(32);
        std::vector<ColumnMetaData> columnVec;
        columnVec.reserve(32);

        if (!this->isUpdater) {
            // Problem : Transaktion darf keine Änderungen vornehmen
            sserr << "Problem : Transaktion darf keine Änderungen vornehmen" << std::endl;
            throw std::runtime_error(sserr.str());
        }

        valuesFile = std::fopen(valuesPath.c_str(), "r");
        headerFile = std::fopen(headerPath.c_str(), "r");

        if (!valuesFile || !headerFile) {
            // Problem : Dateien konnten nicht geöffnet werden
            sserr << "Problem : Dateien konnten nicht geöffnet werden:\n\tvaluesPath = " << valuesPath << "\n\theaderPath = " << headerPath << std::endl;
            throw std::runtime_error(sserr.str());
        }

        auto curColumnIDs = cm->getColumnIDs();

        if (curColumnIDs.find(ID_BAT_COLNAMES) == curColumnIDs.end()) {
            cm->createColumn(ID_BAT_COLNAMES, sizeof(char) * LEN_VALUE);
            cm->createColumn(ID_BAT_COLTYPES, sizeof(type_t));
            cm->createColumn(ID_BAT_COLIDENT, sizeof(id_t));
        } else {
            // Zurückspulen der Iteratoren
            this->close(ID_BAT_COLNAMES);
            this->close(ID_BAT_COLTYPES);
            this->close(ID_BAT_COLIDENT);
        }

        this->open(ID_BAT_COLNAMES);
        this->open(ID_BAT_COLTYPES);
        this->open(ID_BAT_COLIDENT);

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
                sserr << "[TransactionManager::load] Name of column is too long (>" << (LEN_VALUE - 1) << ")!";
                throw std::runtime_error(sserr.str());
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

            std::strncpy(static_cast<str_t>(bun.tail), value, attr_name_len);

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

        while (buffer != nullptr) {
            size_t bufSlen = strlen(buffer);

            // freie Spalte suchen
            curColumnIDs = cm->getColumnIDs();
            columnId = ID_BAT_FIRST_USER;
            while (curColumnIDs.find(columnId) != curColumnIDs.end()) {
                columnId++;
            }
            columnIDs.push_back(columnId);

            // Spaltentyp einpflegen
            bun = append(ID_BAT_COLTYPES);
            bool stdCreate = true;

            if (std::strncmp(buffer, "INTEGER", 7) == 0 || std::strncmp(buffer, "INT", 3) == 0) {
                *static_cast<type_t*>(bun.tail) = type_int;
                columnWidth = sizeof(int_t);
                std::strncpy(datatype, NAME_INTEGER, LEN_VALUE);
            } else if (std::strncmp(buffer, "TINYINT", 7) == 0) {
                *static_cast<type_t*>(bun.tail) = type_tinyint;
                columnWidth = sizeof(tinyint_t);
                std::strncpy(datatype, NAME_TINYINT, LEN_VALUE);
            } else if (std::strncmp(buffer, "SHORTINT", 8) == 0) {
                *static_cast<type_t*>(bun.tail) = type_shortint;
                columnWidth = sizeof(shortint_t);
                std::strncpy(datatype, NAME_SHORTINT, LEN_VALUE);
            } else if (std::strncmp(buffer, "LARGEINT", 8) == 0) {
                *static_cast<type_t*>(bun.tail) = type_largeint;
                columnWidth = sizeof(bigint_t);
                std::strncpy(datatype, NAME_LARGEINT, LEN_VALUE);
            } else if (std::strncmp(buffer, "STRING", 6) == 0) {
                *static_cast<type_t*>(bun.tail) = type_string;
                size_t maxlen = MAXLEN_STRING;
                if (bufSlen > 6 && buffer[6] == ':') {
                    maxlen = atoi(&buffer[7]);
                }
                columnWidth = sizeof(char_t) * maxlen + 1;
                std::strncpy(datatype, NAME_STRING, LEN_VALUE);
            } else if (std::strncmp(buffer, "FIXED", 5) == 0) {
                *static_cast<type_t*>(bun.tail) = type_fixed;
                columnWidth = sizeof(fixed_t);
                std::strncpy(datatype, NAME_FIXED, LEN_VALUE);
            } else if (std::strncmp(buffer, "CHAR", 4) == 0) {
                *static_cast<type_t*>(bun.tail) = type_char;
                columnWidth = sizeof(char_t);
                std::strncpy(datatype, NAME_CHAR, LEN_VALUE);
            } else if (std::strncmp(buffer, "RESTINY", 7) == 0) {
                *static_cast<type_t*>(bun.tail) = type_restiny;
                columnWidth = sizeof(restiny_t);
                std::strncpy(datatype, NAME_RESTINY, LEN_VALUE);
                cm->createColumn(columnId, ColumnMetaData(columnWidth, std::get<7>(*v2_restiny_t::As), std::get<7>(*v2_restiny_t::Ainvs), v2_restiny_t::UNENC_MAX_U, v2_restiny_t::UNENC_MIN));
                stdCreate = false;
            } else if (std::strncmp(buffer, "RESSHORT", 8) == 0) {
                *static_cast<type_t*>(bun.tail) = type_resshort;
                columnWidth = sizeof(resshort_t);
                std::strncpy(datatype, NAME_RESSHORT, LEN_VALUE);
                cm->createColumn(columnId, ColumnMetaData(columnWidth, std::get<15>(*v2_resshort_t::As), std::get<15>(*v2_resshort_t::Ainvs), v2_resshort_t::UNENC_MAX_U, v2_resshort_t::UNENC_MIN));
                stdCreate = false;
            } else if (std::strncmp(buffer, "RESINT", 6) == 0) {
                *static_cast<type_t*>(bun.tail) = type_resint;
                columnWidth = sizeof(resint_t);
                std::strncpy(datatype, NAME_RESINT, LEN_VALUE);
                cm->createColumn(columnId, ColumnMetaData(columnWidth, std::get<15>(*v2_resint_t::As), std::get<15>(*v2_resint_t::Ainvs), v2_resint_t::UNENC_MAX_U, v2_resint_t::UNENC_MIN));
                stdCreate = false;
            } else {
                sserr << "TransactionManager::Transaction::load() data type " << buffer << " in header unknown" << std::endl;
                throw std::runtime_error(sserr.str());
            }

            columnWidths.push_back(columnWidth);
            if (stdCreate) {
                cm->createColumn(columnId, columnWidth);
            }
            types.push_back(*static_cast<type_t*>(bun.tail));

            // Spaltenidentifikation einpflegen
            bun = append(ID_BAT_COLIDENT);
            *static_cast<id_t*>(bun.tail) = columnId;

            this->open(columnId);
            iterators.push_back(this->iterators[columnId]);

            // create attribute for specified table
            if (tableName) {
                mrm->createAttribute(attribute_names.at(attributeNamesIndex), datatype, columnId, newTableId);
            }

            buffer = std::strtok(nullptr, actDelim);
            attributeNamesIndex++;
            columnId++;
        }

        // prefetch all Column pointers so we don't end up probing the unordered_set for each and every value
        auto columns = cm->getColumnMetaData();
        for (auto id : columnIDs) {
            columnVec.push_back((*columns)[id]); // pointer of the actual Column object
        }
        delete columns;

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
            size_t colIdx = 0;
            while (buffer != nullptr) {
                if (typesIterator == types.end()) {
                    if (ignoreMoreData) {
                        buffer = nullptr;
                        continue;
                    } else {
                        sserr << "TransactionManager::Transaction::load() more values than types registered (#" << numVal << ")!";
                        throw std::runtime_error(sserr.str());
                    }
                }

                // Spaltentyp und Spaltenidentifikation bestimmen
                type = *typesIterator;
                ci = *iteratorsIterator;
                auto record = ci->append();
                switch (type) {
                    case type_tinyint:
                        *(static_cast<tinyint_t*>(record.content)) = static_cast<tinyint_t>(std::atoi(buffer));
                        break;

                    case type_shortint:
                        *(static_cast<shortint_t*>(record.content)) = static_cast<shortint_t>(std::atoi(buffer));
                        break;

                    case type_int:
                        *(static_cast<int_t*>(record.content)) = std::atol(buffer);
                        break;

                    case type_largeint:
                        *(static_cast<bigint_t*>(record.content)) = static_cast<bigint_t>(std::atoll(buffer));
                        break;

                    case type_string:
                        std::strncpy(static_cast<str_t>(record.content), buffer, columnWidths[colIdx]);
                        break;

                    case type_fixed:
                        *(static_cast<fixed_t*>(record.content)) = std::atof(buffer);
                        break;

                    case type_char:
                        *(static_cast<char_t*>(record.content)) = buffer[0];
                        break;

                    case type_restiny:
                        *(static_cast<restiny_t*>(record.content)) = std::atol(buffer) * static_cast<restiny_t>(columnVec[colIdx].AN_A);
                        break;

                    case type_resshort:
                        *(static_cast<resshort_t*>(record.content)) = std::atol(buffer) * static_cast<resshort_t>(columnVec[colIdx].AN_A);
                        break;

                    case type_resint:
                        *(static_cast<resint_t*>(record.content)) = std::atoll(buffer) * static_cast<resint_t>(columnVec[colIdx].AN_A);
                        break;

                    default:
                        sserr << "TransactionManager::Transaction::load() data type unknown" << std::endl;
                        throw std::runtime_error(sserr.str());
                }

                ++colIdx;
                ++numVal;
                typesIterator++;
                iteratorsIterator++;

                buffer = std::strtok(nullptr, actDelim);
            }
            std::memset(line, 0, LEN_LINE);
        }

        // Spalten schließen
        this->close(ID_BAT_COLNAMES);
        this->close(ID_BAT_COLTYPES);
        this->close(ID_BAT_COLIDENT);

        this->open(ID_BAT_COLIDENT);
        bun = this->get(ID_BAT_COLIDENT, offset);
        while (bun.tail != nullptr) {
            this->close(*static_cast<oid_t*>(bun.tail));
            bun = next(ID_BAT_COLIDENT);
        }
        this->close(ID_BAT_COLIDENT);

        // Dateien schließen
        std::fclose(valuesFile);
        std::fclose(headerFile);
        return n;
    }

    std::unordered_set<id_t> TransactionManager::Transaction::list() {
        return ColumnManager::getInstance()->getColumnIDs();
    }

    std::pair<size_t, size_t> TransactionManager::Transaction::open(id_t id) {
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
        return isOK ? std::make_pair<size_t, size_t>(this->iterators[id]->size(), this->iterators[id]->consumption()) : std::make_pair<size_t, size_t>(0, 0);
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
                    return std::move(bun);
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
        for (id_t id = 0; id < this->iterators.size(); id++) {
            if (this->iterators[id] != 0) {
                this->iterators[id]->undo();
                delete this->iterators[id];
            }
        }
    }

}

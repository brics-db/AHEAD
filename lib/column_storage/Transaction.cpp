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
#include <fstream>

#include <AHEAD.hpp>
#include <ColumnStore.h>
#include <column_storage/TransactionManager.h>
#include <column_storage/Storage.hpp>
#include "../meta_repository/MetaRepositoryManager.h"
#include <util/resilience.hpp>

namespace ahead {

    TransactionManager::Transaction::Transaction(
            bool isUpdater,
            version_t currentVersion)
            : botVersion(currentVersion),
              eotVersion(isUpdater ? (new id_t(std::numeric_limits<id_t>::max())) : (&this->botVersion)),
              isUpdater(isUpdater),
              iterators(),
              iteratorPositions() {
    }

    TransactionManager::Transaction::Transaction(
            const Transaction &copy)
            : Transaction(copy.isUpdater, copy.botVersion) {
    }

    TransactionManager::Transaction::~Transaction() {
        for (auto pIter : iterators) {
            if (pIter) {
                delete pIter;
            }
        }
    }

    TransactionManager::Transaction& TransactionManager::Transaction::operator=(
            const Transaction &copy) {
        new (this) Transaction(copy);
        return *this;
    }

    size_t TransactionManager::Transaction::load(
            const char * const path,
            const char * const tableName,
            const char * const prefix,
            const size_t size,
            const char * const delim,
            const bool ignoreMoreData) {
        std::stringstream sserr;
        if (!this->isUpdater) {
            // Problem : Transaktion darf keine Änderungen vornehmen
            sserr << "TransactionManager::Transaction::load(" << __FILE__ << ":" << __LINE__ << ") Transaktion darf keine Änderungen vornehmen" << std::endl;
            throw std::runtime_error(sserr.str());
        }

        const size_t LEN_PATH = 1024;
        const size_t LEN_LINE = 1024;
        const size_t LEN_VALUE = 256;

        if (path == nullptr) {
            sserr << "TransactionManager::Transaction::load(" << __FILE__ << ":" << __LINE__ << ")  You must provide a path!" << std::endl;
            throw std::runtime_error(sserr.str());
        }
        size_t pathLen = strnlen(path, LEN_PATH);
        if (pathLen == 0 || pathLen > LEN_PATH) {
            sserr << "TransactionManager::Transaction::load(" << __FILE__ << ":" << __LINE__ << ") path is too long (>1024)!" << std::endl;
            throw std::runtime_error(sserr.str());
        }
        const char* actDelim = delim == nullptr ? "|" : delim;

        MetaRepositoryManager *mrm = MetaRepositoryManager::getInstance();
        ColumnManager * cm = ColumnManager::getInstance();

        std::list<ColumnManager::ColumnIterator*> columnIters;

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
        column_type_t type(type_void);
        size_t numValues(0); // line counter
        size_t lenPrefix = prefix ? strlen(prefix) : 0;

        id_t newTableId(0); // unique id of the created table
        char datatype[LEN_VALUE];

        const size_t DEFAULT_RESERVE(32);
        std::vector<id_t> column_IDs;
        column_IDs.reserve(DEFAULT_RESERVE);
        std::vector<char*> column_names;
        column_names.reserve(DEFAULT_RESERVE);
        std::vector<size_t> column_widths;
        column_widths.reserve(DEFAULT_RESERVE);
        std::vector<column_type_t> column_types;
        column_types.reserve(DEFAULT_RESERVE);
        std::vector<size_t> nums_values;
        nums_values.reserve(DEFAULT_RESERVE);

        valuesFile = std::fopen(valuesPath.c_str(), "r");
        headerFile = std::fopen(headerPath.c_str(), "r");

        if (!headerFile) {
            // Problem : Dateien konnten nicht geöffnet werden
            sserr << "TransactionManager::Transaction::load(" << __FILE__ << ":" << __LINE__ << ") Header-Datei konnte nicht geöffnet werden (" << headerPath << ')' << std::endl;
            throw std::runtime_error(sserr.str());
        }

        auto curColumnIDs = cm->getColumnIDs();

        this->open(ColumnManager::ID_BAT_COLNAMES);
        this->open(ColumnManager::ID_BAT_COLTYPES);
        this->open(ColumnManager::ID_BAT_COLIDENT);

        //////////////////
        // Create Table //
        //////////////////
        if (tableName) {
            newTableId = mrm->createTable(tableName);
        }

        ///////////////////////
        // Read Headers File //
        ///////////////////////

        //////////////////////////
        //  * Read Column Names //
        //////////////////////////
        // Read the first line of the header file for the column names
        // Defer column creation until after we checked whether either the actual contents file or the converted data files are present
        std::memset(line, 0, LEN_LINE);
        if (std::fgets(line, LEN_LINE, headerFile) != line) {
            sserr << "TransactionManager::Transaction::load(" << __FILE__ << ":" << __LINE__ << ") Error reading line!" << std::endl;
            throw std::runtime_error(sserr.str());
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
                sserr << "TransactionManager::Transaction::load(" << __FILE__ << ":" << __LINE__ << ") Name of column is too long (>" << (LEN_VALUE - 1) << ")!";
                throw std::runtime_error(sserr.str());
            }

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
            column_names.push_back(attribute_name);

            buffer = std::strtok(nullptr, actDelim);
        }

        //////////////////////////
        //  * Read Column Types //
        //////////////////////////
        // Zeile mit Spaltentypen einlesen aus Header-Datei
        std::memset(line, 0, LEN_LINE);
        if (std::fgets(line, LEN_LINE, headerFile) != line) {
            sserr << "TransactionManager::Transaction::load(" << __FILE__ << ":" << __LINE__ << ") Error reading line" << std::endl;
            throw std::runtime_error(sserr.str());
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
            column_type_t columnType;
            size_t columnWidth(0);
            if (std::strncmp(buffer, "INTEGER", 7) == 0 || std::strncmp(buffer, "INT", 3) == 0) {
                columnType = type_int;
                columnWidth = sizeof(int_t);
            } else if (std::strncmp(buffer, "TINYINT", 7) == 0) {
                columnType = type_tinyint;
                columnWidth = sizeof(tinyint_t);
            } else if (std::strncmp(buffer, "SHORTINT", 8) == 0) {
                columnType = type_shortint;
                columnWidth = sizeof(shortint_t);
            } else if (std::strncmp(buffer, "LARGEINT", 8) == 0) {
                columnType = type_largeint;
                columnWidth = sizeof(bigint_t);
            } else if (std::strncmp(buffer, "STRING", 6) == 0) {
                columnType = type_string;
                size_t maxlen = MAXLEN_STRING;
                if (bufSlen > 6 && buffer[6] == ':') {
                    maxlen = atoi(&buffer[7]);
                }
                columnWidth = sizeof(char_t) * maxlen + 1;
            } else if (std::strncmp(buffer, "FIXED", 5) == 0) {
                columnType = type_fixed;
                columnWidth = sizeof(fixed_t);
            } else if (std::strncmp(buffer, "CHAR", 4) == 0) {
                columnType = type_char;
                columnWidth = sizeof(char_t);
            } else if (std::strncmp(buffer, "RESTINY", 7) == 0) {
                columnType = type_restiny;
                columnWidth = sizeof(restiny_t);
            } else if (std::strncmp(buffer, "RESSHORT", 8) == 0) {
                columnType = type_resshort;
                columnWidth = sizeof(resshort_t);
            } else if (std::strncmp(buffer, "RESINT", 6) == 0) {
                columnType = type_resint;
                columnWidth = sizeof(resint_t);
            } else if (std::strncmp(buffer, "RESBIGINT", 9) == 0) {
                columnType = type_resbigint;
                columnWidth = sizeof(resbigint_t);
            } else {
                sserr << "TransactionManager::Transaction::load(" << __FILE__ << ":" << __LINE__ << ") data type " << buffer << " in header unknown" << std::endl;
                throw std::runtime_error(sserr.str());
            }

            column_types.push_back(columnType);
            column_widths.push_back(columnWidth);

            buffer = std::strtok(nullptr, actDelim);
        }

        ////////////////////////////////////////////////
        // Test if all converted contents files exist //
        ////////////////////////////////////////////////
        const size_t numColumns = column_names.size();
        bool areAllColumnFilesPresent = true;
        std::vector<bool> vecColumnPresent(column_names.size());
        if (AHEAD::getInstance()->isConvertTableFilesOnLoad()) {
            auto columnItersIterator = columnIters.begin();
            for (size_t i = 0; i < column_names.size(); ++i) {
                std::string columnFilePath(path);
                columnFilePath.append("_").append(column_names[i]).append(".ahead");
                std::ifstream columnIStream(columnFilePath);
                vecColumnPresent[i] = columnIStream.is_open();
                areAllColumnFilesPresent &= columnIStream.is_open();
                columnIStream.close();
                ++columnItersIterator;
            }
        }

        //////////////////////////////////////////////////////////////////////////////////////////
        // Test if either converted contents files exist or the text content file is accessible //
        //////////////////////////////////////////////////////////////////////////////////////////
        if (!areAllColumnFilesPresent && !valuesFile) {
            // Problem : Dateien konnten nicht geöffnet werden
            sserr << "TransactionManager::load(@" << __LINE__ << ") Content-Datei konnte nicht geöffnet werden (" << valuesPath << "). Fehlende Spalten sind:\n";
            for (size_t i = 0; i < column_names.size(); ++i) {
                if (!vecColumnPresent[i]) {
                    sserr << '\t' << column_names[i] << '\n';
                }
            }
            throw std::runtime_error(sserr.str());
        }

        ///////////////////////
        // Create Columns    //
        ///////////////////////
        for (size_t i = 0; i < numColumns; ++i) {
            // find next free column ID
            auto column_ID = cm->getNextColumnID();
            column_IDs[i] = column_ID;

            bun = append(ColumnManager::ID_BAT_COLNAMES);
            std::strncpy(static_cast<str_t>(bun.tail), column_names[i], strlen(column_names[i]) + 1);

            bool stdCreate = true;
            switch (column_types[i]) {
                case type_tinyint:
                    std::strncpy(datatype, NAME_TINYINT, LEN_VALUE);
                    break;

                case type_shortint:
                    std::strncpy(datatype, NAME_SHORTINT, LEN_VALUE);
                    break;

                case type_int:
                    std::strncpy(datatype, NAME_INTEGER, LEN_VALUE);
                    break;

                case type_largeint:
                    std::strncpy(datatype, NAME_LARGEINT, LEN_VALUE);
                    break;

                case type_string:
                    std::strncpy(datatype, NAME_STRING, LEN_VALUE);
                    break;

                case type_fixed:
                    std::strncpy(datatype, NAME_FIXED, LEN_VALUE);
                    break;

                case type_char:
                    std::strncpy(datatype, NAME_CHAR, LEN_VALUE);
                    break;

                case type_restiny:
                    std::strncpy(datatype, NAME_RESTINY, LEN_VALUE);
                    cm->createColumn(column_ID,
                            ColumnMetaData(column_widths[i], std::get<7>(*v2_restiny_t::As), std::get<7>(*v2_restiny_t::Ainvs), v2_restiny_t::UNENC_MAX_U, v2_restiny_t::UNENC_MIN));
                    stdCreate = false;
                    break;

                case type_resshort:
                    std::strncpy(datatype, NAME_RESSHORT, LEN_VALUE);
                    cm->createColumn(column_ID,
                            ColumnMetaData(column_widths[i], std::get<15>(*v2_resshort_t::As), std::get<15>(*v2_resshort_t::Ainvs), v2_resshort_t::UNENC_MAX_U, v2_resshort_t::UNENC_MIN));
                    stdCreate = false;
                    break;

                case type_resint:
                    std::strncpy(datatype, NAME_RESINT, LEN_VALUE);
                    cm->createColumn(column_ID, ColumnMetaData(column_widths[i], std::get<15>(*v2_resint_t::As), std::get<15>(*v2_resint_t::Ainvs), v2_resint_t::UNENC_MAX_U, v2_resint_t::UNENC_MIN));
                    stdCreate = false;
                    break;

                case type_resbigint:
                    [[fallthrough]];
                case type_resstring:
                    sserr << "TransactionManager::Transaction::load(@" << __LINE__ << ") data type " << buffer << " in header currently unsupported for load" << std::endl;
                    throw std::runtime_error(sserr.str());

                default:
                    sserr << "TransactionManager::Transaction::load(@" << __LINE__ << ") data type " << buffer << " in header unknown" << std::endl;
                    throw std::runtime_error(sserr.str());
            }

            if (stdCreate) {
                cm->createColumn(column_ID, column_widths[i]);
            }

            bun = append(ColumnManager::ID_BAT_COLTYPES);
            *static_cast<column_type_t*>(bun.tail) = column_types[i];

            // Spaltenidentifikation einpflegen
            bun = append(ColumnManager::ID_BAT_COLIDENT);
            *static_cast<id_t*>(bun.tail) = column_ID;

            this->open(column_ID);
            columnIters.push_back(this->iterators[column_ID]);

            // create attribute for specified table
            if (tableName) {
                mrm->createAttribute(column_names.at(attributeNamesIndex), datatype, column_ID, newTableId);
            }

            attributeNamesIndex++;
        }

        //////////////////////////////////////////////
        // Check for converted data for faster load //
        //////////////////////////////////////////////
        bool areAllColumnsConverted = AHEAD::getInstance()->isConvertTableFilesOnLoad();
        std::vector<bool> vecIscolumnAlreadyLoaded(numColumns);
        nums_values.resize(column_names.size());
        if (AHEAD::getInstance()->isConvertTableFilesOnLoad()) {
            auto columnItersIterator = columnIters.begin();
            for (size_t i = 0; i < column_names.size(); ++i) {
                std::string attrFilePath(path);
                attrFilePath.append("_").append(column_names[i]).append(".ahead");
                std::ifstream attrIStream(attrFilePath);
                areAllColumnsConverted &= attrIStream.is_open();
                if (attrIStream) {
                    nums_values[i] = (*columnItersIterator)->read(attrIStream);
                    if (attrIStream.fail() | attrIStream.bad()) {
                        sserr << "TransactionManager::Transaction::load(@" << __LINE__ << ") error loading binary data from file \"" << attrFilePath << "\"";
                        throw std::runtime_error(sserr.str());
                    }
                    vecIscolumnAlreadyLoaded[i] = true;
                }
                ++columnItersIterator;
            }
        }

        ////////////////////////
        // Read Contents File //
        ////////////////////////
        // Spaltenwerte zeilenweise aus Datei einlesen
        auto columnsMetaData = cm->getColumnMetaData();
        if (!areAllColumnsConverted) {
            numValues = 0;
            std::memset(line, 0, LEN_LINE);
            while (std::fgets(line, LEN_LINE, valuesFile) != 0 && numValues < size) {
                if ((pPos = std::strchr(line, '\n')) != nullptr) {
                    *pPos = 0;
                }
                if (*(pPos - 1) == '\r') {
                    *(pPos - 1) = 0;
                }
                numValues++; // increase line counter

                // Iteratoren für Spaltentypen und Spalteniteratoren zur?cksetzen
                auto columnItersIterator = columnIters.begin();
                auto typesIterator = column_types.begin();

                // Zeile durch Zeichen actDelim in Einzelwerte trennen
                buffer = std::strtok(line, actDelim);
                size_t numVal = 1;
                size_t colIdx = 0;
                while (buffer != nullptr) {
                    if (typesIterator == column_types.end()) {
                        if (ignoreMoreData) {
                            buffer = nullptr;
                            continue;
                        } else {
                            sserr << "TransactionManager::Transaction::load(@" << __LINE__ << ") more values than types registered (#" << numVal << ")!";
                            throw std::runtime_error(sserr.str());
                        }
                    }

                    if (!vecIscolumnAlreadyLoaded[colIdx]) {
                        // Spaltentyp und Spaltenidentifikation bestimmen
                        type = *typesIterator;
                        ci = *columnItersIterator;
                        auto record = ci->append();
                        switch (type) {
                            case type_tinyint:
                                *(static_cast<tinyint_t*>(record.content)) = static_cast<tinyint_t>(std::atoi(buffer));
                                break;

                            case type_shortint:
                                *(static_cast<shortint_t*>(record.content)) = static_cast<shortint_t>(std::atoi(buffer));
                                break;

                            case type_int:
                                *(static_cast<int_t*>(record.content)) = static_cast<int_t>(std::atol(buffer));
                                break;

                            case type_largeint:
                                *(static_cast<bigint_t*>(record.content)) = static_cast<bigint_t>(std::atoll(buffer));
                                break;

                            case type_string:
                                std::strncpy(static_cast<str_t>(record.content), buffer, column_widths[colIdx]);
                                break;

                            case type_fixed:
                                *(static_cast<fixed_t*>(record.content)) = std::atof(buffer);
                                break;

                            case type_char:
                                *(static_cast<char_t*>(record.content)) = buffer[0];
                                break;

                            case type_restiny:
                                *(static_cast<restiny_t*>(record.content)) = static_cast<restiny_t>(std::atol(buffer)) * static_cast<restiny_t>((*columnsMetaData)[column_IDs[colIdx]].AN_A);
                                break;

                            case type_resshort:
                                *(static_cast<resshort_t*>(record.content)) = static_cast<resshort_t>(std::atol(buffer)) * static_cast<resshort_t>((*columnsMetaData)[column_IDs[colIdx]].AN_A);
                                break;

                            case type_resint:
                                *(static_cast<resint_t*>(record.content)) = static_cast<resint_t>(std::atoll(buffer)) * static_cast<resint_t>((*columnsMetaData)[column_IDs[colIdx]].AN_A);
                                break;

                            case type_resbigint:
                                [[fallthrough]];
                            case type_resstring:
                                sserr << "TransactionManager::Transaction::load(@" << __LINE__ << ") data type " << buffer << " in header currently unsupported for load" << std::endl;
                                throw std::runtime_error(sserr.str());

                            default:
                                sserr << "TransactionManager::Transaction::load(@" << __LINE__ << ") data type unknown" << std::endl;
                                throw std::runtime_error(sserr.str());
                        }
                    }

                    ++colIdx;
                    ++numVal;
                    typesIterator++;
                    columnItersIterator++;

                    buffer = std::strtok(nullptr, actDelim);
                }
                std::memset(line, 0, LEN_LINE);
            }
            for (size_t i = 0; i < column_names.size(); ++i) {
                if (!vecIscolumnAlreadyLoaded[i]) {
                    nums_values[i] = numValues;
                }
            }
        }
        delete columnsMetaData;

        /////////////////////////////////////////
        // Write newly converted Contents File //
        /////////////////////////////////////////
        if (AHEAD::getInstance()->isConvertTableFilesOnLoad() && !areAllColumnsConverted) {
            auto columnItersIterator = columnIters.begin();
            for (size_t i = 0; i < numColumns; ++i) {
                if (!vecIscolumnAlreadyLoaded[i]) {
                    std::string attrFilePath(path);
                    attrFilePath.append("_").append(column_names[i]).append(".ahead");
                    std::ofstream attrOStream(attrFilePath);
                    (*columnItersIterator)->write(attrOStream);
                    attrOStream.flush();
                }
                ++columnItersIterator;
            }
        }

        ////////////////////////////
        // Close metadata columns //
        ////////////////////////////
        this->close(ColumnManager::ID_BAT_COLNAMES);
        this->close(ColumnManager::ID_BAT_COLTYPES);
        this->close(ColumnManager::ID_BAT_COLIDENT);

        ////////////////////////
        // Close data columns //
        ////////////////////////
        for (auto columnID : column_IDs) {
            this->close(columnID);
        }

        ////////////////////////////////
        // Close remaining open files //
        ////////////////////////////////
        if (valuesFile) {
            std::fclose(valuesFile);
        }
        std::fclose(headerFile);

        ////////////////////////////////////////////
        // Test if all columns have the same size //
        ////////////////////////////////////////////
        size_t previousNumValues = nums_values[0];
        for (size_t i = 1; i < column_names.size(); ++i) {
            if (nums_values[i] != previousNumValues) {
                std::stringstream sserr;
                sserr << "Transaction::load(@" << __FILE__ << ':' << __LINE__ << ") Column sizes don't match! previous (" << (i - 1) << ") = " << previousNumValues << ", current (" << i << ") = "
                        << nums_values[i];
                throw std::runtime_error(sserr.str().c_str());
            }
            previousNumValues = nums_values[i];
        }

        return nums_values[0];
    }

    std::unordered_set<id_t> TransactionManager::Transaction::list() {
        return ColumnManager::getInstance()->getColumnIDs();
    }

    std::pair<size_t, size_t> TransactionManager::Transaction::open(
            id_t id) {
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

    void TransactionManager::Transaction::close(
            id_t id) {
        if (id >= this->iterators.size() || (id < this->iterators.size() && this->iterators[id] == nullptr)) {
            // Problem : Spalte nicht geoeffnet
        } else {
            this->iteratorPositions[id] = -1;
        }
    }

    TransactionManager::BinaryUnit TransactionManager::Transaction::next(
            id_t id) {
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

    TransactionManager::BinaryUnit TransactionManager::Transaction::get(
            id_t id,
            oid_t index) {
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

    TransactionManager::BinaryUnit TransactionManager::Transaction::edit(
            id_t id) {
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

    TransactionManager::BinaryUnit TransactionManager::Transaction::append(
            id_t id) {
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

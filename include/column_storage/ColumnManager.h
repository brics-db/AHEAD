// Copyright (c) 2010 Julian Hollender
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

/**
 * @author Julian Hollender
 * @date 20.07.2010
 *
 */

#ifndef COLUMNMANAGER_H
#define COLUMNMANAGER_H

#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <memory>

#include <ColumnStore.h>
#include <column_storage/BucketManager.h>
#include <column_storage/ColumnMetaData.hpp>

namespace ahead {

    /**
     * @brief Klasse zur Verwaltung von Spalten mit Einträgen fester Größe
     *
     * Die Klasse verwaltet Spalten mit Einträgen fester Größen, sogenannte Records. Die Records einer Spalte werden in Buckets abgelegt, die durch die Klasse BucketManager verwaltet werden. Jegliche Änderungen an einer Spalte werden nach dem Mehrversionen-Konzept archiviert, wodurch keine Synchronisation zwischen mehreren Lesern notwendig ist. Die Klasse implementiert das Singleton-Pattern, wodurch sichergestellt wird, dass maximal ein Objekt der Klasse existiert.
     */
    class ColumnManager {

        friend class TransactionManager;
        friend class AHEAD;

    public:
        static const size_t BAT_COLNAMES_MAXLEN;
        static const id_t ID_BAT_COLNAMES;
        static const id_t ID_BAT_COLTYPES;
        static const id_t ID_BAT_COLIDENT;
        static const id_t ID_BAT_FIRST_USER;

        /**
         * Die Datenstruktur kapselt einen Zeiger auf den Speicherbereich fester Größe eines Records.
         */
        struct Record {

            void *content;

            Record(
                    void* content)
                    : content(content) {
            }
        };

        /**
         * @brief Klasse zum Lesen und Editieren der Records einer Spalte
         *
         * Die Klasse stellt eine Möglichekeit zur Verfügung eine Spalte nach dem Open/Next/Close-Prinzip zu lesen und editieren.
         * Beim Erzeugen der Klasse muss ein Zeiger auf eine Version übergeben werden, dessen Inhalt, im folgenden Version des
         * Iterators genannt, angibt welche Records das Open/Next/Close-Interface liefert. Man liest daraufhin die Records mit
         * der größten Versionnummer kleiner oder gleich der Version des Iterators. Hierbei ist zu beachten, dass sich die Version
         * des Iterators während der kompletten Lebensdauer eines Objekts dieser Klasse nicht ändern darf, da man ggf. falsche
         * Daten geliefert bekommt und bei Änderungen die komplette Datenbasis zerstören kann. Außerdem ist darauf zu achten, dass
         * zu einem festen Zeitpunkt maximal einen Iterator der Änderung durchgeführt hat oder Änderungen durchfphren wird pro
         * Spalte gibt. Operationen zur Änderung der Datenbasis dürfen nur aufgerufen werden, falls die Version des Iterators größer
         * als die aktuellste Version ist. Der Speicher für die Version eines Iterators, welcher Änderungen an der Datenbasis
         * vollzogen hat, darf nach Zerstörung des Objektes nicht freigegeben werden und der Inhalt darf auf eine Nummer größer als
         * die aktuelle Version verändert werden. Der Speicher für die Version eines Iterators, der ausschließlich lesend auf die
         * Datenbasis zugegriffen hat, kann nach Zerstörung des Objekts freigegeben werden.
         */
        class ColumnIterator {

            friend class ColumnManager;

        public:
            /**
             * @author Julian Hollender
             *
             * @return Anzahl der Records innerhalb der Spalte
             *
             * Die Funktion gibt die Anzahl der sichtbaren Records innerhalb der Spalte zurück.
             */
            size_t size();

            /**
             * @author Till Kolditz
             *
             * @return Amount of storage actually allocated for this column
             */
            size_t consumption();

            /**
             * @author Julian Hollender
             *
             * @return Zeiger auf Inhalt des nächsten Records innerhalb der Spalte
             *
             * Die Funktion gibt einen Zeiger auf den Inhalt des nächsten Records innerhalb der Spalte zurück. Falls keine weiteren Records
             * vorhanden sein sollten, wird ein NULL-Zeiger zurückgegeben. Um die Datenintegrität zu erhalten, darf der Inhalt des Records
             * nicht verändert werden.
             */
            Record next();
            /**
             * @author Julian Hollender
             *
             * @param index Position innerhalb der Spalte
             *
             * @return Zeiger auf den Record an der übergebenen Position innerhalb der Spalte
             *
             * Die Funktion gibt einen Zeiger auf den Record an der übergebenen Position zurück. Hierbei ist zu beachten, dass die Nummerierung
             * der Positionen innerhalb einer Spalte bei 0 beginnt. Falls zur übergebenen Position kein entsprechender Record gefunden wurde,
             * wird rewind() aufgerufen und ein NULL-Zeiger zurückgegeben. Um die Datenintegrität zu erhalten, darf der Inhalt des Records
             * nicht verändert werden.
             */
            Record seek(
                    oid_t index);
            /**
             * @author Julian Hollender
             *
             * Die Funktion setzt den Iterator in seine Ausgangsposition zurück, wodurch beim nächsten Aufruf der Funktion next() wieder
             * der Record an erster Position in der Spalte zurückgegeben wird. Mögliche Änderungen an der Datenbasis werden nicht rückgängig gemacht.
             */
            void rewind();

            /**
             * @author Julian Hollender
             *
             * @return Zeiger auf den Record an der aktuellen Position
             *
             * Die Funktion liefert einen Zeiger auf den Record an der aktuellen Position zurück, dessen Inhalt geändert werden darf.
             * Hierbei wird der Zeiger für die Version des Records auf die Version des Iterators gesetzt. Ein erneuter Aufruf der Funktion
             * während der Lebenszeit des Iterator-Objektes an der gleichen Position in der Spalte liefert einen Zeiger auf die gleiche
             * Speicherposition zurück.
             */
            Record edit();

            /**
             * @author Julian Hollender
             *
             * @return Zeiger auf einen Record, der an das Ende des Bucket-Streams angehängt wurde
             *
             * Die Funktion liefert einen Zeiger auf einen Record, der an das Ende der Spalte angehängt wurde. Hierbei wird der Zeiger für
             * die Version des Records auf die Version des Iterators gesetzt. Nach dem Aufruf steht der Iterator auf dem neu angehängten
             * Record. Ein erneutes Aufrufen der Funktion edit() würde also einen Zeiger auf den gleiche Speicherbereich liefern.
             */
            Record append();

            /**
             * @author Till Kolditz
             *
             * @param content Pointer to the contents
             * @param lenContent length of the content array in BYTES
             * @param dataSizeInBits length of one data item in BYTES
             * @return number of read values
             *
             * BULK inserts the storage and automatically splits it into bucket sizes.
             */
            size_t read(
                    std::istream & istream);

            /**
             * @author Till Kolditz
             */
            void write(
                    std::ostream & ostream);

            /**
             * @author Julian Hollender
             *
             * Die Funktion nimmt alle bisher durchgeführten Änderungen des Iterators zurück und setzt anschließend den Iterator in seine
             * Ausgangsposition zurück, wodurch beim nächsten Aufruf der Funktion next() wieder den Record an erster Position in der Spalte
             * zurückgegeben wird.
             */
            void undo();

        private:
            BucketManager::BucketIterator *iterator;
            ColumnMetaData columnMetaData;
            BucketManager::Chunk *currentChunk;
            oid_t currentPosition;
            const oid_t recordsPerBucket;

            ColumnIterator(
                    ColumnMetaData & columnMetaData,
                    BucketManager::BucketIterator *iterator);
            ColumnIterator(
                    const ColumnIterator & copy);

        public:
            virtual ~ColumnIterator();
            ColumnIterator& operator=(
                    const ColumnIterator & copy);
        };

        /**
         * @author Julian Hollender
         *
         * @return Zeiger auf einziges Objekt der Klasse ColumnManager
         *
         * Die Funktion liefert einen Zeiger auf das einzig existierende Objekt der Klasse. Falls noch kein Objekt der Klasse existiert, wird ein Objekt erzeugt und anschließend ein Zeiger auf das Objekt zurückgegeben.
         */
        static std::shared_ptr<ColumnManager> getInstance();

        /**
         * @author Julian Hollender
         *
         * @param id Identifikationsnummer der zu öffnenden Spalte
         * @param version Zeiger auf Version, die angibt welche Records sichtbar für Iterator sind
         * @return Zeiger auf ColumnIterator zur Spalte mit Identifikationsnummer id
         *
         * Die Funktion erzeugt ein Objekt der Klasse ColumnIterator zum Bearbeiten einer Spalte mit der Identifikationsnummer id. Hierbei sind nur die Records mit der größten Versionsnummer kleiner oder gleich dem Inhalt des Zeigers version sichtbar. Bei jeder Änderung am Datenbestand durch den erzeugten ColumnIterator, wird der Zeiger version in die Verwaltungsstrukturen der Spalte kopiert. Daher darf der Speicher, auf den der Zeiger version zeigt, nach Änderungen an der Datenbasis nicht mehr freigegeben werden. Falls eine Spalte mit der übergebenen Identifikationsnummer nicht existiert, wird ein NULL-Zeiger zurückgegeben. Es ist darauf zu achten, dass zu einem festen Zeitpunkt maximal einen Iterator der Änderung durchgeführt hat oder Änderungen durchführen wird pro Spalte gibt.
         */
        ColumnIterator* openColumn(
                id_t id,
                std::shared_ptr<version_t> & version);

        /**
         * @author Julian Hollender
         *
         * @return Menge von Identifikationsnummern von Spalten
         *
         * Die Funktion liefert die Menge von Identifikationsnummern aller existierenden Spalten.
         */
        std::unordered_set<id_t> getColumnIDs();

        std::unordered_map<id_t, ColumnMetaData> * getColumnMetaData();

        ColumnMetaData getColumnMetaData(
                id_t id);

        /**
         * @author Till Kolditz
         *
         * @return the next available column ID for inserting
         */
        id_t getNextColumnID();

        /**
         * @author Julian Hollender
         *
         * Die Funktion legt eine leere Spalte mit der Identifikationsnummer id und Spaltenbreite width, d.h. die Größe eines enthaltenden Records, an. Falls bereits eine Spalte mit der Identifikationsnummer id existiert, wird keine Operation ausgeführt.
         */
        ColumnMetaData & createColumn(
                id_t id,
                uint32_t width);

        ColumnMetaData & createColumn(
                id_t id,
                ColumnMetaData && column);

    private:

        static std::shared_ptr<ColumnManager> instance;

        static void destroyInstance();

        std::unordered_map<id_t, ColumnMetaData> columnMetaData;

        std::atomic<id_t> nextID;

        ColumnManager();
        ColumnManager(
                const ColumnManager &copy);

    public:
        virtual ~ColumnManager();
    };

}

#endif


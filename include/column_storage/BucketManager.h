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

/**
 * @author Julian Hollender
 * @date 20.07.2010
 *
 * @todo Speicherallokation auf Memory-Pool-Manager (wie z.B. Boost Pool Library) umstellen
 * @todo Exception-Framework einsetzen
 * @todo Concurrency-Framework einsetzen (insbesondere Ersetzung von Kommentaren der Art "// Atomic Block Start" etc.)
 * @todo Notwendigkeit von older in Datenstruktur Bucket überdenken
 */

#ifndef BUCKETMANAGER_H
#define BUCKETMANAGER_H

#include <unordered_map>
#include <list>
#include <stack>
#include <vector>
#include <cstdlib>
#include <cstring>

#include <ColumnStore.h>

#ifdef DEBUG
#include <iostream>
#endif

namespace ahead {
    /**
     * Die Konstante bestimmt die Anzahl der Bytes die für einen Bucket reserviert werden.
     */
#define CHUNK_CONTENT_SIZE (2 * 1024 * 1024)

    /**
     * @brief Klasse zur Verwaltung von Bucket-Streams mit Buckets fester Größe
     *
     * Die Klasse verwaltet einfach verkettete Listen von Speicherbereichen fester Größe, sogenannte Bucket-Streams.
     * Jegliche Änderungen an einem Bucket-Stream werden nach dem Mehrversionen-Konzept archiviert, wodurch keine
     * Synchronisation zwischen mehreren Lesern notwendig ist. Die Klasse implemtiert das Singleton-Pattern, wodurch
     * sichergestellt wird, dass maximal ein Objekt der Klasse existiert.
     */
    class BucketManager {

    public:
        friend class TransactionManager;

        /**
         * Die Datenstruktur kapselt einen Zeiger auf einen Speicherbereich fester Größe für die Datenstruktur Bucket.
         */
        struct Chunk {

            void *content;

            Chunk();

            Chunk(
                    void* content);

            Chunk(
                    const Chunk &);

            Chunk& operator=(
                    const Chunk &);
        };

        /**
         * Die Datenstruktur stellt den Zustand eines Buckets zu einer festen Version dar und beinhaltet neben einem
         * Zeiger auf einen Chunk auch einen Zeiger auf den im Bucket-Stream nachfolgenden Bucket, sowie jeweils einen
         * Zeiger auf die nächst aktuellere und die ältere Version des Buckets. Außerdem enthält die Datenstruktur die
         *  Version und die Position eines Buckets innerhalb des Bucket-Stream.
         */
        struct Bucket {

            id_t number;
            version_t *version;
            Bucket *next, *older, *newer;
            Chunk *chunk;

            Bucket();

            Bucket(
                    id_t number,
                    version_t *version,
                    Bucket *next,
                    Bucket *older,
                    Bucket* newer,
                    Chunk *chunk);

            Bucket(
                    const Bucket &);

            virtual ~Bucket();

            Bucket& operator=(
                    const Bucket &);
        };

        /**
         * Die Datenstruktur enthält jeweils einen Zeiger auf den Anfang und das Ende einer einfach verketteten Liste von Buckets.
         * Hierbei ist zu beachten, dass nur die aktuellste Version direkt miteinander verkettet ist und man für ältere Versionen
         * (z.B. die im Bezug auf Datenbanken konsistente Version) mittels der Zusatzzeiger in der Datenstruktur Bucket absteigen
         * muss. Zusätzlich enthält die Datenstruktur einen Vektor der für jede Position innerhalb des Bucket-Streams einen Zeiger
         * auf die aktuellste Version des Buckets an der entsprechenden Position bereitstellt.
         */
        struct BucketStream {

            Bucket *head, *tail;
            std::vector<Bucket*> index;
            size_t size;

            BucketStream();

            BucketStream(
                    Bucket *head,
                    Bucket *tail,
                    std::vector<Bucket*> & index,
                    size_t size);

            BucketStream(
                    const BucketStream &);

            BucketStream& operator=(
                    const BucketStream &);
        };

        /**
         * @brief Klasse zum Lesen und Editieren eines Bucket-Streams
         *
         * Die Klasse stellt eine Möglichekeit zur Verfügung einen Bucket-Stream nach dem Open/Next/Close-Prinzip zu lesen und editieren.
         * Beim Erzeugen der Klasse muss ein Zeiger auf eine Version übergeben werden, dessen Inhalt, im folgenden Version des Iterators
         * genannt, angibt welche Buckets das Open/Next/Close-Interface liefert. Man liest daraufhin die Buckets mit der größten
         * Versionnummer kleiner oder gleich der Version des Iterators. Hierbei ist zu beachten, dass sich die Version des Iterators
         * während der kompletten Lebensdauer eines Objekts dieser Klasse nicht ändern darf, da man ggf. falsche Daten geliefert bekommt
         * und bei Änderungen die komplette Datenbasis zerstören kann. Außerdem ist darauf zu achten, dass zu einem festen Zeitpunkt
         * maximal einen Iterator der Änderung durchgeführt hat oder Änderungen durchführen wird pro Bucket-Stream gibt. Operationen zur
         * Änderung der Datenbasis dürfen nur aufgerufen werden, falls die Version des Iterators größer als die aktuellste Version ist.
         * Der Speicher für die Version eines Iterators, welcher Änderungen an der Datenbasis vollzogen hat, darf nach Zerstörung des
         * Objektes nicht freigegeben werden und der Inhalt darf auf eine Nummer größer als die aktuelle Version verändert werden.
         * Der Speicher für die Version eines Iterators, der ausschließlich lesend auf die Datenbasis zugegriffen hat, kann nach
         * Zerstörung des Objekts freigegeben werden.
         */
        class BucketIterator {

            friend class BucketManager;

        public:
            /**
             * @author Julian Hollender
             *
             * @return Anzahl der Buckets innerhalb des Bucket-Streams
             *
             * Die Funktion gibt die Anzahl der sichtbaren Buckets innerhalb des Bucket-Streams zurück.
             */
            size_t countBuckets();
            /**
             * @author Julian Hollender
             *
             * @return aktuelle Position innerhalb des Bucket-Streams
             *
             * Die Funktion gibt die Position des Buckets innerhalb des Bucket-Streams zurück, welcher beim nächsten Aufruf der Funktion next()
             * zurückgegeben wird. Falls das Ende des Bucket-Streams erreicht wurde, wird die Anzahl der sichtbaren Buckets innerhalb des
             * Bucket-Streams zurückgegeben. Hierbei ist zu beachten, dass die Nummerierung der Positionen innerhalb eines Bucket-Streams bei 0 beginnt.
             */
            size_t position();

            /**
             * @author Julian Hollender
             *
             * Die Funktion setzt den Iterator in seine Ausgangsposition zurück, wodurch beim nächsten Aufruf der Funktion next() wieder der
             * Bucket an erster Position im Bucket-Stream zurückgegeben wird. Mögliche Änderungen an der Datenbasis werden nicht rückgängig gemacht.
             */
            void rewind();
            /**
             * @author Julian Hollender
             *
             * @return Zeiger auf Inhalt des nächsten Buckets innerhalb des Bucket-Streams
             *
             * Die Funktion gibt einen Zeiger auf den Inhalt des nächsten Buckets innerhalb des Bucket-Streams zurück. Falls keine weiteren Buckets
             * vorhanden sein sollten, wird ein NULL-Zeiger zurückgegeben. Um die Datenintegrität zu erhalten, darf der Inhalt des Buckets nicht verändert werden.
             */
            Chunk* next();
            /**
             * @author Julian Hollender
             *
             * @param number Position innerhalb des Bucket-Streams
             *
             * @return Zeiger auf den Inhalt des Buckets an der übergebenen Position innerhalb des Bucket-Streams
             *
             * Die Funktion gibt einen Zeiger auf den Inhalt des Buckets an der übergebenen Position zurück. Hierbei ist zu beachten, dass die Nummerierung
             * der Positionen innerhalb eines Bucket-Streams bei 0 beginnt. Falls zur übergebenen Position kein entsprechender Bucket gefunden wurde, wird
             * ein NULL-Zeiger zurückgegeben. Um die Datenintegrität zu erhalten, darf der Inhalt des Buckets nicht verändert werden.
             */
            Chunk* seek(
                    size_t number);

            /**
             * @author Julian Hollender
             *
             * @return Zeiger auf den Inhalt des Buckets an der aktuellen Position
             *
             * Die Funktion liefert einen Zeiger auf den Inhalt des Buckets an der aktuellen Position zurück, dessen Inhalt geändert werden darf.
             * Hierbei wird der Zeiger für die Version des Buckets auf die Version des Iterators gesetzt. Ein erneuter Aufruf der Funktion während
             * der Lebenszeit des Iterator-Objektes an der gleichen Position im Bucket-Stream liefert einen Zeiger auf die gleiche Speicherposition zurück.
             */
            Chunk* edit();
            /**
             * @author Julian Hollender
             *
             * @return Zeiger auf den Inhalt eines Buckets, der an das Ende des Bucket-Streams angehängt wurde
             *
             * Die Funktion liefert einen Zeiger auf den Inhalt eines Buckets, der an das Ende des Bucket-Streams angehängt wurde. Hierbei wird der Zeiger
             * für die Version des Buckets auf die Version des Iterators gesetzt. Nach dem Aufruf steht der Iterator auf dem neu angehängten Bucket.
             * Ein erneutes Aufrufen der Funktion edit() würde also einen Zeiger auf den gleiche Speicherbereich liefern.
             */
            Chunk* append();

            /**
             * @author Julian Hollender
             *
             * Die Funktion nimmt alle bisher durchgeführten Änderungen des Iterators zurück und setzt anschließend den Iterator in seine Ausgangsposition
             * zurück, wodurch beim nächsten Aufruf der Funktion next() wieder der Bucket an erster Position im Bucket-Stream zurückgegeben wird.
             */
            void undo();

#ifdef DEBUG
            static void printBucket (Bucket *bucket);
            static void printBucketStream (BucketStream *stream);
            void printDebugInformation ();
#endif

        private:
            BucketStream *stream;
            version_t *version;

            Bucket *currentBucket;
            Bucket *previousBucket;

            /**
             * Der Stack wird benötigt um die Undo-Funktionalität zu realisieren. Bei jedem Aufruf der Funktion append() oder edit() auf ein Bucket,
             * welches bisher nicht von einem BucketIterator mit der gleichen Version (Referenzgleichheit) angelegt oder geändert wurde, wird ein
             * Zeiger auf den Vorgängerbucket auf dem Stack abgelegt.
             */
            std::stack<Bucket*> log;

            BucketIterator(
                    BucketStream *stream,
                    version_t *version);
            BucketIterator(
                    const BucketIterator &copy);
            virtual ~BucketIterator();

            BucketIterator& operator=(
                    const BucketIterator& copy);

            friend class ColumnManager;
        };

        /**
         * @author Julian Hollender
         *
         * @return Zeiger auf einziges Objekt der Klasse BucketManager
         *
         * Die Funktion liefert einen Zeiger auf das einzig existierende Objekt der Klasse. Falls noch kein Objekt der Klasse existiert, wird ein
         * Objekt erzeugt und anschließend ein Zeiger auf das Objekt zurückgegeben.
         */
        static BucketManager* getInstance();

        /**
         * @author Julian Hollender
         *
         * @param id Identifikationsnummer des zu öffnenden BucketStreams
         * @param version Zeiger auf Version, die angibt welche Buckets sichtbar für Iterator sind
         * @return Zeiger auf BucketIterator zum BucketStream mit Identifikationsnummer id
         *
         * Die Funktion erzeugt ein Objekt der Klasse BucketIterator zum Bearbeiten eines BucketStreams mit der Identifikationsnummer id.
         * Hierbei sind nur die aktuellsten Buckets, deren Versionsnummer kleiner oder gleich dem Inhalt des Zeigers version ist, sichtbar.
         * Bei jeder Änderung am Datenbestand durch den erzeugten BucketIterator, wird der Zeiger version in die Verwaltungsstrukturen des
         * BucketStreams kopiert. Daher darf der Speicher, auf den der Zeiger version zeigt, nach Änderungen an der Datenbasis nicht mehr
         * freigegeben werden. Falls ein BucketStream mit der übergebenen Identifikationsnummer bisher nicht existiert, wird ein leerer
         * BucketStream angelegt. Es ist darauf zu achten, dass zu einem festen Zeitpunkt maximal einen Iterator der Änderung durchgeführt
         * hat oder Änderungen durchführen wird pro BucketStream gibt.
         */
        BucketIterator* openStream(
                id_t id,
                version_t *version);

#ifdef DEBUG
        void printDebugInformation ();
#endif

    private:
        static BucketManager *instance;

        static void destroyInstance();

        std::unordered_map<id_t, BucketStream> streams;

        BucketManager();
        BucketManager(
                const BucketManager &copy);
        virtual ~BucketManager();
    };

}

#endif

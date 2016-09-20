/**
 * @author Julian Hollender
 * @date 20.07.2010
 *
 * @todo Exception-Framework einsetzen
 * @todo Concurrency-Framework einsetzen (insbesondere Ersetzung von Kommentaren der Art "// Atomic Block Start" etc.)
 * @todo Art der Ablage der Records in Buckets �berdenken
 * @todo Typisierung von Spalten? (evtl. �ber Templates)
 */

#ifndef COLUMNMANAGER_H
#define COLUMNMANAGER_H

#include <set>

#include <ColumnStore.h>
#include "column_storage/BucketManager.h"

/**
 * @brief Klasse zur Verwaltung von Spalten mit Eintr�gen fester Gr��e
 *
 * Die Klasse verwaltet Spalten mit Eintr�gen fester Gr��en, sogenannte Records. Die Records einer Spalte werden in Buckets abgelegt, die durch die Klasse BucketManager verwaltet werden. Jegliche �nderungen an einer Spalte werden nach dem Mehrversionen-Konzept archiviert, wodurch keine Synchronisation zwischen mehreren Lesern notwendig ist. Die Klasse implementiert das Singleton-Pattern, wodurch sichergestellt wird, dass maximal ein Objekt der Klasse existiert.
 */
class ColumnManager {
public:
    friend class TransactionManager;

    /**
     * Die Datenstruktur enth�lt die Metadaten einer Spalte, wie etwa die Gr��e eines Records innerhalb dieser Spalte.
     */
    struct Column {
        unsigned int width;
    };

    /**
     * Die Datenstruktur kapselt einen Zeiger auf den Speicherbereich fester Gr��e eines Records.
     */
    struct Record {
        void *content;

        Record(void* content) : content(content) {
        }
    };

    /**
     * @brief Klasse zum Lesen und Editieren der Records einer Spalte
     *
     * Die Klasse stellt eine M�glichekeit zur Verf�gung eine Spalte nach dem Open/Next/Close-Prinzip zu lesen und editieren. Beim Erzeugen der Klasse muss ein Zeiger auf eine Version �bergeben werden, dessen Inhalt, im folgenden Version des Iterators genannt, angibt welche Records das Open/Next/Close-Interface liefert. Man liest daraufhin die Records mit der gr��ten Versionnummer kleiner oder gleich der Version des Iterators. Hierbei ist zu beachten, dass sich die Version des Iterators w�hrend der kompletten Lebensdauer eines Objekts dieser Klasse nicht �ndern darf, da man ggf. falsche Daten geliefert bekommt und bei �nderungen die komplette Datenbasis zerst�ren kann. Au�erdem ist darauf zu achten, dass zu einem festen Zeitpunkt maximal einen Iterator der �nderung durchgef�hrt hat oder �nderungen durchf�hren wird pro Spalte gibt. Operationen zur �nderung der Datenbasis d�rfen nur aufgerufen werden, falls die Version des Iterators gr��er als die aktuellste Version ist. Der Speicher f�r die Version eines Iterators, welcher �nderungen an der Datenbasis vollzogen hat, darf nach Zerst�rung des Objektes nicht freigegeben werden und der Inhalt darf auf eine Nummer gr��er als die aktuelle Version ver�ndert werden. Der Speicher f�r die Version eines Iterators, der ausschlie�lich lesend auf die Datenbasis zugegriffen hat, kann nach Zerst�rung des Objekts freigegeben werden.
     */
    class ColumnIterator {
        friend class ColumnManager;

    public:
        /**
         * @author Julian Hollender
         *
         * @return Anzahl der Records innerhalb der Spalte
         *
         * Die Funktion gibt die Anzahl der sichtbaren Records innerhalb der Spalte zur�ck.
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
         * @return Zeiger auf Inhalt des n�chsten Records innerhalb der Spalte
         *
         * Die Funktion gibt einen Zeiger auf den Inhalt des n�chsten Records innerhalb der Spalte zur�ck. Falls keine weiteren Records vorhanden sein sollten, wird ein NULL-Zeiger zur�ckgegeben. Um die Datenintegrit�t zu erhalten, darf der Inhalt des Records nicht ver�ndert werden.
         */
        Record&& next();
        /**
         * @author Julian Hollender
         *
         * @param index Position innerhalb der Spalte
         *
         * @return Zeiger auf den Record an der �bergebenen Position innerhalb der Spalte
         *
         * Die Funktion gibt einen Zeiger auf den Record an der �bergebenen Position zur�ck. Hierbei ist zu beachten, dass die Nummerierung der Positionen innerhalb einer Spalte bei 0 beginnt. Falls zur �bergebenen Position kein entsprechender Record gefunden wurde, wird rewind() aufgerufen und ein NULL-Zeiger zur�ckgegeben. Um die Datenintegrit�t zu erhalten, darf der Inhalt des Records nicht ver�ndert werden.
         */
        Record&& seek(oid_t index);
        /**
         * @author Julian Hollender
         *
         * Die Funktion setzt den Iterator in seine Ausgangsposition zur�ck, wodurch beim n�chsten Aufruf der Funktion next() wieder der Record an erster Position in der Spalte zur�ckgegeben wird. M�gliche �nderungen an der Datenbasis werden nicht r�ckg�ngig gemacht.
         */
        void rewind();

        /**
         * @author Julian Hollender
         *
         * @return Zeiger auf den Record an der aktuellen Position
         *
         * Die Funktion liefert einen Zeiger auf den Record an der aktuellen Position zur�ck, dessen Inhalt ge�ndert werden darf. Hierbei wird der Zeiger f�r die Version des Records auf die Version des Iterators gesetzt. Ein erneuter Aufruf der Funktion w�hrend der Lebenszeit des Iterator-Objektes an der gleichen Position in der Spalte liefert einen Zeiger auf die gleiche Speicherposition zur�ck.
         */
        Record&& edit();
        /**
         * @author Julian Hollender
         *
         * @return Zeiger auf einen Record, der an das Ende des Bucket-Streams angeh�ngt wurde
         *
         * Die Funktion liefert einen Zeiger auf einen Record, der an das Ende der Spalte angeh�ngt wurde. Hierbei wird der Zeiger f�r die Version des Records auf die Version des Iterators gesetzt. Nach dem Aufruf steht der Iterator auf dem neu angeh�ngten Record. Ein erneutes Aufrufen der Funktion edit() w�rde also einen Zeiger auf den gleiche Speicherbereich liefern.
         */
        Record&& append();

        /**
         * @author Julian Hollender
         *
         * Die Funktion nimmt alle bisher durchgef�hrten �nderungen des Iterators zur�ck und setzt anschlie�end den Iterator in seine Ausgangsposition zur�ck, wodurch beim n�chsten Aufruf der Funktion next() wieder den Record an erster Position in der Spalte zur�ckgegeben wird.
         */
        void undo();

    private:
        BucketManager::BucketIterator *iterator;
        Column *column;
        BucketManager::Chunk *currentChunk;
        oid_t currentPosition;
        const oid_t recordsPerBucket;

        ColumnIterator(Column *column, BucketManager::BucketIterator *iterator);
        ColumnIterator(const ColumnIterator &copy);

    public:
        virtual ~ColumnIterator();
    };

    /**
     * @author Julian Hollender
     *
     * @return Zeiger auf einziges Objekt der Klasse ColumnManager
     *
     * Die Funktion liefert einen Zeiger auf das einzig existierende Objekt der Klasse. Falls noch kein Objekt der Klasse existiert, wird ein Objekt erzeugt und anschlie�end ein Zeiger auf das Objekt zur�ckgegeben.
     */
    static ColumnManager* getInstance();

    /**
     * @author Julian Hollender
     *
     * @param id Identifikationsnummer der zu �ffnenden Spalte
     * @param version Zeiger auf Version, die angibt welche Records sichtbar f�r Iterator sind
     * @return Zeiger auf ColumnIterator zur Spalte mit Identifikationsnummer id
     *
     * Die Funktion erzeugt ein Objekt der Klasse ColumnIterator zum Bearbeiten einer Spalte mit der Identifikationsnummer id. Hierbei sind nur die Records mit der gr��ten Versionsnummer kleiner oder gleich dem Inhalt des Zeigers version sichtbar. Bei jeder �nderung am Datenbestand durch den erzeugten ColumnIterator, wird der Zeiger version in die Verwaltungsstrukturen der Spalte kopiert. Daher darf der Speicher, auf den der Zeiger version zeigt, nach �nderungen an der Datenbasis nicht mehr freigegeben werden. Falls eine Spalte mit der �bergebenen Identifikationsnummer nicht existiert, wird ein NULL-Zeiger zur�ckgegeben. Es ist darauf zu achten, dass zu einem festen Zeitpunkt maximal einen Iterator der �nderung durchgef�hrt hat oder �nderungen durchf�hren wird pro Spalte gibt.
     */
    ColumnIterator* openColumn(id_t id, version_t *version);
    /**
     * @author Julian Hollender
     *
     * @return Menge von Identifikationsnummern von Spalten
     *
     * Die Funktion liefert die Menge von Identifikationsnummern aller existierenden Spalten.
     */
    std::set<id_t> listColumns();
    /**
     * @author Julian Hollender
     *
     * Die Funktion legt eine leere Spalte mit der Identifikationsnummer id und Spaltenbreite width, d.h. die Gr��e eines enthaltenden Records, an. Falls bereits eine Spalte mit der Identifikationsnummer id existiert, wird keine Operation ausgef�hrt.
     */
    void createColumn(id_t id, size_t width);

private:
    static ColumnManager *instance;

    static void destroyInstance();

    std::map<id_t, Column> columns;

    ColumnManager();
    ColumnManager(const ColumnManager &copy);
    virtual ~ColumnManager();
};

#endif


/**
 * @author Julian Hollender
 * @date 20.07.2010
 *
 * @todo Speicherallokation auf Memory-Pool-Manager (wie z.B. Boost Pool Library) umstellen
 * @todo Exception-Framework einsetzen
 * @todo Concurrency-Framework einsetzen (insbesondere Ersetzung von Kommentaren der Art "// Atomic Block Start" etc.)
 * @todo Notwendigkeit von older in Datenstruktur Bucket �berdenken
 */

#ifndef BUCKETMANAGER_H
#define BUCKETMANAGER_H

#include <map>
#include <list>
#include <stack>
#include <vector>
#include <cstdlib>
#include <cstring>

#include <ColumnStore.h>

#ifdef DEBUG
#include <iostream>
#endif

/**
 * Die Konstante bestimmt die Anzahl der Bytes die für einen Bucket reserviert werden.
 */
#define CHUNK_CONTENT_SIZE (16 * 1024 * 1024)

/**
 * @brief Klasse zur Verwaltung von Bucket-Streams mit Buckets fester Größe
 *
 * Die Klasse verwaltet einfach verkettete Listen von Speicherbereichen fester Größe, sogenannte Bucket-Streams. Jegliche Änderungen an einem Bucket-Stream werden nach dem Mehrversionen-Konzept archiviert, wodurch keine Synchronisation zwischen mehreren Lesern notwendig ist. Die Klasse implemtiert das Singleton-Pattern, wodurch sichergestellt wird, dass maximal ein Objekt der Klasse existiert.
 */
class BucketManager {

public:
    friend class TransactionManager;

    /**
     * Die Datenstruktur kapselt einen Zeiger auf einen Speicherbereich fester Größe für die Datenstruktur Bucket.
     */
    struct Chunk {

        void *content;

        Chunk ();
        Chunk (void* content);
        Chunk (const Chunk &);

        Chunk& operator= (const Chunk &);
    };

    /**
     * Die Datenstruktur stellt den Zustand eines Buckets zu einer festen Version dar und beinhaltet neben einem Zeiger auf einen Chunk auch einen Zeiger auf den im Bucket-Stream nachfolgenden Bucket, sowie jeweils einen Zeiger auf die nächst aktuellere und die ältere Version des Buckets. Außerdem enthält die Datenstruktur die Version und die Position eines Buckets innerhalb des Bucket-Stream.
     */
    struct Bucket {

        id_t number;
        version_t *version;
        Bucket *next, *older, *newer;
        Chunk *chunk;

        Bucket ();
        Bucket (id_t number, version_t *version, Bucket *next, Bucket *older, Bucket* newer, Chunk *chunk);
        Bucket (const Bucket &);

        Bucket& operator= (const Bucket &);
    };

    /**
     * Die Datenstruktur enth�lt jeweils einen Zeiger auf den Anfang und das Ende einer einfach verketteten Liste von Buckets. Hierbei ist zu beachten, dass nur die aktuellste Version direkt miteinander verkettet ist und man f�r �ltere Versionen (z.B. die im Bezug auf Datenbanken konsistente Version) mittels der Zusatzzeiger in der Datenstruktur Bucket absteigen muss. Zus�tzlich enth�lt die Datenstruktur einen Vektor der f�r jede Position innerhalb des Bucket-Streams einen Zeiger auf die aktuellste Version des Buckets an der entsprechenden Position bereitstellt.
     */
    struct BucketStream {

        Bucket *head, *tail;
        vector<Bucket*> index;
        size_t size;

        BucketStream ();
        BucketStream (Bucket *head, Bucket *tail, vector<Bucket*> index, size_t size);
        BucketStream (const BucketStream &);

        BucketStream& operator= (const BucketStream &);
    };

    /**
     * @brief Klasse zum Lesen und Editieren eines Bucket-Streams
     *
     * Die Klasse stellt eine M�glichekeit zur Verf�gung einen Bucket-Stream nach dem Open/Next/Close-Prinzip zu lesen und editieren. Beim Erzeugen der Klasse muss ein Zeiger auf eine Version �bergeben werden, dessen Inhalt, im folgenden Version des Iterators genannt, angibt welche Buckets das Open/Next/Close-Interface liefert. Man liest daraufhin die Buckets mit der gr��ten Versionnummer kleiner oder gleich der Version des Iterators. Hierbei ist zu beachten, dass sich die Version des Iterators w�hrend der kompletten Lebensdauer eines Objekts dieser Klasse nicht �ndern darf, da man ggf. falsche Daten geliefert bekommt und bei �nderungen die komplette Datenbasis zerst�ren kann. Au�erdem ist darauf zu achten, dass zu einem festen Zeitpunkt maximal einen Iterator der �nderung durchgef�hrt hat oder �nderungen durchf�hren wird pro Bucket-Stream gibt. Operationen zur �nderung der Datenbasis d�rfen nur aufgerufen werden, falls die Version des Iterators gr��er als die aktuellste Version ist. Der Speicher f�r die Version eines Iterators, welcher �nderungen an der Datenbasis vollzogen hat, darf nach Zerst�rung des Objektes nicht freigegeben werden und der Inhalt darf auf eine Nummer gr��er als die aktuelle Version ver�ndert werden. Der Speicher f�r die Version eines Iterators, der ausschlie�lich lesend auf die Datenbasis zugegriffen hat, kann nach Zerst�rung des Objekts freigegeben werden.
     */
    class BucketIterator {

        friend class BucketManager;

    public:
        /**
         * @author Julian Hollender
         *
         * @return Anzahl der Buckets innerhalb des Bucket-Streams
         *
         * Die Funktion gibt die Anzahl der sichtbaren Buckets innerhalb des Bucket-Streams zur�ck.
         */
        size_t countBuckets ();
        /**
         * @author Julian Hollender
         *
         * @return aktuelle Position innerhalb des Bucket-Streams
         *
         * Die Funktion gibt die Position des Buckets innerhalb des Bucket-Streams zur�ck, welcher beim n�chsten Aufruf der Funktion next() zur�ckgegeben wird. Falls das Ende des Bucket-Streams erreicht wurde, wird die Anzahl der sichtbaren Buckets innerhalb des Bucket-Streams zur�ckgegeben. Hierbei ist zu beachten, dass die Nummerierung der Positionen innerhalb eines Bucket-Streams bei 0 beginnt.
         */
        size_t position ();

        /**
         * @author Julian Hollender
         *
         * Die Funktion setzt den Iterator in seine Ausgangsposition zur�ck, wodurch beim n�chsten Aufruf der Funktion next() wieder der Bucket an erster Position im Bucket-Stream zur�ckgegeben wird. M�gliche �nderungen an der Datenbasis werden nicht r�ckg�ngig gemacht.
         */
        void rewind ();
        /**
         * @author Julian Hollender
         *
         * @return Zeiger auf Inhalt des n�chsten Buckets innerhalb des Bucket-Streams
         *
         * Die Funktion gibt einen Zeiger auf den Inhalt des n�chsten Buckets innerhalb des Bucket-Streams zur�ck. Falls keine weiteren Buckets vorhanden sein sollten, wird ein NULL-Zeiger zur�ckgegeben. Um die Datenintegrit�t zu erhalten, darf der Inhalt des Buckets nicht ver�ndert werden.
         */
        Chunk* next ();
        /**
         * @author Julian Hollender
         *
         * @param number Position innerhalb des Bucket-Streams
         *
         * @return Zeiger auf den Inhalt des Buckets an der �bergebenen Position innerhalb des Bucket-Streams
         *
         * Die Funktion gibt einen Zeiger auf den Inhalt des Buckets an der �bergebenen Position zur�ck. Hierbei ist zu beachten, dass die Nummerierung der Positionen innerhalb eines Bucket-Streams bei 0 beginnt. Falls zur �bergebenen Position kein entsprechender Bucket gefunden wurde, wird ein NULL-Zeiger zur�ckgegeben. Um die Datenintegrit�t zu erhalten, darf der Inhalt des Buckets nicht ver�ndert werden.
         */
        Chunk* seek (size_t number);

        /**
         * @author Julian Hollender
         *
         * @return Zeiger auf den Inhalt des Buckets an der aktuellen Position
         *
         * Die Funktion liefert einen Zeiger auf den Inhalt des Buckets an der aktuellen Position zur�ck, dessen Inhalt ge�ndert werden darf. Hierbei wird der Zeiger f�r die Version des Buckets auf die Version des Iterators gesetzt. Ein erneuter Aufruf der Funktion w�hrend der Lebenszeit des Iterator-Objektes an der gleichen Position im Bucket-Stream liefert einen Zeiger auf die gleiche Speicherposition zur�ck.
         */
        Chunk* edit ();
        /**
         * @author Julian Hollender
         *
         * @return Zeiger auf den Inhalt eines Buckets, der an das Ende des Bucket-Streams angeh�ngt wurde
         *
         * Die Funktion liefert einen Zeiger auf den Inhalt eines Buckets, der an das Ende des Bucket-Streams angeh�ngt wurde. Hierbei wird der Zeiger f�r die Version des Buckets auf die Version des Iterators gesetzt. Nach dem Aufruf steht der Iterator auf dem neu angeh�ngten Bucket. Ein erneutes Aufrufen der Funktion edit() w�rde also einen Zeiger auf den gleiche Speicherbereich liefern.
         */
        Chunk* append ();

        /**
         * @author Julian Hollender
         *
         * Die Funktion nimmt alle bisher durchgef�hrten �nderungen des Iterators zur�ck und setzt anschlie�end den Iterator in seine Ausgangsposition zur�ck, wodurch beim n�chsten Aufruf der Funktion next() wieder der Bucket an erster Position im Bucket-Stream zur�ckgegeben wird.
         */
        void undo ();

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
         * Der Stack wird ben�tigt um die Undo-Funktionalit�t zu realisieren. Bei jedem Aufruf der Funktion append() oder edit() auf ein Bucket, welches bisher nicht von einem BucketIterator mit der gleichen Version (Referenzgleichheit) angelegt oder ge�ndert wurde, wird ein Zeiger auf den Vorg�ngerbucket auf dem Stack abgelegt.
         */
        std::stack<Bucket*> log;

        BucketIterator (BucketStream *stream, version_t *version);
        BucketIterator (const BucketIterator &copy);
        virtual ~BucketIterator ();

        BucketIterator& operator= (const BucketIterator& copy);

        friend class ColumnManager;
    };

    /**
     * @author Julian Hollender
     *
     * @return Zeiger auf einziges Objekt der Klasse BucketManager
     *
     * Die Funktion liefert einen Zeiger auf das einzig existierende Objekt der Klasse. Falls noch kein Objekt der Klasse existiert, wird ein Objekt erzeugt und anschlie�end ein Zeiger auf das Objekt zur�ckgegeben.
     */
    static BucketManager* getInstance ();

    /**
     * @author Julian Hollender
     *
     * @param id Identifikationsnummer des zu �ffnenden BucketStreams
     * @param version Zeiger auf Version, die angibt welche Buckets sichtbar f�r Iterator sind
     * @return Zeiger auf BucketIterator zum BucketStream mit Identifikationsnummer id
     *
     * Die Funktion erzeugt ein Objekt der Klasse BucketIterator zum Bearbeiten eines BucketStreams mit der Identifikationsnummer id. Hierbei sind nur die aktuellsten Buckets, deren Versionsnummer kleiner oder gleich dem Inhalt des Zeigers version ist, sichtbar. Bei jeder �nderung am Datenbestand durch den erzeugten BucketIterator, wird der Zeiger version in die Verwaltungsstrukturen des BucketStreams kopiert. Daher darf der Speicher, auf den der Zeiger version zeigt, nach �nderungen an der Datenbasis nicht mehr freigegeben werden. Falls ein BucketStream mit der �bergebenen Identifikationsnummer bisher nicht existiert, wird ein leerer BucketStream angelegt. Es ist darauf zu achten, dass zu einem festen Zeitpunkt maximal einen Iterator der �nderung durchgef�hrt hat oder �nderungen durchf�hren wird pro BucketStream gibt.
     */
    BucketIterator* openStream (id_t id, version_t *version);

#ifdef DEBUG
    void printDebugInformation ();
#endif

private:
    static BucketManager *instance;

    static void destroyInstance ();

    std::map<id_t, BucketStream> streams;

    BucketManager ();
    BucketManager (const BucketManager &copy);
    virtual ~BucketManager ();
};

#endif

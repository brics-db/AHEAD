/**
 * @author Julian Hollender
 * @date 20.07.2010
 *
 * @todo Performance-Probleme lösen (next()/append() aus Transaction vs. ColumnIterator) => vector anstatt map für TransactionManager::Transaction::iterators verwenden
 * @todo Exception-Framework einsetzen
 * @todo Concurrency-Framework einsetzen (insbesondere Ersetzung von Kommentaren der Art "// Atomic Block Start" etc.)
 * @todo Synchronisationskonzept für Updater überdenken (im Moment: Single-User-Betrieb)
 * @todo Mehrfaches Öffnen einer Spalte zulassen innerhalb einer Transaktion? Konzept überdenken! (Kontext: Self-Join)
 */

#include <utility>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>
#include <set>

#include "column_storage/ColumnManager.h"
#include "meta_repository/MetaRepositoryManager.h"

#ifndef TRANSACTIONMANAGER_H
#define TRANSACTIONMANAGER_H

/**
 * @brief Klasse zur Verwaltung von Transaktionen
 *
 * Die Klasse verwaltet Transaktionen, welche Spalten mit Einträgen fester Größe, sogenannte Records, lesen und editieren können. Jegliche Änderungen an einer Spalte werden nach dem Mehrversionen-Konzept archiviert, wodurch keine Synchronisation zwischen mehreren Lesern notwendig ist. Die Klasse implemtiert das Singleton-Pattern, wodurch sichergestellt wird, dass maximal ein Objekt der Klasse existiert. Die Transaktionsverwaltung achtet darauf, dass jede Transaktion einen konsistenten Zustand sieht und übernimmt die komplette Synchronisation der Transaktionen. Bisher darf zu einem festen Zeitpunkt nur eine Update-Transaktion existieren.
 */
class TransactionManager {
public:

    /**
     * Die Datenstruktur stellt ein Tupel dar.
     */
    struct BinaryUnit {
        void *head;
        void *tail;

        BinaryUnit() : head(nullptr), tail(nullptr) {
        }

        BinaryUnit(void *head, void *tail) : head(head), tail(tail) {
        }
    };

    /**
     * @brief Klasse zur Verwaltung einer Transaktion
     *
     * Die Klasse verwaltet eine Transaktion, der es möglich ist Spalten mit Einträgen fester Größe zu lesen und zu editieren. Beim Erzeugen eines Objektes dieser Klasse muss festgelegt werden, ob die Transaktion nur lesend auf den Datenbestand zugreifen darf. Außerdem werden für Update-Transaktionen alle Veränderungen geloggt, um ggf. ein Rollback durchführen zu können.
     */
    class Transaction {
        friend class TransactionManager;

        static const id_t ID_BAT_COLNAMES;
        static const id_t ID_BAT_COLTYPES;
        static const id_t ID_BAT_COLIDENT;
        static const id_t ID_BAT_FIRST_USER;

    public:
        /**
         * @author Julian Hollender
         *
         * @param path Pfad zu Daten
         * @param tableName name of the target table OR nullptr
         * @param prefix prefix string for all column names OR nullptr for no prefix
         * @param delim string of delimiters between values OR nullptr to use default ("|")
         * @return maximum number of tuples to load
         *
         * Die Funktion öffnet die Datei path + '_header.csv' und liest die enthaltenen Spaltennamen und Spaltentypen. Die Spaltennamen ( = Prefix + Spaltenname aus Header-Datei ) werden an die Spalte mit der Identifikationsnummer 0 angehängt, die Spaltentypen an die Spalte mit der Identifikationsnummer 1 und in der Spalte mit der Identifikationsnummer 2 wird die Identifikationsnummer der Spalte angehängt, in der anschließend die zugehörigen Daten landen. Danach werden die Daten aus der Datei path + '.tbl' gelesen und in die entsprechenden Spalten eingepflegt. Hierbei ist zu beachten, dass die Spalten mit der Identifikationsnummer 0, 1 und 2 ausschließlich durch die load()-Funktion verändert werden sollten, um eine korrekte Arbeitsweise der Funktion zu sichern.
         */
        size_t load(const char *path, const char *tableName = nullptr, const char *prefix = nullptr, size_t size = static_cast<size_t> (-1), const char *delim = nullptr, bool ignoreMoreData = true);

        /**
         * @author Julian Hollender
         *
         * @return Menge von Identifikationsnummern von Spalten
         *
         * Die Funktion liefert die Menge von Identifikationsnummern aller existierenden Spalten.
         */
        std::set<unsigned int> list();
        /**
         * @author Julian Hollender
         *
         * @param id Identifikationsnummer einer Spalte
         *
         * @return Größe der Spalte
         *
         * Die Funktion öffnet die Spalte mit der übergebenen Identifikationsnummer. Falls die Spalte bereits geöffnet ist oder keine Spalte zur übergebenen Identifikationsnummer existiert, wird keine Operation ausgeführt.
         */
        pair<size_t, size_t> open(id_t id);
        /**
         * @author Julian Hollender
         *
         * @param id Identifikationsnummer einer Spalte
         *
         * Die Funktion schließt die Spalte mit der übergebenen Identifikationsnummer. Falls die Spalte nicht geöffnet ist oder keine Spalte zur übergebenen Identifikationsnummer existiert, wird keine Operation ausgeführt.
         */
        void close(id_t id);

        /**
         * @author Julian Hollender
         *
         * @param id Identifikationsnummer einer Spalte
         * @return nächster Wert aus Spalte
         *
         * Die Funktion liefert den nächsten Wert aus der Spalte mit der übergebenen Identifikationsnummer id. Hierbei wird die Position des Werts innerhalb der Spalte in die erste Komponente der BinaryUnit kopiert und die zweite Komponente der BinaryUnit auf die Speicherstelle des Wertes verzeigert. Zu beachten ist, dass der Inhalt, auf den die zweite Komponente zeigt, nicht verändert werden darf. Falls die Spalte nicht geöffnet ist, keine Spalte zur übergebenen Identifikationsnummer existiert oder das Ende der Spalte erreicht wurde, wird ein NULL-Zeiger zurückgegeben.
         */
        BinaryUnit&& next(id_t id);
        /**
         * @author Julian Hollender
         *
         * @param id Identifikationsnummer einer Spalte
         * @param index Position innerhalb der Spalte
         * @return Wert aus Spalte an Position index
         *
         * Die Funktion liefert den Wert aus der Spalte mit der übergebenen Identifikationsnummer id an der Position index. Hierbei wird die Position des Werts innerhalb der Spalte in die erste Komponente der BinaryUnit kopiert und die zweite Komponente der BinaryUnit auf die Speicherstelle des Wertes verzeigert. Zu beachten ist, dass der Inhalt, auf den die zweite Komponente zeigt, nicht verändert werden darf. Falls die Spalte nicht geöffnet ist, keine Spalte zur übergebenen Identifikationsnummer existiert oder die Position innerhalb der Spalte nicht belegt ist, wird ein NULL-Zeiger zurückgegeben.
         */
        BinaryUnit&& get(id_t id, oid_t index);

        /**
         * @author Julian Hollender
         *
         * @param id Identifikationsnummer einer Spalte
         * @return aktueller Wert aus Spalte
         *
         * Die Funktion liefert den Wert aus der Spalte mit der übergebenen Identifikationsnummer id auf dem der zugehörige Iterator gerade steht. Hierbei wird die Position des Werts innerhalb der Spalte in die erste Komponente der BinaryUnit kopiert und die zweite Komponente der BinaryUnit auf die Speicherstelle des Wertes verzeigert. Falls die Spalte nicht geöffnet ist, keine Spalte zur übergebenen Identifikationsnummer existiert oder das Ende der Spalte erreicht wurde, wird ein NULL-Zeiger zurückgegeben.
         */
        BinaryUnit&& edit(id_t id);
        /**
         * @author Julian Hollender
         *
         * @param id Identifikationsnummer einer Spalte
         * @return aktueller Wert aus Spalte
         *
         * Die Funktion hängt einen Wert aus der Spalte mit der übergebenen Identifikationsnummer id an. Hierbei wird die Position des Werts innerhalb der Spalte in die erste Komponente der BinaryUnit kopiert und die zweite Komponente der BinaryUnit auf die Speicherstelle des Wertes verzeigert. Der zur Spalte gehörige Iterator steht nach den Aufruf auf dem neu eingefügten Element. Ein erneutes Aufrufen der Funktion edit() würde also einen BinaryUnit mit gleichem Inhalt liefern. Falls die Spalte nicht geöffnet ist oder keine Spalte zur übergebenen Identifikationsnummer existiert, wird ein NULL-Zeiger zurückgegeben.
         */
        BinaryUnit&& append(id_t id);

    private:
        unsigned int botVersion;
        unsigned int *eotVersion;
        bool isUpdater;

        /**
         * @author Julian Hollender
         *
         * Die Zuordnungsvorschrift weißt jeder Identifikationsnummer einer Spalte den ggf. geöffneten ColumnIterator zu und die aktuelle Position des ColumnIterators innerhalb der Spalte zu. Falls die Spalte geschlossen wurde, wird die Position des ColumnIterators auf (-1) gesetzt, um ggf. ein Undo durchführen zu können.
         */
        //		std::map<unsigned int, std::pair<ColumnManager::ColumnIterator*, unsigned int> > iterators;
        std::vector<ColumnManager::ColumnIterator*> iterators;
        std::vector<ssize_t> iteratorPositions;

        Transaction(bool isUpdater, unsigned int currentVersion);
        Transaction(const Transaction &copy);
        virtual ~Transaction();

        void rollback();
    };

    /**
     * @author Julian Hollender
     *
     * @return Zeiger auf einziges Objekt der Klasse ColumnManager
     *
     * Die Funktion liefert einen Zeiger auf das einzig existierende Objekt der Klasse. Falls noch kein Objekt der Klasse existiert, wird ein Objekt erzeugt und anschließend ein Zeiger auf das Objekt zurückgegeben.
     */
    static TransactionManager* getInstance();

    static void destroyInstance();

    /**
     * @author Julian Hollender
     *
     * @param isUpdater Änderungsberechtigung für Transaktion
     * @return neue Transaktion
     *
     * Die Funktion liefert eine Transaktion die den aktuellsten konsistenten Zustand der Datenbasis sieht. Der Parameter gibt an, ob die Transaktion Änderung an der Datenbasis durchführen darf. Zu einem festen Zeitpunkt darf es maximal eine Transaktion mit Änderungsberechtigung geben. Bei dem Versuch eine weitere Transaktion mit Änderungsberechtigung zu erzeugen, wird ein NULL-Zeiger zurückgeben. Die Transaktionverwaltung übernimmt alle Synchronisation zwischen den Transaktionen.
     */
    Transaction* beginTransaction(bool isUpdater);
    /**
     * @author Julian Hollender
     *
     * @param transaction Transaktion, die beendet werden soll
     *
     * Die Funktion beendet die übergebene Transaktion und macht ihre Änderungen zum aktuellen konsistenten Zustand. Eine erfolgreiche Ausführung der Funktion entspricht einem Commit.
     */
    void endTransaction(Transaction *transaction);
    /**
     * @author Julian Hollender
     *
     * @param transaction Transaktion, die zurückgesetzt werden soll
     *
     * Die Funktion setzt die übergebene Transaktion zurück und macht all ihre Änderungen rückgängig.
     */
    void rollbackTransaction(Transaction *transaction);

private:
    static TransactionManager *instance;

    unsigned int currentVersion;

    /**
     * Die Menge enthält alle aktuell aktiven Transaktionen.
     */
    std::set<Transaction*> transactions;

    TransactionManager();
    TransactionManager(const TransactionManager &copy);
    virtual ~TransactionManager();
};

#endif


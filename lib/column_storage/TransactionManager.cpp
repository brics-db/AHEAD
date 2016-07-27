#include "column_storage/TransactionManager.h"

using namespace std;

TransactionManager* TransactionManager::instance = 0;

TransactionManager* TransactionManager::getInstance() {
	if (TransactionManager::instance == 0) {
		TransactionManager::instance = new TransactionManager();
	}

	return TransactionManager::instance;
}

TransactionManager::Transaction* TransactionManager::beginTransaction(bool isUpdater) {
	TransactionManager::Transaction *transaction;

	if (isUpdater) {
		// Atomic Block Start
		std::set<Transaction*>::iterator it;

		for (it = this->transactions.begin(); it != this->transactions.end(); it++) {
			if ((*it)->isUpdater) {
				// Problem : Another Active Updater
				return 0;
			}
		}
		// Atomic Block Ende
	}

	transaction = new TransactionManager::Transaction(isUpdater, this->currentVersion);

	this->transactions.insert(transaction);

	return transaction;
}

void TransactionManager::endTransaction(TransactionManager::Transaction *transaction) {
	if (transaction->isUpdater) {
		*transaction->eotVersion = this->currentVersion + 1;
		this->currentVersion++;
	}

	// Atomic Block Start
	this->transactions.erase(this->transactions.find(transaction));
	// Atomic Block Ende

	delete transaction;
}

void TransactionManager::rollbackTransaction(TransactionManager::Transaction *transaction) {
	if (transaction->isUpdater) {
		transaction->rollback();
	}

	// Atomic Block Start
	this->transactions.erase(this->transactions.find(transaction));
	// Atomic Block Ende

	delete transaction;
}

TransactionManager::TransactionManager() {
	this->currentVersion = 0;
}

unsigned TransactionManager::Transaction::load(const char *path, const char *prefix, int size) {
	ColumnManager *cm = ColumnManager::getInstance();

	std::list<ColumnManager::ColumnIterator*> iterators;
	std::list<ColumnManager::ColumnIterator*>::iterator iteratorsIterator;
	std::list<unsigned int> types;
	std::list<unsigned int>::iterator typesIterator;

	ColumnManager::ColumnIterator *ci;
	ColumnManager::Record *record;

	std::set<unsigned int> columns;
	char *valuesPath = new char[1024];
	char *headerPath = new char[1024];
	FILE *valuesFile, *headerFile;
	char *line = new char[1024];
	char *value = new char[256];
	char *buffer = new char[1024];
	TransactionManager::BinaryUnit *bun;
	unsigned int offset, type, column;
	bool firstAppend = true;
	int n = 0;  // line counter

	if (this->isUpdater) {
		columns = cm->listColumns();

		if (columns.find(0) == columns.end()) {
			cm->createColumn(0,sizeof(char)*256);
			cm->createColumn(1,sizeof(unsigned int));
			cm->createColumn(2,sizeof(unsigned int));
		}

		strcpy(valuesPath, path);
		strcat(valuesPath, ".tbl");

		strcpy(headerPath, path);
		strcat(headerPath, "_header.csv");

		valuesFile = fopen(valuesPath, "r");
		headerFile = fopen(headerPath, "r");

		if (valuesFile != 0 && headerFile != 0) {
			// Zurückspulen der Iteratoren
			close(0); close(1); close(2);
			open(0); open(1); open(2);

			// Zeile mit Spaltennamen einlesen aus Header-Datei
			fgets(line, 1024, headerFile);
			
			if (strchr(line, '\n') != 0) {
				*strchr(line, '\n') = 0;
			}
			
			// Zeile durch Zeichen "|" in Einzelwerte trennen
			buffer = strtok(line, "|");

			while (buffer != 0) {
				if (strlen(prefix) + strlen(buffer) > 255) {
					// Problem : Name für Spalte (inkl. Prefix) zu lang
				}

				bun = append(0);

				// Spaltenname = Prefix + Spaltenname aus Header-Datei
				strcpy(value, prefix);
				strcat(value, buffer);

				strcpy((char*)bun->tail, value);

				// Berechnung der Anzahl bisher vorhandenen Spalten
				if (firstAppend) {
					offset = *((unsigned int*)bun->head);
					firstAppend = false;
				}

				delete bun->head;
				delete bun;

				buffer = strtok(NULL, "|");
			}

			// Zeile mit Spaltentypen einlesen aus Header-Datei
			fgets(line, 1024, headerFile);
			
			if (strchr(line, '\n') != 0) {
				*strchr(line, '\n') = 0;
			}

			// Zeile durch Zeichen "|" in Einzelwerte trennen
			buffer = strtok(line, "|");

			while (buffer != 0) {
				// freie Spalte suchen
				columns = cm->listColumns();
				column = 3;

				while (columns.find(column) != columns.end()) {
					column++;
				}

				// Spaltentyp einpflegen
				bun = append(1);

				if ( strncmp(buffer, "INTEGER",7) == 0 ) {
					*((unsigned int*)bun->tail) = 0;
					cm->createColumn(column, sizeof(int));
					//cerr << "Type integer" << column << endl;
				}
				else if( strncmp(buffer, "STRING",6) == 0 ) {
					*((unsigned int*)bun->tail) = 1;
					cm->createColumn(column, sizeof(char)*45);
					//cerr << "Type string" << column << endl;
				}
				else if( strncmp(buffer,"FIXED",5) == 0) {
					// data type fixed
					*((unsigned int*)bun->tail) = 2;
					cm->createColumn(column, sizeof(double));
					//cerr << "Type double" << column << endl;
				}
				else {
					// data type unknown
					cerr << "TransactionManager::Transaction::load() data type " << buffer << " in header unknown" << endl;
					abort();
				}


				types.push_back(*((unsigned int*)bun->tail));

				delete bun->head;
				delete bun;

				// Spaltenidentifikation einpflegen
				bun = append(2);

				*((unsigned int*)bun->tail) = column;

				delete bun->head;
				delete bun;

				open(column);

				iterators.push_back(this->iterators[column]);

				buffer = strtok(NULL, "|");
			}

			// Spaltenwerte zeilenweise aus Datei einlesen		
			while (fgets(line, 1024, valuesFile) != 0 && n != size) {
				if (strchr(line, '\n') != 0) {
					*strchr(line, '\n') = 0;
				}
				n++;  // increase line counter

				// Iteratoren f?r Spaltentypen und Spalteniteratoren zur?cksetzen
				iteratorsIterator = iterators.begin();
				typesIterator = types.begin();						

				// Zeile durch Zeichen "|" in Einzelwerte trennen
				buffer = strtok(line, "|");

				while (buffer != 0) {
					// Spaltentyp und Spaltenidentifikation bestimmen
					type = *typesIterator;
					ci = *iteratorsIterator;

					record = ci->append();

					switch (type) {
						case 0: {
							*((int*)record->content) = atoi(buffer);
							break;
						}
						case 1: {
							strcpy((char*)record->content, strcat(buffer,"\0"));
							break;
						}
						case 2: {
							// fixed
							*((double*)record->content) = atof(buffer);
							break;
						}
						default: {
							cerr << "TransactionManager::Transaction::load() data type unknown" << endl;
							abort();
						}
					} 

					delete record;

					typesIterator++;
					iteratorsIterator++;

					buffer = strtok(NULL, "|");
				}
			}
			cout <<"Number of BUNs: "<<n<<endl;

			// Spalten schließen
			close(0); close(1);	close(2);

			open(2);

			if (offset > 0) {
				bun = get(2, offset);
			} else {
				bun = next(2);
			}

			while (bun != 0) {
				close(*((unsigned int*)bun->tail));

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
			cerr << "Problem : Dateien konnten nicht geöffnet werden" << endl;
		}
	} else {
		// Problem : Transaktion darf keine Änderungen vornehmen
		cerr << "Problem : Transaktion darf keine Änderungen vornehmen" << endl;
	}
	return n;
}

std::set<unsigned int> TransactionManager::Transaction::list() {
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
	} else if (id < this->iterators.size() && this->iterators[id] != 0) {
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
	if (id >= this->iterators.size() || (id < this->iterators.size() && this->iterators[id] == 0)) {
		// Problem : Spalte nicht geoeffnet
	} else {
		this->iteratorPositions[id] = -1;
	}
}

TransactionManager::BinaryUnit* TransactionManager::Transaction::next(unsigned int id) {
	if (id < this->iterators.size() && this->iterators[id] != 0 && this->iteratorPositions[id] != -1) {
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
				return 0;
			}
		} else {
			// Problem : Spalte nicht geoffnet
			return 0;
		}
	} else {
		// Problem : Transaktion darf keine Änderungen vornehmen
		return 0;
	}
}

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

TransactionManager::Transaction::~Transaction(){
	for(unsigned int id = 0; id < this->iterators.size(); id++) {
		if (this->iterators[id] != 0) {
			delete this->iterators[id];
		}
	}
}

void TransactionManager::Transaction::rollback() {
	for(unsigned int id = 0; id < this->iterators.size(); id++) {
		if (this->iterators[id] != 0) {
			this->iterators[id]->undo();
			delete this->iterators[id];
		}
	}
}



unsigned TransactionManager::Transaction::load(char *path, char *table_name, char *prefix, int size) {
	ColumnManager *cm = ColumnManager::getInstance();
	MetaRepositoryManager *mrm = MetaRepositoryManager::getInstance();

	std::list<ColumnManager::ColumnIterator*> iterators;
	std::list<ColumnManager::ColumnIterator*>::iterator iteratorsIterator;
	std::list<unsigned int> types;
	std::list<unsigned int>::iterator typesIterator;

	ColumnManager::ColumnIterator *ci;
	ColumnManager::Record *record;

	std::set<unsigned int> columns;
	char *valuesPath = new char[1024];
	char *headerPath = new char[1024];
	FILE *valuesFile, *headerFile;
	char *line = new char[1024];
	char *value = new char[256];
	char *buffer = new char[1024];
	TransactionManager::BinaryUnit *bun;
	unsigned int offset, type, column;
	bool firstAppend = true;
	int n = 0;  // line counter

	unsigned newTableId; // unique id of the created table
	unsigned BATId;
	char *datatype = new char[256];
	std::vector<char*> attribute_names;

	if (this->isUpdater) {
		columns = cm->listColumns();

		if (columns.find(0) == columns.end()) {
			cm->createColumn(0,sizeof(char)*256);
			cm->createColumn(1,sizeof(unsigned int));
			cm->createColumn(2,sizeof(unsigned int));
		}

		strcpy(valuesPath, path);
		strcat(valuesPath, ".tbl");

		strcpy(headerPath, path);
		strcat(headerPath, "_header.csv");

		valuesFile = fopen(valuesPath, "r");
		headerFile = fopen(headerPath, "r");

		if (valuesFile != 0 && headerFile != 0) {
			// Zurückspulen der Iteratoren
			close(0); close(1); close(2);
			open(0); open(1); open(2);

			// create table
			newTableId = mrm->createTable(table_name);

			// Zeile mit Spaltennamen einlesen aus Header-Datei
			fgets(line, 1024, headerFile);

			if (strchr(line, '\n') != 0) {
				*strchr(line, '\n') = 0;
			}

			// Zeile durch Zeichen "|" in Einzelwerte trennen
			buffer = strtok(line, "|");

			while (buffer != 0) {
				if (strlen(prefix) + strlen(buffer) > 255) {
					// Problem : Name für Spalte (inkl. Prefix) zu lang
				}

				bun = append(0);

				// Spaltenname = Prefix + Spaltenname aus Header-Datei
				strcpy(value, prefix);
				strcat(value, buffer);

				strcpy((char*)bun->tail, value);

				// Berechnung der Anzahl bisher vorhandenen Spalten
				if (firstAppend) {
					offset = *((unsigned int*)bun->head);
					firstAppend = false;
				}

				delete bun->head;
				delete bun;

				buffer = strtok(NULL, "|");

				// prevents the referencing of value
				char* attribute_name = new char[256];
				strcpy(attribute_name, value);

				attribute_names.push_back(attribute_name);
			}

			// Zeile mit Spaltentypen einlesen aus Header-Datei
			fgets(line, 1024, headerFile);

			if (strchr(line, '\n') != 0) {
				*strchr(line, '\n') = 0;
			}

			// Zeile durch Zeichen "|" in Einzelwerte trennen
			buffer = strtok(line, "|");

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

				if ( strncmp(buffer, "INTEGER",7) == 0 ) {
					*((unsigned int*)bun->tail) = 0;
					cm->createColumn(column, sizeof(int));
					strcpy(datatype, "INTEGER");
					//cerr << "Type integer" << column << endl;
				}
				else if( strncmp(buffer, "STRING",6) == 0 ) {
					*((unsigned int*)bun->tail) = 1;
					cm->createColumn(column, sizeof(char)*45);
					strcpy(datatype, "STRING");
					//cerr << "Type string" << column << endl;
				}
				else if( strncmp(buffer,"FIXED",5) == 0) {
					// data type fixed
					*((unsigned int*)bun->tail) = 2;
					cm->createColumn(column, sizeof(double));
					strcpy(datatype, "FIXED");
					//cerr << "Type double" << column << endl;
				}
				else {
					// data type unknown
					cerr << "TransactionManager::Transaction::load() data type " << buffer << " in header unknown" << endl;
					abort();
				}


				types.push_back(*((unsigned int*)bun->tail));

				delete bun->head;
				delete bun;

				// Spaltenidentifikation einpflegen
				bun = append(2);

				*((unsigned int*)bun->tail) = column;

				delete bun->head;
				delete bun;

				open(column);

				iterators.push_back(this->iterators[column]);

				buffer = strtok(NULL, "|");

				BATId = column;

				// create attribute for specified table
				mrm->createAttribute(attribute_names.at(attributeNamesIndex), datatype, BATId, newTableId);

				attributeNamesIndex++;
			}

			// Spaltenwerte zeilenweise aus Datei einlesen
			while (fgets(line, 1024, valuesFile) != 0 && n != size) {
				if (strchr(line, '\n') != 0) {
					*strchr(line, '\n') = 0;
				}
				n++;  // increase line counter

				// Iteratoren f?r Spaltentypen und Spalteniteratoren zur?cksetzen
				iteratorsIterator = iterators.begin();
				typesIterator = types.begin();

				// Zeile durch Zeichen "|" in Einzelwerte trennen
				buffer = strtok(line, "|");

				while (buffer != 0) {
					// Spaltentyp und Spaltenidentifikation bestimmen
					type = *typesIterator;
					ci = *iteratorsIterator;

					record = ci->append();

					switch (type) {
						case 0: {
							*((int*)record->content) = atoi(buffer);
							break;
						}
						case 1: {
							strcpy((char*)record->content, buffer);
							break;
						}
						case 2: {
							// fixed
							*((double*)record->content) = atof(buffer);
							break;
						}
						default: {
							cerr << "TransactionManager::Transaction::load() data type unknown" << endl;
							abort();
						}
					}

					delete record;

					typesIterator++;
					iteratorsIterator++;

					buffer = strtok(NULL, "|");
				}
			}
			cout <<"Number of BUNs: "<<n<<endl;

			// Spalten schließen
			close(0); close(1);	close(2);

			open(2);

			if (offset > 0) {
				bun = get(2, offset);
			} else {
				bun = next(2);
			}

			while (bun != 0) {
				close(*((unsigned int*)bun->tail));

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
		}
	} else {
		// Problem : Transaktion darf keine Änderungen vornehmen
	}
	return n;
}


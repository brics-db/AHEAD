/**
 * @file column_table.h
 * @date Mar 7, 2011
 * @author Hannes Rauhe
 *
 */

#ifndef COLUMN_TABLE_H_
#define COLUMN_TABLE_H_

#include "column_storage/generic_column.h"
#include <stdexcept>
#include <string>
#include <sstream>

class column_table {
private:
    std::vector<generic_column> cols;
public:
    unsigned init_size;

    column_table(std::istream& csv_header_stream,std::istream& csv_stream,unsigned partition_size=0) {
        init_size=1;
        std::string line;
        std::string name_line;
        std::string type_line;
        if(std::getline(csv_header_stream,name_line) && std::getline(csv_header_stream,type_line)) {
            if (name_line[name_line.size() - 1] == '\r')
                name_line.resize(name_line.size() - 1);
            if (type_line[type_line.size() - 1] == '\r')
                type_line.resize(type_line.size() - 1);
            std::stringstream  nameStream(name_line);
            std::stringstream  typeStream(type_line);

            std::string        n;
            std::string        t;

            while(std::getline(typeStream,t,'|') && std::getline(nameStream,n,'|'))
            {
                cols.push_back(generic_column(t));
                cols.back().setName(n);
            }

            init_size = 0;

            while(std::getline(csv_stream,line))
            {

                if (line[line.size() - 1] == '\r')
                    line.resize(line.size() - 1);
                ++init_size;
                std::stringstream  lineStream(line);
                std::string        cell;
                unsigned c = 0;
                while(std::getline(lineStream,cell,'|'))
                {
                    if(partition_size && (init_size%partition_size)==0)
                        cols[c].create_partition(partition_size);
                    cols[c].push_back(cell);
                    ++c;
                }
            }
        }
    }

    generic_column& getGenCol(unsigned col_id) {
        return cols.at(col_id);
    }

    friend std::ostream &operator<<(std::ostream &os, column_table &obj);
};
/*
std::ostream &operator<<(std::ostream &os, column_table &obj) {
    std::vector<unsigned> len;
    for(std::vector<generic_column>::const_iterator it=obj.cols.begin();it!=obj.cols.end();++it) {
        len.push_back(it->getName().size());
        os<<it->getName()<<"|";
    }
    os<<std::endl;
    for(unsigned row=0;row<obj.init_size;++row) {
        unsigned cell = 0;
        for(std::vector<unsigned>::const_iterator cellwidth=len.begin();cellwidth!=len.end();++cellwidth) {
            os.width(*cellwidth);
            obj.getGenCol(cell++).print_val(os,row);
            os<<"|";
        }
        os<<std::endl;
    }
    return os;
}
*/
#endif /* COLUMN_TABLE_H_ */

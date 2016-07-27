#ifndef QUEUE_ADAPTER_H
#define QUEUE_ADAPTER_H

#include <iostream>
#include "column_storage/adapter.h"
#include <boost/interprocess/ipc/message_queue.hpp>

using namespace boost::interprocess;


template<class T>
class QueueAdapter: public Adapter<T>
{
private:
  bool m_write;
  std::string m_name;
  bool m_remove;
  message_queue *mq;

public:
  QueueAdapter<T>(bool remove = false, std::string name="noname"):m_write(true),m_name(name),m_remove(remove)//constructor
  {
    std::cout << "QueueAdapter::ctor()" << std::endl;
  }
  virtual void init(bool write)
  {
    std::cout << "QueueAdapter::init(" << write << ")" << std::endl;
    m_write = write;
    
    if(m_remove)
    {
      //Erase previous message queue
      message_queue::remove(m_name.c_str());
    }
    
    //if(write)//not needed...
    {
      mq = new message_queue
      (open_or_create	//open_or_create the message queue
      ,m_name.c_str()	//name
      ,2		//max message number
      ,sizeof(T)	//max message size
      );
    }
  }
  
  virtual T read()
  {
    //TODO error check if reading on a write end
    std::cout << "QueueAdapter::read()" << std::endl;
    unsigned int priority;
    std::size_t recvd_size;
    T el;
    mq->receive(&el, sizeof(el), recvd_size, priority);
    return el;
  }
  
  virtual void write(T el)
  {
    if(!m_write)
    {
      std::cerr << "Error: trying to write on a read-adapter!" << std::endl;
      return;
    }
    std::cout << "QueueAdapter::write(" << "'el'" << ")" << std::endl;
    mq->send(&el, sizeof(el), 0);
  }

  virtual void done()
  {
    std::cout << "QueueAdapter::done()" << std::endl;
    message_queue::remove(m_name.c_str());
  }
  
protected:
  virtual ~QueueAdapter<T>()
  {
    std::cout << "TODO QueueAdapter::dtor!" << std::endl;
    //message_queue::remove(name);
    //delete mq;
  }
};

#endif //QUEUE_ADAPTER_H

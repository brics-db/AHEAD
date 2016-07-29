#ifndef SHARED_MEM_ADAPTER_H
#define SHARED_MEM_ADAPTER_H

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdio.h>
#include <iostream>
#include "adapter.h"
#include <cstdlib>
#include "column_storage/circular_buffer.h"
#include <unistd.h> //for usleep

/**
 * Adapter used for IPC
 * Uses Shared Memory and a circular buffer to communicate
 */
template<class T>
class SharedMemAdapter : public Adapter<T> {
private:
    key_t m_key;
    int m_shmid;
    void *m_shm;
    CircularBuffer<T> *m_cb;
    size_t m_size;

public:

    SharedMemAdapter<T>(key_t key = 5678, size_t size = 100, bool create = false) : m_key(key)//default constructor
    {
        std::cout << "SharedMemAdapter::ctor(" << key << ", " << size << ")" << std::endl;
        m_size = size;
        init(create);
    }

private:

    virtual void init(bool create = false) {
        std::cout << "SharedMemAdapter::init(" << m_size << ", " << create << ")" << std::endl;

        //Point **m_read, **m_written;
        if ((m_shmid = shmget(m_key, (sizeof (CircularBuffer<T>) + m_size * sizeof (T)), IPC_CREAT | 0666)) < 0) {
            perror("shmget");
            return;
        }
        //Now we attach the segment to our data space.
        m_shm = (void *) shmat(m_shmid, NULL, 0);
        if ((m_shm) == (void *) - 1) {
            perror("shmat");
            return;
        }
        //instanciate class with shared mem
        m_cb = new CircularBuffer<T>(m_shm, m_size, create);

        std::cout << "/init(...)" << std::endl;
    }

public:

    virtual T read() {
        int n = 0;
        std::cout << "SharedMemAdapter::read()" << std::endl;
        while (m_cb->isEmpty())//shared mem empty?
        {
            if (n == 200)//only print every 2 seconds
            {
                std::cerr << "Shared mem is empty!" << std::endl;
                print_status();
                n = 0; //reset
            }
            n++;
            usleep(10000); //10000 us = 10 ms
        }
        return *(m_cb->read());
    }

    virtual void write(T el) {
        int n = 0;
        std::cout << "SharedMemAdapter::write()" << std::endl;
        while (m_cb->isFull()) {
            if (n == 200) {
                std::cerr << "Shared mem is full!" << std::endl;
                print_status();
                n = 0;
            }
            n++;
            usleep(10000); //10 ms
        }
        m_cb->write(el);
    }

    void print_status() {
        //m_cb->printBuffer();
    }

    virtual void done() {
        std::cout << "SharedMemAdapter::done()" << std::endl;
        shmctl(m_shmid, IPC_RMID, 0);
    }
};

#endif //SHARED_MEM_ADAPTER_H

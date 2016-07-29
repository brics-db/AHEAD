#ifndef CIRCULAR_BUFFER_H
#define CIRCULAR_BUFFER_H

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdio.h>
#include <iostream>
#include <cstdlib>

/**
 * A circular buffer using a shared memory for storing values ('T')
 */
template<class T>
class CircularBuffer {
private:
    uint *m_writePointer; /**< write pointer */
    uint *m_readPointer; /**< read pointer */
    size_t m_size; /**< size of circular buffer */
    T *m_buffer;
    const void *m_addr_shm;
public:

    CircularBuffer(const void *addr_shm, size_t size, bool create = false) : m_addr_shm(addr_shm)//ctor using shared mem for buffer
    {
        std::cout << "CircularBuffer w/ shared mem ctor: size: " << size << std::endl;
        m_size = size + 1; //one empty cell needed to differentiate empty and full
        m_buffer = (T*) (((uint*) addr_shm) + 2);
        m_readPointer = (uint*) addr_shm;
        m_writePointer = (uint*) ((uint*) addr_shm + 1);
        if (create) {
            *m_readPointer = 0;
            *m_writePointer = 0;
        }
    }

    bool isFull() const {
        return ((((*m_writePointer) + 1) % m_size) == *m_readPointer);
    }

    bool isEmpty() const {
        return (*m_writePointer == *m_readPointer);
    }

    void write(T el) {
        if (!isFull()) {
            m_buffer[*m_writePointer] = el;
            std::cout << "Inserted @" << (*m_writePointer) << std::endl;
            (*m_writePointer)++;
            (*m_writePointer) %= m_size;
        } else std::cout << "Buffer full!" << std::endl;
    }

    T* read() {
        T *result = 0;
        if (!isEmpty()) {
            result = &m_buffer[*m_readPointer];
            (*m_readPointer)++;
            (*m_readPointer) %= m_size;
        }
        return (result);
    }

    void printBuffer() const {
        std::cout << "CircularBuffer: " << m_buffer << " w:" << *m_writePointer << " r:" << *m_readPointer << " full?" << isFull() << " empty?" << isEmpty() << " size:" << m_size << std::endl;
        std::cout << "m_addr_shm: " << m_addr_shm << std::endl;
        for (uint i = 0; i < m_size; i++) {
            m_buffer[i].print();
        }
        std::cout << std::endl;
    }

};

#endif //CIRCULAR_BUFFER_H

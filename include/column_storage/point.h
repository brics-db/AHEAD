#ifndef POINT_H
#define POINT_H

#include <iostream>

class Point {
private:
    int _x, _y;
public:

    Point(int x = 0, int y = 0) : _x(x), _y(y) {
    };

    void print() {
        std::cout << "x: " << x() << " y: " << y() << std::endl;
    }

    int x() {
        return _x;
    }

    int y() {
        return _y;
    }

    bool isNull() {
        return _x == 0 && _y == 0;
    }

    Point operator+=(Point p) {
        _x += p.x();
        _y += p.y();
        return *this;
    }

    Point operator/(int i) {
        _x /= i;
        _y /= i;
        return *this;
    }

    /**
     * Enables the output of a Point on an std::ostream (e.g. std::cout)
     */
    friend std::ostream& operator<<(std::ostream &s, Point p) {
        s << "(" << p.x() << ", " << p.y() << ")";
        return s;
    }
};


#endif //POINT_H

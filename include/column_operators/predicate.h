/**
 * @file predicate.h
 * @date Mar 7, 2011
 * @author Hannes Rauhe
 *
 */

#ifndef PREDICATE_H_
#define PREDICATE_H_

template<class T>
class predicate {
public:
    virtual bool check(const T& toCheck) const = 0;

};

template<class T>
class pred_true : public predicate<T> {
public:
    pred_true() {}

    virtual bool check(const T& toCheck) const {
        return true;
    }
};

template<class T>
class pred_eq : public predicate<T> {
    const T v;
public:
    pred_eq(const T& val) : v(val) {}

    virtual bool check(const T& toCheck) const {
        return toCheck==v;
    }
};

template<class T>
class pred_lq : public predicate<T> {
    const T v;
public:
    pred_lq(const T& val) : v(val) {}

    virtual bool check(const T& toCheck) const {
        return toCheck<=v;
    }
};

template<class T>
class pred_gq : public predicate<T> {
    const T v;
public:
    pred_gq(const T& val) : v(val) {}

    virtual bool check(const T& toCheck) const {
        return toCheck>=v;
    }
};

/*
template<class T>
class pred_and : public predicate<T> {
    const predicate<T> p1;
    const predicate<T> p2;
public:
    pred_and(const predicate<T>& _p1,const predicate<T>& _p2) : p1(_p1),p2(_p2) {}

    virtual bool check(const T& toCheck) const {
        return p1.check(toCheck) && p2.check(toCheck);
    }
};

template<class T>
class pred_or : public predicate<T> {
    const predicate<T>& p1;
    const predicate<T>& p2;
public:
    pred_or(const predicate<T>& _p1,const predicate<T>& _p2) : p1(_p1),p2(_p2) {}
    virtual bool check(const T& toCheck) const {
        return p1.check(toCheck) || p2.check(toCheck);
    }
};
*/
#endif /* PREDICATE_H_ */

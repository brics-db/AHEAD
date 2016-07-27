#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys

print "#include <ostream>"
print "#include <cstring>"
print "#include <utility>"
print ""
print "using namespace std;"
print ""
print "class charXcmp {"
print "public:"
print "template<class T>"
print "bool operator()(const T &,const T&) const;"
print "};"
print ""
print "template<class T>"
print "bool charXcmp::operator() (const T & c1,const T & c2 ) const {"
print "return c1 < c2;"
print "}"
print ""
for p in range(1,int(sys.argv[1])+1,1): 
	print 'class char%(p)d {' % vars()
	print "public:"
	print 'char mS[%(p)d];' % vars()
	print 'char%(p)d() {};' % vars()
	print 'char%(p)d(string str) { const char* p = str.c_str(); memcpy(&mS, p, sizeof(this)); }; ' % vars()
	print 'char%(p)d(int value) { const char* p = ""; memcpy(&mS, p, sizeof(this)); }; ' % vars()
	print "};" 
	print 'bool operator<(const char%(p)d & c1,const char%(p)d & c2) { return (strcmp(c1.mS,c2.mS) < 0); }' % vars()
	print 'bool operator>(char%(p)d c1,char%(p)d c2) { return (strcmp(c1.mS,c2.mS) > 0); }' % vars()
	print 'bool operator>=(char%(p)d c1,char%(p)d c2) { return (strcmp(c1.mS,c2.mS) >= 0); }' % vars()
	print 'bool operator<=(char%(p)d c1,char%(p)d c2) { return (strcmp(c1.mS,c2.mS) <= 0); }' % vars()
	print 'bool operator==(char%(p)d c1,char%(p)d c2) { return (strcmp(c1.mS,c2.mS) == 0); }' % vars()
	print 'bool operator!=(char%(p)d c1,char%(p)d c2) { return (strcmp(c1.mS,c2.mS) != 0); }' % vars()
	print 'ostream& operator<<(ostream&s,char%(p)d c) { return s << c.mS; }' % vars()
	print " "
	print 'class Less_char%(p)d {' % vars()
	print "public:"
	print 'bool operator()(const char%(p)d &,const char%(p)d &)const;'  % vars()
	print "};"
	print " "
	print 'bool Less_char%(p)d::operator()(const char%(p)d & c1,const char%(p)d & c2) const {'  % vars()
	print "return (c1 < c2) ; "
	print "}"
	print " "

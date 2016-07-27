#include <ostream>
#include <utility>
#include <string>

class charXcmp {
public:
template<class T>
bool operator()(const T &,const T&) const;
};

template<class T>
bool charXcmp::operator() (const T & c1,const T & c2 ) const {
return c1 < c2;
}

template < int N, class char_type = char >
class charN {
private:
enum { static_N = N };
public:
char_type mS[N];
/// beware: this constructor leaves the object unitialized
charN() {};
charN(const char_type* source)
{
  this->assign( source );
}
charN(const char_type* source, int len)
{
  this->assign( source, len );
}
charN(const std::basic_string< char_type >& str)
{
  this->assign( str );
}
void assign(const std::basic_string< char_type >& str)
{
  std::copy( str.begin(), std::min( str.end(), str.begin() + (N-1) ), &mS[0] );
  mS[ std::min( static_cast<int>(str.size()), N-1 ) ] = static_cast<char_type>(0);
}
void assign(const char_type* source, int len)
{
  assert( len < static_N );
  std::copy( source, source+len, &mS[0] );
  mS[ len ] = static_cast<char_type>(0);
}
void assign(const char_type* source)
{
  char_type* dest = &mS[0];
  do
  {
    assert( dest != &mS[N] );
    *(dest++) = *source;
  } while ( *(source++) != static_cast<char_type>(0) );
}

};

template < int N, int M, class char_type >
inline int compare(const charN< N, char_type> & lhs, const charN< M, char_type> & rhs)
{
  for ( int i = 0; i < (N>M?M:N); ++i )
  {
    if ( lhs.mS[ i ] != rhs.mS[ i ] )
    {
      return lhs.mS[ i ] < rhs.mS[ i ] ? -1 : 1;
    }
    else
    {
      if ( lhs.mS[ i ] == static_cast<char_type>(0) )
	return 0;
    }
  }
  return 0;
}


template < int N, int M, class char_type >
inline bool operator<(const charN< N, char_type> & lhs, const charN< M, char_type> & rhs)
{
  return compare( lhs, rhs ) < 0;
}
template < int N, int M, class char_type >
inline bool operator>(const charN< N, char_type> & lhs, const charN< M, char_type> & rhs)
{
  return operator< < M, N, char_type >( rhs, lhs );
}
template < int N, int M, class char_type >
inline bool operator<=(const charN< N, char_type> & lhs, const charN< M, char_type> & rhs)
{
  return compare( lhs, rhs ) >= 0;
}
template < int N, int M, class char_type >
inline bool operator>=(const charN< N, char_type> & lhs, const charN< M, char_type> & rhs)
{
  return operator<= < N, M, char_type >( lhs, rhs );
}
template < int N, int M, class char_type >
inline bool operator==(const charN< N, char_type> & lhs, const charN< M, char_type> & rhs)
{
  return compare( lhs, rhs ) == 0;
}
template < int N, int M, class char_type >
inline bool operator!=(const charN< N, char_type> & lhs, const charN< M, char_type> & rhs)
{
  return !operator== < N, M, char_type >( lhs, rhs );
}
template < int N, class char_type >
inline std::ostream& operator<<( std::ostream& os, const charN< N, char_type> & y )
{
  return os << y.mS;
}

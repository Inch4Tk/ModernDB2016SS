#ifndef H_Record_HPP
#define H_Record_HPP

#include <cstring>
#include <cstdlib>
#include <stdint.h>

// A simple Record implementation
class Record
{
	uint32_t len;
	uint8_t* data;

public:
	// Assignment Operator: deleted
	Record& operator=( Record& rhs ) = delete;
	// Copy Constructor: deleted
	Record( Record& t ) = delete;
	// Move Constructor
	Record( Record&& t );
	// Constructor
	Record( uint32_t len, const uint8_t* const ptr );
	// Destructor
	~Record();
	// Get pointer to data
	const uint8_t* GetData() const;
	// Get data size in bytes
	uint32_t GetLen() const;
};

Record::Record( Record&& t ) : len( t.len ), data( t.data )
{
	t.data = nullptr;
	t.len = 0;
}

Record::Record( uint32_t len, const uint8_t* const ptr ) : len( len )
{
	data = static_cast<uint8_t*>(malloc( len ));
	if ( data )
		memcpy( data, ptr, len );
}

const uint8_t* Record::GetData() const
{
	return data;
}

uint32_t Record::GetLen() const
{
	return len;
}

Record::~Record()
{
	free( data );
}

#endif
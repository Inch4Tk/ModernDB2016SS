
#include "Record.h"

Record::Record( Record&& t ) : len( t.len ), data( t.data )
{
	t.data = nullptr;
	t.len = 0;
}

Record::Record( uint32_t len, const uint8_t* const ptr ) : len( len )
{
	if (ptr == nullptr)
	{
		data = nullptr;
	}
	else
	{
		data = static_cast<uint8_t*>(malloc( len ));
		if ( data )
			memcpy( data, ptr, len );
	}
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

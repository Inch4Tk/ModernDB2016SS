
#include "utility/defines.h"
#include "utility/helpers.h"
#include "buffer/BufferManager.h"

#include <iostream>
#include <string>
#include <stdint.h>
#include <assert.h>

int main( int argc, char* argv[] )
{
	// Windows memory leak detection debug system call
#ifdef _CRTDBG_MAP_ALLOC
	_CrtSetDbgFlag( _CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF );
#endif // _CRTDBG_MAP_ALLOC

	BufferManager mgr( 100 );

	BufferFrame& buf1 = mgr.FixPage( 0, true );
	BufferFrame& buf2 = mgr.FixPage( 1, true );
	BufferFrame& buf3 = mgr.FixPage( 2, true );
	mgr.UnfixPage( buf1, true );
	mgr.UnfixPage( buf2, true );
	mgr.UnfixPage( buf3, false ); // Should not write this one back to disc

	return 0;
}
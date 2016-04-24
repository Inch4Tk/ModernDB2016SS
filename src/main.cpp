
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

	return 0;
}
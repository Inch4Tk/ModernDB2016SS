
#include "utility/defines.h"
#include "utility/helpers.h"
#include "buffer/BufferManager.h"

#include <iostream>
#include <string>
#include <stdint.h>
#include <assert.h>

#include <future>
#include <thread>
#include <vector>
#include <stdlib.h>
#include <time.h>


BufferManager* bm;
uint32_t pagesOnDisk;
uint32_t pagesInRAM;
uint32_t threadCount;
volatile bool stop = false;

uint32_t randomPage( uint32_t threadNum )
{
	// pseudo-gaussian, causes skewed access pattern
	uint32_t page = 0;
	for ( uint32_t i = 0; i < 20; i++ )
		page += rand() % pagesOnDisk; // TODO: replace this for thread safe random on unix
	return page / 20;
}

void scan()
{
	// scan all pages and check if the counters are not decreasing
	std::vector<uint32_t> counters( pagesOnDisk, 0 );

	while ( !stop )
	{
		uint32_t start = rand() % (pagesOnDisk - 10); // TODO: replace this for thread safe random on unix
		for ( uint32_t page = start; page < start + 10; page++ )
		{
			BufferFrame& bf = bm->FixPage( page, false );
			uint32_t newcount = reinterpret_cast<uint32_t*>(bf.GetData())[0];
			assert( counters[page] <= newcount );
			counters[page] = newcount;
			bm->UnfixPage( bf, false );
		}
	}
}

uint32_t readWrite( uint32_t threadNum )
{
	uint32_t count = 0;
	for ( uint32_t i = 0; i < 100000 / threadCount; i++ )
	{
		bool isWrite = rand() % 128 < 10; // TODO: replace this for thread safe random on unix
		BufferFrame& bf = bm->FixPage( randomPage( threadNum ), isWrite );

		if ( isWrite )
		{
			++count;
			reinterpret_cast<uint32_t*>(bf.GetData())[0]++;
		}
		bm->UnfixPage( bf, isWrite );
	}

	return count;
}

int main( int argc, char** argv )
{
	pagesOnDisk = 250;
	pagesInRAM = 20;
	threadCount = 5;

	bm = new BufferManager( pagesInRAM );

	// set all counters to 0
	for ( uint32_t i = 0; i < pagesOnDisk; i++ )
	{
		BufferFrame& bf = bm->FixPage( i, true );
		reinterpret_cast<uint32_t*>(bf.GetData())[0] = 0;
		bm->UnfixPage( bf, true );
	}

	// start scan thread
	std::thread scanThread(scan);

	// start read/write threads
	std::vector<std::future<uint32_t>> readWriteFutures;
	for ( uint32_t i = 0; i < threadCount; i++ )
	{
		readWriteFutures.push_back( std::async( readWrite, i ) );
	}

	// wait for read/write threads
	uint32_t totalCount = 0;
	for ( uint32_t i = 0; i < threadCount; i++ )
	{
		totalCount += readWriteFutures[i].get();
	}

	// wait for scan thread
	stop = true;
	scanThread.join();

	// restart buffer manager
	delete bm;
	bm = new BufferManager( pagesInRAM );

	// check counter
	uint32_t totalCountOnDisk = 0;
	for ( uint32_t i = 0; i < pagesOnDisk; i++ )
	{
		BufferFrame& bf = bm->FixPage( i, false );
		totalCountOnDisk += reinterpret_cast<uint32_t*>(bf.GetData())[0];
		bm->UnfixPage( bf, false );
	}
	if ( totalCount == totalCountOnDisk )
	{
		std::cout << "test successful" << std::endl;
		delete bm;
		return 0;
	}
	else
	{
		std::cerr << "error: expected " << totalCount << " but got " << totalCountOnDisk << std::endl;
		delete bm;
		return 1;
	}
}


//int main( int argc, char* argv[] )
//{
//	// Windows memory leak detection debug system call
//#ifdef _CRTDBG_MAP_ALLOC
//	_CrtSetDbgFlag( _CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF );
//#endif // _CRTDBG_MAP_ALLOC
//
//	uint32_t pagesOnDisk = 50;
//	uint32_t pagesInRam = 20;
//	uint32_t threadCount = 5;
//
//	BufferManager* mgr = new BufferManager( pagesInRam );
//
//	for ( unsigned i = 0; i < pagesOnDisk; i++ )
//	{
//		BufferFrame& bf = mgr->FixPage( i, true );
//		reinterpret_cast<unsigned*>(bf.GetData())[0] = 0;
//		mgr->UnfixPage( bf, true );
//	}
//
//	delete mgr;
//
//	return 0;
//}
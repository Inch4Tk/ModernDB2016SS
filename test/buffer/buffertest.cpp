#include "buffer/BufferManager.h"
#include "utility/macros.h"
#include "utility/defines.h"

#include "gtest/gtest.h"

#include <future>
#include <thread>
#include <vector>
#include <random>
#include <stdlib.h>

struct BufferTestInitState
{
	uint32_t pagesOnDisk;
	uint32_t pagesInMemory;
	uint32_t threads;

	friend std::ostream& operator<<( std::ostream& os, const BufferTestInitState& obj )
	{
		return os
			<< "Pages on Disk: " << obj.pagesOnDisk
			<< " Pages in Memory: " << obj.pagesInMemory
			<< " Threads: " << obj.threads;
	}
};

class BufferTest : public ::testing::TestWithParam<BufferTestInitState>
{
public:
	BufferTest()
	{

	}
	virtual ~BufferTest()
	{

	}
	virtual void SetUp() override
	{
		mgr = new BufferManager( GetParam().pagesInMemory );
		// set all counters to 0
		for ( uint32_t i = 0; i < GetParam().pagesOnDisk; i++ )
		{
			BufferFrame& bf = mgr->FixPage( i, true );
			reinterpret_cast<uint32_t*>(bf.GetData())[0] = 0;
			mgr->UnfixPage( bf, true );
		}
	}
	virtual void TearDown() override
	{
		SDELETE(mgr);
	}

	BufferManager* mgr = nullptr;
};

INSTANTIATE_TEST_CASE_P( Default, BufferTest,
						 testing::Values(
							 BufferTestInitState{ 50,20,5 },
							 BufferTestInitState{ 100,50,20 },
							 BufferTestInitState{ 20,30,10 },
							 BufferTestInitState{ 4000, 400, 20 } // Bigger test, probably will remove this once we have more tests to save time
) );

// Thread methods
void Scan( BufferManager* bm, uint32_t pagesOnDisk, bool& stop, uint32_t& fails )
{
	// Construct a random device to seed the random generator with true random numbers
	std::random_device rd;
	std::minstd_rand randGen( rd() );

	// scan all pages and check if the counters are not decreasing
	std::vector<uint32_t> counters( pagesOnDisk, 0 );

	while ( !stop )
	{
		uint32_t start = randGen() % (pagesOnDisk - 10);
		for ( uint32_t page = start; page < start + 10; page++ )
		{
			BufferFrame& bf = bm->FixPage( page, false );
			uint32_t newcount = reinterpret_cast<uint32_t*>(bf.GetData())[0];
			if (newcount < counters[page])
			{
				++fails;
			}
			counters[page] = newcount;
			bm->UnfixPage( bf, false );
		}
	}
}

uint32_t RandomPage(float ranNum, uint32_t pages)
{
	float pageHalf = 0.5f * pages;
	return static_cast<uint32_t>(std::max( 0.0f, ranNum * pageHalf + pageHalf )) % pages;
}

uint32_t ReadWrite( BufferManager* bm, uint32_t threadNum, uint32_t threadCount, uint32_t pagesOnDisk )
{
	// Construct a random device to seed the random generator with true random numbers
	std::random_device rd;
	std::minstd_rand randGen( rd() );
	std::normal_distribution<float> normalDist(0.0f, 0.2f);

	uint32_t count = 0;
	for ( uint32_t i = 0; i < 100000 / threadCount; i++ )
	{
		bool isWrite = randGen() % 128 < 10;
		BufferFrame& bf = bm->FixPage( RandomPage(normalDist(randGen), pagesOnDisk), isWrite );

		if ( isWrite )
		{
			++count;
			reinterpret_cast<uint32_t*>(bf.GetData())[0]++;
		}
		bm->UnfixPage( bf, isWrite );
	}

	return count;
}

// Test if we can read and write from multiple threads without failures
TEST_P(BufferTest, MultiThreadScanAndWrite)
{
	// Start scan thread
	bool stop = false;
	uint32_t fails = 0;
	std::thread scanThread( Scan, mgr, GetParam().pagesOnDisk, std::ref(stop), std::ref(fails) );

	// Start read/write threads
	std::vector<std::future<uint32_t>> readWriteFutures;
	for ( uint32_t i = 0; i < GetParam().threads; i++ )
	{
		readWriteFutures.push_back( std::async( ReadWrite, mgr, i, 
												GetParam().threads, GetParam().pagesOnDisk ) );
	}

	// Wait for read/write threads
	uint32_t totalCount = 0;
	for ( uint32_t i = 0; i < GetParam().threads; i++ )
	{
		totalCount += readWriteFutures[i].get();
	}

	// Wait for scan thread
	stop = true;
	scanThread.join();
	EXPECT_EQ( 0, fails );

	// Restart buffer manager
	SDELETE(mgr);
	mgr = new BufferManager( GetParam().pagesInMemory );

	// Verify results
	uint32_t totalCountOnDisk = 0;
	for ( uint32_t i = 0; i < GetParam().pagesOnDisk; i++ )
	{
		BufferFrame& bf = mgr->FixPage( i, false );
		totalCountOnDisk += reinterpret_cast<uint32_t*>(bf.GetData())[0];
		mgr->UnfixPage( bf, false );
	}
	EXPECT_EQ( totalCount, totalCountOnDisk );
}

// Tests for expected exception when we lock more than memory pages
TEST_P( BufferTest, CreatePageOverload )
{
	uint32_t locksNecessary = GetParam().pagesInMemory + 1;
	uint32_t failedLocks = 0;
	std::vector<BufferFrame*> frames;
	for ( uint32_t i = 0; i < locksNecessary; i++ )
	{
		try
		{
			frames.push_back(&mgr->FixPage( i, true ));
		}
		catch (const std::runtime_error& e)
		{
			UNREFERENCED_PARAMETER( e );
			++failedLocks;
		}
	}
	for ( BufferFrame* f : frames )
	{
		mgr->UnfixPage( *f, false );
	}
	
	// We do exactly 1 lock more than pages, this one should throw
	EXPECT_EQ( 1u, failedLocks );
}

TEST( BufferTest, MergeSplitPageId )
{
	// Except both ways to result in the same things
	uint64_t id = BufferManager::MergePageId( 3, 1256 );
	auto split = BufferManager::SplitPageId( id );
	EXPECT_EQ( 3, split.first );
	EXPECT_EQ( 1256, split.second );

	split = BufferManager::SplitPageId( 0x000500000000005Ful );
	id = BufferManager::MergePageId( split.first, split.second );
	EXPECT_EQ( 0x000500000000005Ful, id );

	// Expect merge to cut away things that are too big
	id = BufferManager::MergePageId( 0xFFFFFul, 0x000FFFFFFFFFFFFFul );
	split = BufferManager::SplitPageId( id );
	EXPECT_EQ( 0xFFFFul, split.first );
	EXPECT_EQ( 0x0000FFFFFFFFFFFFul, split.second );
}
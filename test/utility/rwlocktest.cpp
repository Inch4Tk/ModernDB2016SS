#include "utility/RWLock.h"
#include "utility/macros.h"

#include "gtest/gtest.h"

#include <chrono>
#include <thread>

// Test that our implementation correctly wraps the platform dependent rwlocks
class RWLockTest : public ::testing::Test
{
public:
	virtual void SetUp() override
	{
		lock = new RWLock();
	}
	virtual void TearDown() override
	{
		SDELETE( lock );
	}
	RWLock* lock;
};

TEST_F(RWLockTest, NoReadWhenWriting)
{
	uint32_t x = 0;
	std::thread t1( [&]()
	{
		lock->LockWrite();
		std::this_thread::sleep_for( std::chrono::milliseconds(1000) );
		for ( uint32_t i = 0; i < 100000; ++i)
		{
			++x;
		}
		lock->UnlockWrite();
	} );
	uint32_t num = 0;
	std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );
	std::thread t2( [&]()
	{
		lock->LockRead();
		num = x;
		lock->UnlockRead();
	} );
	t1.join();
	t2.join();
	EXPECT_EQ( x, num );
}

TEST_F(RWLockTest, NoWriteWhenReading)
{
	uint32_t inc = 0;
	uint32_t x = 0;
	uint32_t y = 0;
	uint32_t z = 0;
	// Readers
	std::thread tx( [&]()
	{
		lock->LockRead();
		std::this_thread::sleep_for( std::chrono::milliseconds( 1000 ) );
		x = inc;
		lock->UnlockRead();
	} );
	std::thread ty( [&]()
	{
		lock->LockRead();
		std::this_thread::sleep_for( std::chrono::milliseconds( 1000 ) );
		y = inc;
		lock->UnlockRead();
	} );
	std::thread tz( [&]()
	{
		lock->LockRead();
		std::this_thread::sleep_for( std::chrono::milliseconds( 1000 ) );
		y = inc;
		lock->UnlockRead();
	} );
	// Writer
	std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );
	std::thread tw( [&]()
	{
		lock->LockWrite();
		for ( uint32_t i = 0; i < 100000; ++i )
		{
			++inc;
		}
		lock->UnlockWrite();
	} );
	tx.join();
	ty.join();
	tz.join();
	tw.join();

	EXPECT_EQ( 0u, x );
	EXPECT_EQ( 0u, y );
	EXPECT_EQ( 0u, z );
	EXPECT_EQ( 100000u, inc );
}

TEST_F( RWLockTest, TryWriteFails )
{
	std::thread t1( [&]()
	{
		lock->LockRead();
		std::this_thread::sleep_for( std::chrono::milliseconds( 1000 ) );
		lock->UnlockRead();
	} );
	bool failedLock = false;
	std::this_thread::sleep_for( std::chrono::milliseconds( 500 ) );
	std::thread t2( [&]()
	{
		failedLock = !lock->TryLockWrite();
		if ( !failedLock )
		{
			lock->UnlockWrite();
		}
	} );
	t1.join();
	t2.join();
	EXPECT_EQ( true, failedLock );
}

TEST_F(RWLockTest, SimultaneousReaders)
{

	auto start = std::chrono::high_resolution_clock::now();
	std::vector<std::thread*> ts;
	for ( uint32_t i = 0; i < 5; ++i)
	{
		ts.push_back(new std::thread( [&]()
		{
			lock->LockRead();
			std::this_thread::sleep_for( std::chrono::milliseconds( 1000 ) );
			lock->UnlockRead();
		} ) );
	}
	for (std::thread* t : ts)
	{
		t->join();
		SDELETE( t );
	}
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> elapsed = end - start;
	double elapsedMs = elapsed.count();
	EXPECT_GT( 1500.0, elapsedMs );
}
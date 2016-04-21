#include "BufferFrame.h"

#include <assert.h>

/// <summary>
/// Initializes a new instance of the <see cref="BufferFrame"/> class.
/// </summary>
BufferFrame::BufferFrame() : mLoaded(false), mDirty(false), mExclusive(false), mSharedBy(0)
{
}

/// <summary>
/// Initializes a new instance of the <see cref="BufferFrame"/> class.
/// </summary>
/// <param name="">The .</param>
BufferFrame::BufferFrame( const BufferFrame& bf ) : 
	mLoaded( bf.mLoaded.load() ), mDirty( bf.mDirty.load() ), 
	mExclusive( bf.mExclusive.load() ), mSharedBy( bf.mSharedBy.load() )
{
}

/// <summary>
/// Finalizes an instance of the <see cref="BufferFrame"/> class.
/// </summary>
BufferFrame::~BufferFrame()
{
}

/// <summary>
/// Gets the page identifier.
/// </summary>
/// <returns></returns>
uint64_t BufferFrame::GetPageId() const
{
	return mPageId;
}

/// <summary>
/// Gets the data from the buffer frames page.
/// </summary>
/// <returns>Datapointer</returns>
void* BufferFrame::GetData() const
{
	return mData;
}

/// <summary>
/// Determines whether this instance is dirty.
/// </summary>
/// <returns></returns>
bool BufferFrame::IsDirty() const
{
	return mDirty.load();
}

/// <summary>
/// Tries to acquire write lock (now). Returns true if succeeded, false if failed. Also sets necessary state variables on success.
/// </summary>
/// <returns></returns>
bool BufferFrame::TryLockWrite()
{
	bool success = mRWLock.TryLockWrite();
	if (success)
	{
		mExclusive.store(true);
		assert( mSharedBy.load() == 0 );
	}
	return success;
}

/// <summary>
/// Locks for writing if exclusive is true. Locks for reading (shared access among readers) if exclusive is false.
/// Also sets necessary state variables.
/// </summary>
/// <param name="exclusive">if set to <c>true</c> [exclusive].</param>
void BufferFrame::Lock( bool exclusive )
{
	if (exclusive)
	{
		mRWLock.LockWrite();
		mExclusive.store( true );
		assert( mSharedBy.load() == 0 );
	}
	else
	{
		mRWLock.LockRead();
		++mSharedBy;
		assert( !mExclusive.load() );
	}
}

/// <summary>
/// Unlocks multipurpose unlock. If we currently are in read mode, we perform a read unlock, 
/// if we are in writing mode we perform writing unlock.
/// </summary>
/// <returns></returns>
void BufferFrame::Unlock()
{
	if ( mExclusive.load() )
	{
		assert( mSharedBy.load() == 0 );
		mExclusive.store( false );
		mRWLock.UnlockWrite();
	}
	else
	{
		assert( !mExclusive.load() );
		assert( mSharedBy.load() > 0 ); // Okay, this one is arguably suboptimal, since it does not guarantee anything for the next line
		--mSharedBy;
		mRWLock.UnlockRead();
	}
}


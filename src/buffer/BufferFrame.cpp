#include "BufferFrame.h"

#include <assert.h>

/// <summary>
/// Initializes a new instance of the <see cref="BufferFrame"/> class.
/// </summary>
BufferFrame::BufferFrame()
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
/// Determines whether this instance is loaded.
/// </summary>
/// <returns></returns>
bool BufferFrame::IsLoaded() const
{
	return mLoaded;
}

/// <summary>
/// Determines whether this instance is dirty.
/// </summary>
/// <returns></returns>
bool BufferFrame::IsDirty() const
{
	return mDirty;
}


/// <summary>
/// Determines whether this instance is exclusive.
/// </summary>
/// <returns></returns>
bool BufferFrame::IsExclusive() const
{
	return mExclusive;
}

/// <summary>
/// Determines whether this instance is shared.
/// </summary>
/// <returns></returns>
bool BufferFrame::IsShared() const
{
	return mShares > 0;
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
		mExclusive = true; // TODO: replace with atomic if necessary
		assert( mShares == 0 );
	}
	return success;
}

/// <summary>
/// Locks for writing. Also sets necessary state variables.
/// </summary>
/// <returns></returns>
void BufferFrame::LockWrite()
{
	mRWLock.LockWrite();
	mExclusive = true; // TODO: replace with atomic if necessary
	assert( mShares == 0 );
}

/// <summary>
/// Locks for reading (shared access among readers). Also sets necessary state variables.
/// </summary>
/// <returns></returns>
void BufferFrame::LockRead()
{
	mRWLock.LockRead();
	assert( !mExclusive );
}

/// <summary>
/// Unlocks multipurpose unlock. If we currently are in read mode, we perform a read unlock, 
/// if we are in writing mode we perform writing unlock.
/// </summary>
/// <returns></returns>
void BufferFrame::Unlock()
{
	// TODO
}


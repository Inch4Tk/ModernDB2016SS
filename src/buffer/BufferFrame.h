#pragma once
#ifndef BUFFER_FRAME_H
#define BUFFER_FRAME_H

#include "utility/RWLock.h"

#include <stdint.h>
#include <atomic>

// Forwards
class BufferManager;

/// <summary>
/// Buffer frame class used by the buffer manager to store pages and related information.
/// </summary>
class BufferFrame
{
	friend class BufferManager;
public:
	BufferFrame();
	BufferFrame( const BufferFrame& bf );
	~BufferFrame();

	uint64_t GetPageId() const;
	void* GetData() const;

	bool IsDirty() const;

	bool operator==( const BufferFrame& other ) const;
	bool operator!=( const BufferFrame& other ) const;
private:
	// Status
	// These are conservatively atomic, we could probably get away with non-atomics,
	// for exclusive, loaded and dirty (as these are only set when the buffer is locked for writing). 
	// SharedBy has to be atomic, since it is changed when reading.
	// The variables are mainly used for determining buffer eviction and unlock behavior
	std::atomic<bool> mLoaded;
	std::atomic<bool> mDirty;
	std::atomic<bool> mExclusive;
	std::atomic<uint32_t> mSharedBy;
	std::atomic<uint64_t> mEvictionScore;
	uint64_t mPageId = 0;
	RWLock mRWLock;

	void* mData = nullptr;

	bool IsFixedProbably();
	// Locking/unlocking methods (this is not just a pure mirror, we add functionality)
	bool TryLockWrite();
	void Lock(bool exclusive);
	void Unlock();
};

#endif
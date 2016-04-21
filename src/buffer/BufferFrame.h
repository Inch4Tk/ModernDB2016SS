#pragma once
#ifndef BUFFER_FRAME_H
#define BUFFER_FRAME_H

#include <stdint.h>
#include "utility/RWLock.h"

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
	~BufferFrame();

	uint64_t GetPageId() const;
	void* GetData() const;

	bool IsLoaded() const;
	bool IsDirty() const;
	bool IsFixed() const;
	bool IsExclusive() const;
	bool IsShared() const;
	

private:
	// Status
	bool mLoaded = false; // TODO: replace these with atomics
	bool mDirty = false;
	bool mExclusive = false;
	uint32_t mShares = 0;
	uint64_t mPageId = 0;
	RWLock mRWLock;

	void* mData = nullptr;
	// Locking/unlocking methods (this is not just a pure mirror, we add functionality)
	inline bool TryLockWrite();
	inline void LockWrite();
	inline void LockRead();
	inline void Unlock();
};

#endif
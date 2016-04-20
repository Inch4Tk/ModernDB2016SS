#pragma once
#ifndef BUFFER_FRAME_H
#define BUFFER_FRAME_H

#include <stdint.h>

// Forwards
class BufferManager;

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
	bool mLoaded = false;
	bool mDirty = false;
	bool mFixed = false;
	bool mExclusive = false;
	uint32_t mShares = 0;
	uint64_t mPageId = 0;

	void* mData = nullptr;
};

#endif
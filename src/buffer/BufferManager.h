#pragma once
#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include "BufferFrame.h"
#include "utility/RWLock.h"

#include <stdint.h>
#include <utility>
#include <vector>
#include <unordered_map>

/// <summary>
/// Concurrent Buffer Manager, enabling loading from and flushing to disc.
/// </summary>
class BufferManager
{
public:
	BufferManager( uint32_t pageCount );
	~BufferManager();

	BufferFrame& FixPage( uint64_t pageId, bool exclusive );
	void UnfixPage( BufferFrame& frame, bool isDirty );

private:
	uint32_t mPageCount;
	// Memory and Buffer related
	uint8_t* mBufferMemory = nullptr;
	std::vector<BufferFrame> mFrames;
	RWLock mHashMapLock;
	std::unordered_map<uint64_t, BufferFrame*> mLoadedFrames;

	// Helpers
	BufferFrame* FindReplacementPage();
	void LoadPage( BufferFrame& frame );
	void WritePage( BufferFrame& frame );
	std::pair<uint64_t, uint64_t> SplitPageId( uint64_t pageId );
};

#endif
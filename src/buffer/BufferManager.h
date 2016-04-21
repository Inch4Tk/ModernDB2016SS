#pragma once
#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include "BufferFrame.h"

#include <stdint.h>
#include <utility>
#include <vector>

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
	// TODO:
	// Hash-Map containing page -> buffer
	// Some sort of List containing unfixed pages (sorted in a scheme that allows for reuse)

	// Helpers
	void LoadPage( BufferFrame& frame );
	void WritePage( BufferFrame& frame );
	std::pair<uint64_t, uint64_t> SplitPageId( uint64_t pageId );
};

#endif
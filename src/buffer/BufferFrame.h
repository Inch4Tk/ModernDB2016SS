#pragma once
#ifndef BUFFER_FRAME_H
#define BUFFER_FRAME_H

#include <stdint.h>

class BufferFrame
{
public:
	BufferFrame();
	~BufferFrame();

	uint64_t GetPageId() const;
	void* GetData() const;

private:
	void* mData = nullptr;
	uint64_t mPageId = 0;
};

#endif
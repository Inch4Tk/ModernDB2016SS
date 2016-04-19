#pragma once
#ifndef BUFFER_FRAME_H
#define BUFFER_FRAME_H

#include <stdint.h>

class BufferFrame
{
public:
	BufferFrame();
	~BufferFrame();

	void* GetData() const;
private:
	void* mData = nullptr;
};

#endif
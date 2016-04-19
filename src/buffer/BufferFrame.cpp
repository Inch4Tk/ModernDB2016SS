#include "BufferFrame.h"

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

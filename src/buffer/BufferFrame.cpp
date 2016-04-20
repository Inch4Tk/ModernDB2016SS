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
/// Determines whether this instance is fixed.
/// </summary>
/// <returns></returns>
bool BufferFrame::IsFixed() const
{
	return mFixed;
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

#pragma once
#ifndef BPTREENODE_H
#define BPTREENODE_H

#include "utility/defines.h"

#include <stdint.h>
#include <utility>

/// <summary>
/// Parameterized B+ Tree implementation. Operations are reentrant.
/// </summary>
template <class T, typename CMP>
class BPTreeNode
{
public:
	uint32_t BinarySearch( T key );
	bool IsRoot();
	bool IsLeaf();
	uint32_t GetFreeCount();
	T GetKey(uint32_t n);
private:
	uint8_t mRootMarker; // 0 == root, > 0 == not root, this is a convenience thing for checking after acquiring a lock on bufferframe
	uint8_t mNodeType; // 0 == leaf, >0 == inner
	uint16_t mPadding; // Some padding to be 32 bit aligned
	uint32_t mCount; // Number of entries
	uint64_t mNextUpper; // If leaf then this contains pageid of next leaf node. If inner this contains pageid of highest page
	uint8_t mData[DB_PAGE_SIZE - 16]; // key/child or key/tid pairs. Reduce size by the header values
};

/// <summary>
/// Gets the n-th key. (0 indexed)
/// </summary>
/// <param name="n">The n.</param>
/// <returns></returns>
template <class T, typename CMP>
T BPTreeNode<T, CMP>::GetKey( uint32_t n )
{
	const uint32_t pairsize = sizeof( T ) + sizeof( uint64_t );
	return *reinterpret_cast<T*>(&mData[n*pairsize]);
}

/// <summary>
/// Perform a binary search over the key/x pairs. Returns offset in mData to the spot where key > left side and key <= right side
/// </summary>
/// <param name="key">The key.</param>
/// <returns></returns>
template <class T, typename CMP>
uint32_t BPTreeNode<T, CMP>::BinarySearch( T key )
{
	uint32_t start = 0;
	uint32_t end = mCount;
	while (true)
	{
		if ( start == end )
		{
			return start;
		}
		uint32_t current = (end - start) / 2 + start; // Prevent overflows

		T currentKey = GetKey( current );

		if ( key < currentKey )
		{
			end = current;
		}
		else if ( key > currentKey )
		{
			start = current + 1;
		}
		else
		{
			return current;
		}
	}
}

/// <summary>
/// Determines whether this instance is a leaf.
/// </summary>
/// <returns></returns>
template <class T, typename CMP>
bool BPTreeNode<T, CMP>::IsLeaf()
{
	return mNodeType == 0;
}

/// <summary>
/// Determines whether this instance is the root.
/// </summary>
/// <returns></returns>
template <class T, typename CMP>
bool BPTreeNode<T, CMP>::IsRoot()
{
	return mRootMarker == 0;
}

/// <summary>
/// Gets the amount of free key-x pairs.
/// </summary>
/// <returns></returns>
template <class T, typename CMP>
uint32_t BPTreeNode<T, CMP>::GetFreeCount()
{
	const uint32_t pagebytes = (DB_PAGE_SIZE - 16);
	const uint32_t pairsize = sizeof( T ) + sizeof( uint64_t );
	assert( pagebytes / pairsize >= mCount );
	return pagebytes / pairsize - mCount;
}

#endif
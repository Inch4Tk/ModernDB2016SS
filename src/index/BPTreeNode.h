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
	// Getter type methods
	uint32_t BinarySearch( T key );
	bool IsRoot();
	bool IsLeaf();
	uint64_t GetNextUpper();
	uint32_t GetCount();
	uint32_t GetFreeCount();
	uint64_t GetValue( uint32_t index );
	T GetKey( uint32_t index );

	// Setter type methods
	void MakeInner();
	void MakeNotRoot();
	void SetValue( uint32_t index, uint64_t value );
	void SetNextUpper( uint64_t nextUpper );
	uint32_t InsertShift( T key, uint64_t value );
	void Erase( uint32_t index );
	T SplitTo( BPTreeNode* other );

private:
	uint8_t mRootMarker; // 0 == root, >0 == not root, this is a convenience thing for checking after acquiring a lock on bufferframe
	uint8_t mNodeType; // 0 == leaf, >0 == inner
	uint16_t mPadding; // Some padding to be 32 bit aligned
	uint32_t mCount; // Number of entries
	uint64_t mNextUpper; // If leaf then this contains pageid of next leaf node. If inner this contains pageid of highest page
	uint8_t mData[DB_PAGE_SIZE - 16]; // key/child or key/tid pairs. Reduce size by the header values
};

/// <summary>
/// Gets the next upper.
/// </summary>
/// <returns></returns>
template <class T, typename CMP>
uint64_t BPTreeNode<T, CMP>::GetNextUpper()
{
	return mNextUpper;
}

/// <summary>
/// Sets the value at index position. Automatically sets to next upper if out of bounds to the right.
/// </summary>
/// <param name="index">The index.</param>
/// <param name="value">The value.</param>
template <class T, typename CMP>
void BPTreeNode<T, CMP>::SetValue( uint32_t index, uint64_t value )
{
	if ( index + 1 > mCount ) // prevent uint overflows, just shift the -1
	{
		mNextUpper = value;
	}
	const uint32_t pairsize = sizeof( T ) + sizeof( uint64_t );
	*reinterpret_cast<uint64_t*>(&mData[index * pairsize] + sizeof( T )) = value;
}

/// <summary>
/// Gets the count.
/// </summary>
/// <returns></returns>
template <class T, typename CMP>
uint32_t BPTreeNode<T, CMP>::GetCount()
{
	return mCount;
}

/// <summary>
/// Performs a split to the other node. Assumes other is an empty node. Returns the biggest key on the left side.
/// </summary>
/// <param name="other">The other.</param>
template <class T, typename CMP>
T BPTreeNode<T, CMP>::SplitTo( BPTreeNode* other )
{
	const uint32_t pairsize = sizeof( T ) + sizeof( uint64_t );
	uint32_t newcountRight = mCount / 2;
	uint32_t newcountLeft = newcountRight + mCount % 2; // If uneven, give it to left side
	assert( newcountLeft + newcountRight == mCount );
	memcpy( other->mData, &this->mData[newcountLeft * pairsize], newcountRight * pairsize );
	memset( &this->mData[newcountLeft * pairsize], 0, newcountRight * pairsize );
	mCount = newcountLeft;
	other->mCount = newcountRight;
	return GetKey( mCount - 1 );
}

/// <summary>
/// Sets the next upper field.
/// </summary>
/// <param name="nextUpper">The next upper.</param>
template <class T, typename CMP>
void BPTreeNode<T, CMP>::SetNextUpper( uint64_t nextUpper )
{
	mNextUpper = nextUpper;
}

/// <summary>
/// Makes the node to a non-root node.
/// </summary>
template <class T, typename CMP>
void BPTreeNode<T, CMP>::MakeNotRoot()
{
	mRootMarker = 1;
}

/// <summary>
/// Makes the node to an inner node (does not change anything with root status)
/// </summary>
template <class T, typename CMP>
void BPTreeNode<T, CMP>::MakeInner()
{
	mNodeType = 1;
}

/// <summary>
/// Inserts the key value tuple. Shifts all bigger values to the right. Assumes there was space.
/// Returns the index in which the tuple was inserted.
/// </summary>
/// <param name="key">The key.</param>
/// <param name="value">The value.</param>
template <class T, typename CMP>
uint32_t BPTreeNode<T, CMP>::InsertShift( T key, uint64_t value )
{
	assert( GetFreeCount() > 0 );
	uint32_t index = BinarySearch( key );
	const uint32_t pairsize = sizeof( T ) + sizeof( uint64_t );
	uint32_t startpos = index * pairsize;
	uint32_t sizeAfter = (mCount - index) * pairsize;

	memmove( &mData[startpos], &mData[startpos + pairsize], sizeAfter );
	memcpy( &mData[startpos], &key, sizeof( T ) );
	memcpy( &mData[startpos + sizeof( T )], &value, sizeof( uint64_t ) );
	++mCount;
	return index;
}

/// <summary>
/// Erases the index key value tuple and shifts all the entries afterwards to the left.
/// </summary>
/// <param name="n">The n.</param>
template <class T, typename CMP>
void BPTreeNode<T, CMP>::Erase( uint32_t index )
{
	if ( n >= mCount )
	{
		return;
	}
	--mCount; // Decrement amount of entries, this way we safe ourselves 2 minus ops (doesn't matter but we take it)
	const uint32_t pairsize = sizeof( T ) + sizeof( uint64_t );
	uint32_t startpos = index * pairsize;
	uint32_t sizeAfter = (mCount - index) * pairsize;
	memmove( &mData[startpos], &mData[startpos + pairsize], sizeAfter );
	memset( &mData[mCount], 0, 8 );
}

/// <summary>
/// Gets the n-th value. If n is bigger than count - 1 we get the content of the next upper field.
/// </summary>
/// <param name="n">The n.</param>
/// <returns></returns>
template <class T, typename CMP>
uint64_t BPTreeNode<T, CMP>::GetValue( uint32_t index )
{
	if ( index + 1 > mCount ) // prevent uint overflows, just shift the -1
	{
		return nextUpper;
	}
	const uint32_t pairsize = sizeof( T ) + sizeof( uint64_t );
	return *reinterpret_cast<uint64_t*>(&mData[index * pairsize] + sizeof(T));
}

/// <summary>
/// Gets the index key. (0 indexed)
/// </summary>
/// <param name="n">The n.</param>
/// <returns></returns>
template <class T, typename CMP>
T BPTreeNode<T, CMP>::GetKey( uint32_t index )
{
	const uint32_t pairsize = sizeof( T ) + sizeof( uint64_t );
	return *reinterpret_cast<T*>(&mData[index * pairsize]);
}

/// <summary>
/// Perform a binary search over the key/x pairs. Returns the index (into key/x pairs) where key > left side and key <= right side
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

		if ( CMP(key, currentKey) )
		{
			end = current;
		}
		else if ( CMP(currentKey, key) )
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
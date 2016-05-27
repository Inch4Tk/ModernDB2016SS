#pragma once
#ifndef BPTREE_H
#define BPTREE_H

#include "DBCore.h"
#include "buffer/BufferManager.h"
#include "utility/defines.h"

#include <stdint.h>
#include <utility>

/// <summary>
/// Parameterized B+ Tree implementation. Operations are reentrant.
/// </summary>
template <class T, typename CMP>
class BPTree
{
public:
	BPTree( DBCore& core, BufferManager& bm, uint64_t segmentId );
	~BPTree();

	bool Insert(T key, TID tid);
	bool Erase(T key);
	std::pair<bool, TID> Lookup(T key);

private:
	DBCore& mCore;
	BufferManager& mBufferManager;
	uint64_t mSegmentId;
	uint32_t sizeKey;
};

/// <summary>
/// Initializes a new instance of the <see cref="BPTree{T, CMP}"/> class.
/// </summary>
template <class T, typename CMP>
BPTree<T, CMP>::BPTree( DBCore& core, BufferManager& bm, uint64_t segmentId ) :
	mCore(core), mBufferManager(bm), segmentId(segmentId), sizeKey( sizeof( T ) )
{
}

/// <summary>
/// Finalizes an instance of the <see cref="BPTree{T, CMP}"/> class.
/// </summary>
template <class T, typename CMP>
BPTree<T, CMP>::~BPTree()
{

}

/// <summary>
/// Inserts the specified key, TID tuple.
/// </summary>
/// <param name="key">The key.</param>
/// <param name="tid">The tid.</param>
/// <returns></returns>
template <class T, typename CMP>
bool BPTree<T, CMP>::Insert( T key, TID tid )
{

}

/// <summary>
/// Erases the specified key.
/// </summary>
/// <param name="key">The key.</param>
/// <returns></returns>
template <class T, typename CMP>
bool BPTree<T, CMP>::Erase( T key )
{

}

/// <summary>
/// Looks up the specified key. Indicates success in the first return value.
/// </summary>
/// <param name="key">The key.</param>
/// <returns></returns>
template <class T, typename CMP>
std::pair<bool, TID> BPTree<T, CMP>::Lookup( T key )
{
	// Acquire root
	uint64_t rootId = mCore.GetRootOfIndex( mSegmentId );
	BufferFrame& frame = mBufferManager.FixPage( rootId, false );
	BPTreeNode<T, CMP>* curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame.GetData());
	// Make sure we are still in the root, if not, we retry
	if ( !curNode->IsRoot() )
	{
		mBufferManager.UnfixPage( frame, false );
		return Lookup( key );
	}

	// Traverse the tree until we are in a leaf
	while (!curNode->IsLeaf())
	{
		uint64_t index = curNode->BinarySearch( key );
		// Perform latch coupling
		BufferFrame& oldFrame = frame;
		frame = mBufferManager.FixPage( curNode->GetValue( index ), false );
		mBufferManager.UnfixPage( oldFrame, false );
		curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame.GetData());
	}
	// Once we arrived in our leaf we perform a last binary search for the element
	uint64_t index = curNode->BinarySearch( key );
	foundKey = curNode->GetKey( index );
	bool found = false;
	TID value = 0;
	if (foundKey == key)
	{
		found = true;
		value = curNode->GetValue( index );
	}

	mBufferManager.UnfixPage( frame, false );

	return std::make_pair( found, value );
}

#endif
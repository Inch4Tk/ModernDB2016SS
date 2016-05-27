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
	// Acquire root
	uint64_t rootId = mCore.GetRootOfIndex( mSegmentId );
	BufferFrame& frame = mBufferManager.FixPage( rootId, false );
	BPTreeNode<T, CMP>* curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame.GetData());
	// Make sure we are still in the root, if not, we retry
	if ( !curNode->IsRoot() )
	{
		mBufferManager.UnfixPage( frame, false );
		return Erase( key );
	}

	// Traverse the tree until we are in a leaf
	BufferFrame& oldFrame = frame;
	while ( !curNode->IsLeaf() )
	{
		uint64_t index = curNode->BinarySearch( key );
		// Perform latch coupling
		if (oldFrame != frame)
		{
			// Unfix before overwriting old frame
			mBufferManager.UnfixPage( oldFrame, false );
		}
		oldFrame = frame; // Contains parent page
		frame = mBufferManager.FixPage( curNode->GetValue( index ), false );
		curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame.GetData());
	}

	// Once we are in the leaf, we promote our lock to write lock
	// Since we held onto the parent with a read lock, we know that
	// the leaf could not have been split during the time we released our lock
	// Special case 1: Our root is a leaf, we can detect this if both flags are set, in this case
	// we never even went into the while loop, so oldframe == frame and we only have to release once.
	// Special case 2: Out root was a leaf, but while we promoted lock it got split, 
	// we check if we can find the key, if not, we just restart.
	bool wasRoot = curNode->IsRoot();
	uint64_t pageId = frame.GetPageId();
	mBufferManager.UnfixPage( frame, false );

	frame = mBufferManager.FixPage( pageId, true );
	curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame.GetData());
	
	uint32_t index = curNode->BinarySearch( key );
	uint32_t foundKey = curNode->GetKey( index );

	if ( curNode->IsRoot() )
	{
		// Special case 1:
		assert( foundKey == key );
		curNode->Erase( index );
		mBufferManager.UnfixPage( frame, true );
		return true;
	}
	else if ( wasRoot )
	{
		assert( !curNode->IsRoot() );
		// Special case 2
		if (foundKey == key)
		{
			// Found the key, remove and don't retry
			curNode->Erase( index );
			mBufferManager.UnfixPage( frame, true );
			return true;
		}
		else
		{
			// Retry
			mBufferManager.UnfixPage( frame, false );
			return Erase( key );
		}
	}

	// Normal case, we just remove the value and let go of the locks
	assert( foundKey == key );
	curNode->Erase( index );

	mBufferManager.UnfixPage( oldFrame, false );
	mBufferManager.UnfixPage( frame, true );
	return true;
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
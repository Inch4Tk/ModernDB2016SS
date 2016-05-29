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

	void PerformInsertSplit( BufferFrame& parent, BufferFrame& leftChild, BufferFrame& rightChild );
	void PerformInnerSplit( BufferFrame& parent, BufferFrame& leftChild, BufferFrame& rightChild );
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
	// Acquire root
	uint64_t rootId = mCore.GetRootOfIndex( mSegmentId );
	BufferFrame& frame = mBufferManager.FixPage( rootId, true );
	BPTreeNode<T, CMP>* curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame.GetData());
	// Make sure we are still in the root, if not, we retry
	if ( !curNode->IsRoot() )
	{
		mBufferManager.UnfixPage( frame, false );
		return Insert( key, tid );
	}

	// Traverse the tree until we are in a leaf
	// For convenience we traverse directly with coupled write locks to circumvent problems with splits 
	// occurring in inner nodes while we would promote to write locks preparing to split ourselves.
	// This has slightly worse performance, but implementing the other way seems like a true nightmare of special conditions.
	BufferFrame& parentFrame = frame;
	bool parentFrameDirty = false;
	bool frameDirty = false;
	while ( !curNode->IsLeaf() )
	{
		// Perform a split on the frame if necessary. Do this first, this way we also catch it if the root node
		// needs to be split.
		if ( curNode->GetFreeCount() < 1 )
		{
			// Create another frame
			uint64_t nextPageId = mCore.AddPagesToIndex( mSegmentId, 1 );
			BufferFrame& rightSideFrame = mBufferManager.FixPage( nextPageId, true );
			parentFrameDirty = true;
			frameDirty = true;
			PerformInnerSplit( key, parentFrame, frame, rightSideFrame );
			assert( parentFrame != frame );
		}

		uint64_t index = curNode->BinarySearch( key );
		// Perform latch coupling
		if ( parentFrame != frame )
		{
			// Unfix before overwriting parent frame
			mBufferManager.UnfixPage( parentFrame, parentFrameDirty );
		}
		parentFrame = frame; // Contains parent page
		frame = mBufferManager.FixPage( curNode->GetValue( index ), true );
		parentFrameDirty = frameDirty;
		frameDirty = false;
		curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame.GetData());
	}

	// State: We found a leaf. We performed splits on all inner pages that were full, to make sure we can always insert another entry to parent.
	// We have a write lock on the parent (oldFrame). We have a write lock on the leaf (frame).
	// Case 1: There is free space in the leaf, we just insert and are done
	// Case 2: Split leaf, insert entry on proper side. Insert maximum of left page as separator key, value pointing to left side. 
	//         If this is the rightmost key, update the nextUpper field. If not, update the next entry's value point to the right side.
	// Special Case Root: If the root is a leaf. Everything is fine if we can just insert. We just need to remove only once.
	//         If it is full, we need to create a new root and split our page as well.
	if ( curNode->GetFreeCount() > 0 )
	{
		curNode->InsertShift( key, tid );
		if (!curNode->IsRoot())
		{
			mBufferManager.UnfixPage( parentFrame, parentFrameDirty );
		}
		mBufferManager.UnfixPage( frame, true );
		return true;
	}
	else if ( curNode->IsRoot() )
	{
		assert( parentFrame == frame );
		// Special case. Root == Leaf, but it is full.
		// We create a new inner page and a new right page.
		uint64_t nextPageId = mCore.AddPagesToIndex( mSegmentId, 2 );
		BufferFrame& rightSideFrame = mBufferManager.FixPage( nextPageId - 1, true );
		parentFrame = mBufferManager.FixPage( nextPageId, true );

		// Before we split, we need to transfer root from left to parent
		// we also make parent an inner node.
		// The right node is handled by perform insert split
		curNode->MakeNotRoot();
		BPTreeNode<T, CMP>* parentNode = reinterpret_cast<BPTreeNode<T, CMP>*>(parentFrame.GetData());
		parentNode->MakeInner();
		// Also tell our core we changed root.
		mCore.SetRootOfIndex( mSegmentId, nextPageId );

		// Perform the actual split and insertion
		PerformInsertSplit( parentFrame, frame, rightSideFrame );
		return true;
	}
	else
	{
		// We split, so first we acquire an empty new page.
		uint64_t nextPageId = mCore.AddPagesToIndex( mSegmentId, 1 );
		BufferFrame& rightSideFrame = mBufferManager.FixPage( nextPageId, true );
		PerformInsertSplit( parentFrame, frame, rightSideFrame );
		return true;
	}
	assert( false );
	return true;
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

/// <summary>
/// Performs a split. Assumes parent has space and right child is empty.
/// </summary>
/// <param name="parent">The parent.</param>
/// <param name="leftChild">The left child.</param>
/// <param name="rightChild">The right child.</param>
template <class T, typename CMP>
void BPTree<T, CMP>::PerformInsertSplit( BufferFrame& parent, BufferFrame& leftChild, BufferFrame& rightChild )
{
	BPTreeNode<T, CMP>* parentNode = reinterpret_cast<BPTreeNode<T, CMP>*>(parent.GetData());
	BPTreeNode<T, CMP>* leftNode = reinterpret_cast<BPTreeNode<T, CMP>*>(leftChild.GetData());
	BPTreeNode<T, CMP>* rightNode = reinterpret_cast<BPTreeNode<T, CMP>*>(rightChild.GetData());
	rightNode->MakeNotRoot();
	rightNode->SetNextUpper( leftNode->GetNextUpper() );
	leftNode->SetNextUpper( nextPageId );

	// Perform the actual split
	T biggestKeyLeft = leftNode->SplitTo( rightNode );
	if ( CMP( biggestKeyLeft, key ) )
	{
		// biggestKeyLeft < key
		// Insert to the right
		rightNode->InsertShift( key, value );
	}
	else
	{
		// Insert to the left
		leftNode->InsertShift( key, value );
	}
	// Propagate split to parent
	uint32_t insertionIndex = parentNode->InsertShift( biggestKeyLeft, frame.GetPageId() );
	// Set right page as the value to the right neighbor of our inserted key.
	// This automatically sets to nextUpper if out of bounds.
	parentNode->SetValue( insertionIndex + 1, nextPageId );

	mBufferManager.UnfixPage( oldFrame, true ); // Parent
	mBufferManager.UnfixPage( frame, true ); // Left side
	mBufferManager.UnfixPage( rightSideFrame, true );
}

/// <summary>
/// Performs the inner node split. If parent and left child are the same, will build a root node on top.
/// Will put the parent frame into parent, and the correct next child into left child. Right child will be dirty unfixed.
/// </summary>
/// <param name="parent">The parent.</param>
/// <param name="leftChild">The left child.</param>
/// <param name="rightChild">The right child.</param>
template <class T, typename CMP>
void BPTree<T, CMP>::PerformInnerSplit( T key, BufferFrame& parent, BufferFrame& leftChild, BufferFrame& rightChild )
{
	BPTreeNode<T, CMP>* leftNode = reinterpret_cast<BPTreeNode<T, CMP>*>(leftChild.GetData());
	BPTreeNode<T, CMP>* rightNode = reinterpret_cast<BPTreeNode<T, CMP>*>(rightChild.GetData());
	
	rightNode->MakeInner();
	rightNode->MakeNotRoot();
	T leftMaxKey = leftNode->SplitTo( rightNode );

	// Check which side will be the correct side for later
	bool leftCorrect = true;
	if ( CMP( leftMaxKey, key ) )
	{
		// Left side < key, so we have to go with the right side
		leftCorrect = false;
	}

	// Propagate left max key up
	if ( leftNode->IsRoot() )
	{
		// Case where we are currently in the root, this means we need to create another parent and transfer root
		assert( parent == leftChild );
		uint64_t rootPageId = mCore.AddPagesToIndex( mSegmentId, 1 );
		parent = std::move( mBufferManager.FixPage( rootPageId, true ) );
		BPTreeNode<T, CMP>* parentNode = reinterpret_cast<BPTreeNode<T, CMP>*>(rightSideFrame.GetData());
		leftNode->MakeNotRoot();
		parentNode->MakeInner();
		parentNode->InsertShift( leftMaxKey, frame.GetPageId() );
		parentNode->SetNextUpper( nextPageId );
		mCore.SetRootOfIndex( mSegmentId, rootPageId );
	}
	else
	{
		// Case where we are not in the root, so we can assume there is space above us and we
		// already have the parent frame in parentFrame
		assert( parent != leftChild );
		BPTreeNode<T, CMP>* parentNode = reinterpret_cast<BPTreeNode<T, CMP>*>(rightSideFrame.GetData());
		uint32_t insIndex = parentNode->InsertShift( leftMaxKey, frame.GetPageId() );
		parentNode->SetValue( insIndex, nextPageId );
	}

	if ( !leftCorrect )
	{
		// Swap left child and right child
		BufferFrame tmp = std::move( leftChild );
		leftChild = std::move( rightChild );
		rightChild = std::move( tmp );
	}

	// This might not contain the right child anymore
	mBufferManager.UnfixPage( rightChild, true ); 
}


#endif
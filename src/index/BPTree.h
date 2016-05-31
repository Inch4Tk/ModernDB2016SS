#pragma once
#ifndef BPTREE_H
#define BPTREE_H

#include "DBCore.h"
#include "buffer/BufferManager.h"
#include "utility/defines.h"
#include "index/BPTreeNode.h"

#include <stdint.h>
#include <utility>
#include <cassert>

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
	uint32_t GetSize();
private:
	DBCore& mCore;
	BufferManager& mBufferManager;
	uint64_t mSegmentId;

	void LeafSplit( T key, uint64_t value, BufferFrame* parent, BufferFrame* leftChild, BufferFrame* rightChild );
	void InnerSplit( T key, BufferFrame** parent, BufferFrame** leftChild, BufferFrame** rightChild );
};

/// <summary>
/// Initializes a new instance of the <see cref="BPTree{T, CMP}"/> class.
/// </summary>
template <class T, typename CMP>
BPTree<T, CMP>::BPTree( DBCore& core, BufferManager& bm, uint64_t segmentId ) :
	mCore(core), mBufferManager(bm), mSegmentId(segmentId)
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
/// Gets the size.
/// </summary>
/// <returns></returns>
template <class T, typename CMP>
uint32_t BPTree<T, CMP>::GetSize()
{
	// Acquire root
	uint64_t rootId = mCore.GetRootOfIndex( mSegmentId );
	BufferFrame* frame = &mBufferManager.FixPage( rootId, false );
	BPTreeNode<T, CMP>* curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	// Make sure we are still in the root, if not, we retry
	if ( !curNode->IsRoot() )
	{
		mBufferManager.UnfixPage( *frame, false );
		return GetSize();
	}

	// Traverse the tree until we are in the leftmost leaf
	while ( !curNode->IsLeaf() )
	{
		// Perform latch coupling
		BufferFrame* oldFrame = frame;
		frame = &mBufferManager.FixPage( curNode->GetValue( 0 ), false );
		mBufferManager.UnfixPage( *oldFrame, false );
		curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	}
	uint32_t sizesum = 0;
	assert( curNode->IsLeaf() );
	sizesum += curNode->GetCount();
	while ( curNode->GetNextUpper() != 0 )
	{
		BufferFrame* oldFrame = frame;
		frame = &mBufferManager.FixPage( curNode->GetNextUpper(), false );
		mBufferManager.UnfixPage( *oldFrame, false );
		curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
		assert( curNode->IsLeaf() );
		sizesum += curNode->GetCount();
	}
	mBufferManager.UnfixPage( *frame, false );
	return sizesum;
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
	BufferFrame* frame = &mBufferManager.FixPage( rootId, true );
	BPTreeNode<T, CMP>* curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	BPTreeNode<T, CMP>* parNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	// Make sure we are still in the root, if not, we retry
	if ( !curNode->IsRoot() )
	{
		mBufferManager.UnfixPage( *frame, false );
		return Insert( key, tid );
	}

	// Traverse the tree until we are in a leaf
	// For convenience we traverse directly with coupled write locks to circumvent problems with splits 
	// occurring in inner nodes while we would promote to write locks preparing to split ourselves.
	// This has slightly worse performance, but implementing the other way seems like a true nightmare of special conditions.
	BufferFrame* parentFrame = frame;
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
			BufferFrame* rightSideFrame = &mBufferManager.FixPage( nextPageId, true );
			parentFrameDirty = true;
			frameDirty = true;
			InnerSplit( key, &parentFrame, &frame, &rightSideFrame );
			// Redo the current node, since we might have swapped left and right direction
			curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
			assert( *parentFrame != *frame );
		}

		uint32_t index = curNode->BinarySearch( key );
		//assert( curNode->GetValue( index ) >= mSegmentId );
		if ( curNode->GetValue( index ) < mSegmentId )
		{
			index = curNode->BinarySearch( key );
		}
		
		// Perform latch coupling
		if ( *parentFrame != *frame )
		{
			// Unfix before overwriting parent frame
			mBufferManager.UnfixPage( *parentFrame, parentFrameDirty );
		}
		parentFrame = frame; // Contains parent page
		frame = &mBufferManager.FixPage( curNode->GetValue( index ), true );
		parentFrameDirty = frameDirty;
		frameDirty = false;
		curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
		parNode = reinterpret_cast<BPTreeNode<T, CMP>*>(parentFrame->GetData());
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
			mBufferManager.UnfixPage( *parentFrame, parentFrameDirty );
		}
		mBufferManager.UnfixPage( *frame, true );
		return true;
	}
	else if ( curNode->IsRoot() )
	{
		assert( *parentFrame == *frame );
		// Special case. Root == Leaf, but it is full.
		// We create a new inner page and a new right page.
		uint64_t nextPageId = mCore.AddPagesToIndex( mSegmentId, 2 );
		BufferFrame* rightSideFrame = &mBufferManager.FixPage( nextPageId - 1, true );
		parentFrame = &mBufferManager.FixPage( nextPageId, true );

		// Before we split, we need to transfer root from left to parent
		// we also make parent an inner node.
		// The right node is handled by perform insert split
		curNode->MakeNotRoot();
		BPTreeNode<T, CMP>* parentNode = reinterpret_cast<BPTreeNode<T, CMP>*>(parentFrame->GetData());
		parentNode->MakeInner();
		// Also tell our core we changed root.
		mCore.SetRootOfIndex( mSegmentId, nextPageId );

		// Perform the actual split and insertion
		LeafSplit( key, tid, parentFrame, frame, rightSideFrame );
		return true;
	}
	else
	{
		// We split, so first we acquire an empty new page.
		uint64_t nextPageId = mCore.AddPagesToIndex( mSegmentId, 1 );
		BufferFrame* rightSideFrame = &mBufferManager.FixPage( nextPageId, true );
		LeafSplit( key, tid, parentFrame, frame, rightSideFrame );
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
	CMP comparer;
	// Acquire root
	uint64_t rootId = mCore.GetRootOfIndex( mSegmentId );
	BufferFrame* frame = &mBufferManager.FixPage( rootId, false );
	BPTreeNode<T, CMP>* curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	// Make sure we are still in the root, if not, we retry
	if ( !curNode->IsRoot() )
	{
		mBufferManager.UnfixPage( *frame, false );
		return Erase( key );
	}

	// Traverse the tree until we are in a leaf
	BufferFrame* oldFrame = frame;
	while ( !curNode->IsLeaf() )
	{
		uint32_t index = curNode->BinarySearch( key );
		assert( curNode->GetValue( index ) >= mSegmentId );
		// Perform latch coupling
		if (*oldFrame != *frame)
		{
			// Unfix before overwriting old frame
			mBufferManager.UnfixPage( *oldFrame, false );
		}
		oldFrame = frame; // Contains parent page
		frame = &mBufferManager.FixPage( curNode->GetValue( index ), false );
		curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	}

	// Once we are in the leaf, we promote our lock to write lock
	// Since we held onto the parent with a read lock, we know that
	// the leaf could not have been split during the time we released our lock
	// Special case 1: Our root is a leaf, we can detect this if both flags are set, in this case
	// we never even went into the while loop, so oldframe == frame and we only have to release once.
	// Special case 2: Out root was a leaf, but while we promoted lock it got split, 
	// we check if we can find the key, if not, we just restart.
	bool wasRoot = curNode->IsRoot();
	uint64_t pageId = frame->GetPageId();
	mBufferManager.UnfixPage( *frame, false );

	frame = &mBufferManager.FixPage( pageId, true );
	curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	
	uint32_t index = curNode->BinarySearch( key );
	T foundKey = curNode->GetKey( index );

	if ( curNode->IsRoot() )
	{
		// Special case 1:
		assert( !comparer( foundKey, key ) && !comparer( key, foundKey ) ); // Equality
		curNode->Erase( index );
		mBufferManager.UnfixPage( *frame, true );
		return true;
	}
	else if ( wasRoot )
	{
		assert( !curNode->IsRoot() );
		// Special case 2
		if ( !comparer( foundKey, key ) && !comparer( key, foundKey ) )
		{
			// Found the key, remove and don't retry
			curNode->Erase( index );
			mBufferManager.UnfixPage( *frame, true );
			return true;
		}
		else
		{
			// Retry
			mBufferManager.UnfixPage( *frame, false );
			return Erase( key );
		}
	}

	// Normal case, we just remove the value and let go of the locks
	if ( comparer( foundKey, key ) || comparer( key, foundKey ) ) // neq
	{
		mBufferManager.UnfixPage( *oldFrame, false );
		mBufferManager.UnfixPage( *frame, false );
		return false;
	}
	curNode->Erase( index );

	mBufferManager.UnfixPage( *oldFrame, false );
	mBufferManager.UnfixPage( *frame, true );
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
	CMP comparer;
	// Acquire root
	uint64_t rootId = mCore.GetRootOfIndex( mSegmentId );
	BufferFrame* frame = &mBufferManager.FixPage( rootId, false );
	BPTreeNode<T, CMP>* curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	// Make sure we are still in the root, if not, we retry
	if ( !curNode->IsRoot() )
	{
		mBufferManager.UnfixPage( *frame, false );
		return Lookup( key );
	}

	// Traverse the tree until we are in a leaf
	while (!curNode->IsLeaf())
	{
		uint32_t index = curNode->BinarySearch( key );
		assert( curNode->GetValue( index ) >= mSegmentId );
		// Perform latch coupling
		BufferFrame* oldFrame = frame;
		frame = &mBufferManager.FixPage( curNode->GetValue( index ), false );
		mBufferManager.UnfixPage( *oldFrame, false );
		curNode = reinterpret_cast<BPTreeNode<T, CMP>*>(frame->GetData());
	}
	// Once we arrived in our leaf we perform a last binary search for the element
	uint32_t index = curNode->BinarySearch( key );
	T foundKey = curNode->GetKey( index );
	bool found = false;
	TID value = 0;
	if ( !comparer( foundKey, key ) && !comparer( key, foundKey ) ) // Equality
	{
		found = true;
		value = curNode->GetValue( index );
	}

	mBufferManager.UnfixPage( *frame, false );

	return std::make_pair( found, value );
}

/// <summary>
/// Performs a split. Assumes parent has space and right child is empty.
/// </summary>
/// <param name="key">The key.</param>
/// <param name="value">The value.</param>
/// <param name="parent">The parent.</param>
/// <param name="leftChild">The left child.</param>
/// <param name="rightChild">The right child.</param>
template <class T, typename CMP>
void BPTree<T, CMP>::LeafSplit( T key, uint64_t value, BufferFrame* parent, BufferFrame* leftChild, BufferFrame* rightChild )
{
	CMP comparer;
	BPTreeNode<T, CMP>* parentNode = reinterpret_cast<BPTreeNode<T, CMP>*>(parent->GetData());
	BPTreeNode<T, CMP>* leftNode = reinterpret_cast<BPTreeNode<T, CMP>*>(leftChild->GetData());
	BPTreeNode<T, CMP>* rightNode = reinterpret_cast<BPTreeNode<T, CMP>*>(rightChild->GetData());
	rightNode->MakeNotRoot();
	rightNode->SetNextUpper( leftNode->GetNextUpper() );
	leftNode->SetNextUpper( rightChild->GetPageId() );

	// Perform the actual split
	T biggestKeyLeft = leftNode->SplitTo( rightNode );
	if ( comparer( biggestKeyLeft, key ) )
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
	uint32_t insertionIndex = parentNode->InsertShift( biggestKeyLeft, leftChild->GetPageId() );
	// Set right page as the value to the right neighbor of our inserted key.
	// This automatically sets to nextUpper if out of bounds.
	parentNode->SetValue( insertionIndex + 1, rightChild->GetPageId() );

	mBufferManager.UnfixPage( *parent, true ); // Parent
	mBufferManager.UnfixPage( *leftChild, true ); // Left side
	mBufferManager.UnfixPage( *rightChild, true );
}

/// <summary>
/// Performs the inner node split. If parent and left child are the same, will build a root node on top.
/// Will put the parent frame into parent, and the correct next child into left child. Right child will be dirty unfixed.
/// </summary>
/// <param name="parent">The parent.</param>
/// <param name="leftChild">The left child.</param>
/// <param name="rightChild">The right child.</param>
template <class T, typename CMP>
void BPTree<T, CMP>::InnerSplit( T key, BufferFrame** parent, BufferFrame** leftChild, BufferFrame** rightChild )
{
	CMP comparer;
	BPTreeNode<T, CMP>* leftNode = reinterpret_cast<BPTreeNode<T, CMP>*>((*leftChild)->GetData());
	BPTreeNode<T, CMP>* rightNode = reinterpret_cast<BPTreeNode<T, CMP>*>((*rightChild)->GetData());
	assert( !leftNode->IsLeaf() );
	
	rightNode->MakeInner();
	rightNode->MakeNotRoot();
	T leftMaxKey = leftNode->SplitTo( rightNode );
	rightNode->SetNextUpper( leftNode->GetNextUpper() );
	leftNode->SetNextUpper( 0 ); // Empty value, since we should have taken the right path instead of ending up here

	// Check which side will be the correct side for later
	bool leftCorrect = true;
	if ( comparer( leftMaxKey, key ) )
	{
		// Left side < key, so we have to go with the right side
		leftCorrect = false;
	}

	// Propagate left max key up
	if ( leftNode->IsRoot() )
	{
		// Case where we are currently in the root, this means we need to create another parent and transfer root
		assert( **parent == **leftChild );
		uint64_t rootPageId = mCore.AddPagesToIndex( mSegmentId, 1 );
		*parent = &mBufferManager.FixPage( rootPageId, true );
		BPTreeNode<T, CMP>* parentNode = reinterpret_cast<BPTreeNode<T, CMP>*>((*parent)->GetData());
		leftNode->MakeNotRoot();
		parentNode->MakeInner();
		parentNode->InsertShift( leftMaxKey, (*leftChild)->GetPageId() );
		parentNode->SetNextUpper( (*rightChild)->GetPageId() );
		mCore.SetRootOfIndex( mSegmentId, rootPageId );
	}
	else
	{
		// Case where we are not in the root, so we can assume there is space above us,
		// because we split everything without space while traversing and we
		// already have the parent frame in parentFrame
		assert( **parent != **leftChild );
		BPTreeNode<T, CMP>* parentNode = reinterpret_cast<BPTreeNode<T, CMP>*>((*parent)->GetData());
		uint32_t insIndex = parentNode->InsertShift( leftMaxKey, (*leftChild)->GetPageId() );
		parentNode->SetValue( insIndex + 1, (*rightChild)->GetPageId() );
	}

	if ( !leftCorrect )
	{
		// Swap left child and right child so the correct path is in left child, even though we might go down right child
		BufferFrame* tmp = *leftChild;
		*leftChild = *rightChild;
		*rightChild = tmp;
	}

	// This might not contain the right child anymore
	mBufferManager.UnfixPage( **rightChild, true ); 
}


#endif
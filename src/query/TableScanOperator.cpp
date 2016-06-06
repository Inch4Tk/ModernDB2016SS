#include "TableScanOperator.h"

#include "Register.h"
#include "DBCore.h"
#include "buffer/BufferManager.h"
#include "buffer/BufferFrame.h"

TableScanOperator::TableScanOperator( const std::string& relationName, DBCore& core, BufferManager& bm ) : 
	mCore(core), mBufferManager(bm)
{
	mSegmentId = mCore.GetSegmentIdOfRelation(relationName);
}

TableScanOperator::TableScanOperator( uint64_t segmentId, DBCore& core, BufferManager& bm ):
	mCore( core ), mBufferManager( bm ), mSegmentId( segmentId )
{

}

TableScanOperator::~TableScanOperator()
{

}

/// <summary>
/// Opens this instance.
/// </summary>
void TableScanOperator::Open()
{
	if (mCurFrame)
	{
		mBufferManager.UnfixPage( *mCurFrame, false );
	}
	mCurPageId = 0;
	mCurSlot = 0;
	mBufferManager.FixPage( mCurPageId, true );
}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool TableScanOperator::Next()
{
	return false;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> TableScanOperator::GetOutput()
{
	return std::vector<Register*>();
}

/// <summary>
/// Closes this instance.
/// </summary>
void TableScanOperator::Close()
{
	if( mCurFrame )
	{
		mBufferManager.UnfixPage( *mCurFrame, false );
		mCurFrame = nullptr;
	}
	mCurPageId = 0;
	mCurSlot = 0;
}

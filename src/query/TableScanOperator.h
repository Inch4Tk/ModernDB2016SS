#pragma once
#ifndef TABLE_SCAN_OPERATOR_H
#define TABLE_SCAN_OPERATOR_H

#include "query/QueryOperator.h"
#include <vector>

// Forwards
class Register;
class BufferManager;
class BufferFrame;
class DBCore;

// Scans a relation and produces all tuples as output.
class TableScanOperator : QueryOperator
{
public:
	TableScanOperator( const std::string& relationName, DBCore& core, BufferManager& bm );
	TableScanOperator( uint64_t segmentId, DBCore& core, BufferManager& bm );
	~TableScanOperator();
	
	void Open() override;
	bool Next() override;
	std::vector<Register*> GetOutput() override;
	void Close() override;

private:
	uint64_t mSegmentId = 0;
	uint64_t mCurPageId = 0;
	uint64_t mCurSlot = 0;
	BufferFrame* mCurFrame = nullptr;
	DBCore& mCore;
	BufferManager& mBufferManager;
	
};
#endif
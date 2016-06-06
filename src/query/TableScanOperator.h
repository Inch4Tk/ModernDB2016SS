#pragma once
#ifndef TABLE_SCAN_OPERATOR_H
#define TABLE_SCAN_OPERATOR_H

#include "query/QueryOperator.h"
#include <vector>

// Forwards
class Register;
class BufferManager;
class DBCore;

// Scans a relation and produces all tuples as output.
class TableScanOperator : QueryOperator
{
public:
	TableScanOperator( DBCore& core, BufferManager& bm );
	~TableScanOperator();
	
	void Open() override;
	bool Next() override;
	std::vector<Register*> GetOutput() override;
	void Close() override;

private:
	DBCore& mCore;
	BufferManager& mBufferManager;

};
#endif
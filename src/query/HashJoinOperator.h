#pragma once
#ifndef HASH_JOIN_OPERATOR_H
#define HASH_JOIN_OPERATOR_H

#include "query/QueryOperator.h"
#include <vector>

// Forwards
class Register;

// Compute inner join by storing left input in main memory, then find matches 
// for each tuple from the right side. The predicate is of the form left.a = right.b.
class HashJoinOperator : public QueryOperator
{
public:
	HashJoinOperator(QueryOperator& input);
	~HashJoinOperator();
	
	void Open() override;
	bool Next() override;
	std::vector<Register*> GetOutput() override;
	void Close() override;

private:
	QueryOperator& mInput;

};
#endif
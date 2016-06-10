#pragma once
#ifndef HASH_JOIN_OPERATOR_H
#define HASH_JOIN_OPERATOR_H

#include "sql/SchemaTypes.h"
#include "query/QueryOperator.h"
#include <vector>
#include <unordered_map>

// Forwards
class Register;

// Compute inner join by storing left input in main memory, then find matches 
// for each tuple from the right side. The predicate is of the form left.a = right.b.
// Will produce all possible combinations of tuples
class HashJoinOperator : public QueryOperator
{
public:
	HashJoinOperator(QueryOperator& inputLeft, QueryOperator& inputRight, std::string leftAttrName, std::string rightAttrName );
	~HashJoinOperator();
	
	void Open() override;
	bool Next() override;
	std::vector<Register*> GetOutput() override;
	void Close() override;

private:
	bool mAttrIsString;
	QueryOperator& mInputLeft;
	QueryOperator& mInputRight;

	// Register ids
	std::string mLeftAttrName;
	std::string mRightAttrName;
	uint32_t mLeftId;
	uint32_t mRightId;

	// Hashmap storage
	std::unordered_multimap<Integer, std::vector<Register>> mHashInt;
	std::unordered_multimap<std::string, std::vector<Register>> mHashString;
	uint32_t mIteratorOffest;

	// Register vectors
	std::vector<Register*> mInputRegisterLeft;
	std::vector<Register*> mInputRegisterRight;
	std::vector<Register*> mOutputRegister;
};
#endif
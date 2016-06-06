#pragma once
#ifndef SELECT_OPERATOR_H
#define SELECT_OPERATOR_H

#include "query/QueryOperator.h"
#include <vector>

// Forwards
class Register;

// Implements predicates of the form a = c where a is an attribute and c is a constant
class SelectOperator : QueryOperator
{
public:
	SelectOperator(QueryOperator& input);
	~SelectOperator();
	
	void Open() override;
	bool Next() override;
	std::vector<Register*> GetOutput() override;
	void Close() override;

private:
	QueryOperator& mInput;

};
#endif
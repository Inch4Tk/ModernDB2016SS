#pragma once
#ifndef SELECT_OPERATOR_H
#define SELECT_OPERATOR_H

#include "sql/SchemaTypes.h"
#include "query/QueryOperator.h"
#include <vector>

// Forwards
class Register;

// Implements predicates of the form a = c where a is an attribute and c is a constant
class SelectOperator : public QueryOperator
{
public:
	SelectOperator( QueryOperator& input, std::string targetAttrName, std::string constant );
	SelectOperator( QueryOperator& input, std::string targetAttrName, uint32_t constant );
	~SelectOperator();
	
	void Open() override;
	bool Next() override;
	std::vector<Register*> GetOutput() override;
	void Close() override;

private:
	QueryOperator& mInput;
	bool mConstantIsString;
	std::string mStrConstant;
	Integer mIntConstant;
	std::string mTargetAttrName;
	uint32_t mTargetRegister;
	std::vector<Register*> mInputRegister;
};
#endif
#pragma once
#ifndef PRINT_OPERATOR_H
#define PRINT_OPERATOR_H

#include "query/QueryOperator.h"
#include <vector>
#include <iostream>

// Forwards
class Register;

// Prints out all input tuples in a human-readable format.
class PrintOperator : public QueryOperator
{
public:
	PrintOperator( QueryOperator& input, std::ostream& outstream );
	~PrintOperator();
	
	void Open() override;
	bool Next() override;
	std::vector<Register*> GetOutput() override;
	void Close() override;

private:
	QueryOperator& mInput;
	std::ostream& mOutstream;
	std::vector<Register*> mInputRegister;
};
#endif
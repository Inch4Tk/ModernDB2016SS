#pragma once
#ifndef QUERY_OPERATOR_H
#define QUERY_OPERATOR_H

#include <vector>

// Forwards
class Register;

class QueryOperator
{
public:
	virtual void Open() = 0;
	virtual bool Next() = 0;
	virtual std::vector<Register*> GetOutput() = 0;
	virtual void Close() = 0;

};
#endif
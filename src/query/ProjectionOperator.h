#pragma once
#ifndef PROJECTION_OPERATOR_H
#define PROJECTION_OPERATOR_H

#include "query/QueryOperator.h"
#include <vector>

// Forwards
class Register;

// Projects to a subset of the input schema.
class ProjectionOperator: public QueryOperator
{
public:
	ProjectionOperator(QueryOperator& input);
	~ProjectionOperator();
	
	void Open() override;
	bool Next() override;
	std::vector<Register*> GetOutput() override;
	void Close() override;

private:
	QueryOperator& mInput;

};
#endif
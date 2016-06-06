#include "HashJoinOperator.h"

#include "Register.h"

HashJoinOperator::HashJoinOperator( QueryOperator& input ) : mInput(input)
{

}

HashJoinOperator::~HashJoinOperator()
{

}

/// <summary>
/// Opens this instance.
/// </summary>
void HashJoinOperator::Open()
{

}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool HashJoinOperator::Next()
{
	return false;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> HashJoinOperator::GetOutput()
{
	return std::vector<Register*>();
}

/// <summary>
/// Closes this instance.
/// </summary>
void HashJoinOperator::Close()
{

}

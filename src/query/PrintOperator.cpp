#include "PrintOperator.h"

#include "Register.h"

PrintOperator::PrintOperator( QueryOperator& input ) : mInput(input)
{

}

PrintOperator::~PrintOperator()
{

}

/// <summary>
/// Opens this instance.
/// </summary>
void PrintOperator::Open()
{

}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool PrintOperator::Next()
{
	return true;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> PrintOperator::GetOutput()
{
	return std::vector<Register*>();
}

/// <summary>
/// Closes this instance.
/// </summary>
void PrintOperator::Close()
{

}

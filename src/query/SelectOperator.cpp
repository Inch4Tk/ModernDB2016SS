#include "SelectOperator.h"

#include "Register.h"

SelectOperator::SelectOperator( QueryOperator& input ) : mInput(input)
{

}

SelectOperator::~SelectOperator()
{

}

/// <summary>
/// Opens this instance.
/// </summary>
void SelectOperator::Open()
{

}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool SelectOperator::Next()
{
	return false;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> SelectOperator::GetOutput()
{
	return std::vector<Register*>();
}

/// <summary>
/// Closes this instance.
/// </summary>
void SelectOperator::Close()
{

}

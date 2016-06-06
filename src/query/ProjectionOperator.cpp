#include "ProjectionOperator.h"

#include "Register.h"

ProjectionOperator::ProjectionOperator( QueryOperator& input ) : mInput(input)
{

}

ProjectionOperator::~ProjectionOperator()
{

}

/// <summary>
/// Opens this instance.
/// </summary>
void ProjectionOperator::Open()
{

}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool ProjectionOperator::Next()
{
	return false;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> ProjectionOperator::GetOutput()
{
	return std::vector<Register*>();
}

/// <summary>
/// Closes this instance.
/// </summary>
void ProjectionOperator::Close()
{

}

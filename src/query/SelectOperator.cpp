#include "SelectOperator.h"

#include "Register.h"
#include <cassert>

SelectOperator::SelectOperator( QueryOperator& input, std::string targetAttrName, uint32_t constant ) : 
	mInput( input ), mTargetAttrName( targetAttrName ), mIntConstant( constant ), mConstantIsString( false )
{

}

SelectOperator::SelectOperator( QueryOperator& input, std::string targetAttrName, std::string constant ) : 
	mInput( input ), mTargetAttrName( targetAttrName ), mStrConstant( constant ), mConstantIsString( true )
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
	mInput.Open();
	mInputRegister = mInput.GetOutput();
	// Get correct comparison index
	uint32_t curidx = 0;
	bool found = false;
	for ( Register* r : mInputRegister )
	{
		if ( r->GetAttributeName() == mTargetAttrName )
		{
			found = true;
			mTargetRegister = curidx;
			if (mConstantIsString)
				assert( r->GetType() == SchemaTypes::Tag::Char );
			else
				assert( r->GetType() == SchemaTypes::Tag::Integer );
			break;
		}
		++curidx;
	}
	assert( found );
}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool SelectOperator::Next()
{
	if (mInput.Next())
	{
		bool found = false;
		if (mConstantIsString)
		{
			found = mInputRegister[mTargetRegister]->GetString() == mStrConstant;
		}
		else
		{
			found = mInputRegister[mTargetRegister]->GetInteger() == mIntConstant;
		}

		if ( found )
			return true;
		return Next();
	}
	return false;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> SelectOperator::GetOutput()
{
	return mInputRegister;
}

/// <summary>
/// Closes this instance.
/// </summary>
void SelectOperator::Close()
{
	mInput.Close();
}

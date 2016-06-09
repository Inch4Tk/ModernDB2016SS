#include "ProjectionOperator.h"

#include "Register.h"
#include <cassert>

ProjectionOperator::ProjectionOperator( QueryOperator& input, std::vector<uint32_t> indices ) : mInput(input), mIndices(indices)
{

}

ProjectionOperator::ProjectionOperator( QueryOperator& input, std::vector<std::string> attrNames ) : mInput( input ), mAttrNames(attrNames)
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
	mInput.Open();
	mInputRegister = mInput.GetOutput();

	// Fill indices or attribute names if they are empty
	if (mIndices.size() > 0 && mAttrNames.size() == 0 )
	{
		for ( uint32_t i : mIndices )
		{
			assert( i < mInputRegister.size() );
			mAttrNames.push_back( mInputRegister[i]->GetAttributeName() );
		}
	}
	else if (mAttrNames.size() > 0 && mIndices.size() == 0)
	{
		for ( std::string s : mAttrNames )
		{
			uint32_t curidx = 0;
			for ( Register* r : mInputRegister )
			{
				if (r->GetAttributeName() == s)
				{
					mIndices.push_back( curidx );
				}
				++curidx;
			}
		}
	}

	// Build set of output registers
	mOutputRegister.clear();
	for ( uint32_t i : mIndices )
	{
		mOutputRegister.push_back( mInputRegister[i] );
	}
}

/// <summary>
/// Produces the next tuple in register
/// </summary>
/// <returns></returns>
bool ProjectionOperator::Next()
{
	return mInput.Next();
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> ProjectionOperator::GetOutput()
{
	return mOutputRegister;
}

/// <summary>
/// Closes this instance.
/// </summary>
void ProjectionOperator::Close()
{
	mInput.Close();
}

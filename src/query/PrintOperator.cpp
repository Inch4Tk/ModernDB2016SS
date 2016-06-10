#include "PrintOperator.h"

#include "Register.h"

PrintOperator::PrintOperator( QueryOperator& input, std::ostream& outstream ) : mInput(input), mOutstream(outstream)
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
	mInput.Open();
	mInputRegister = mInput.GetOutput();
}

/// <summary>
/// Produces the next tuple in register of GetOutput and also prints out the tuple
/// </summary>
/// <returns></returns>
bool PrintOperator::Next()
{
	if ( mInput.Next() )
	{
		// Passthrough output register
		for( Register* r : mInputRegister )
		{
			if( r->GetType() == SchemaTypes::Tag::Char )
			{
				mOutstream << r->GetString();
			}
			else if( r->GetType() == SchemaTypes::Tag::Integer )
			{
				mOutstream << r->GetInteger();
			}
			if (mInputRegister.back() != r)
			{
				mOutstream << ", ";
			}
		}
		mOutstream << std::endl;
		return true;
	}
	return false;
}

/// <summary>
/// Gets the output.
/// </summary>
/// <returns></returns>
std::vector<Register*> PrintOperator::GetOutput()
{
	return mInputRegister;
}

/// <summary>
/// Closes this instance.
/// </summary>
void PrintOperator::Close()
{
	mInput.Close();
}

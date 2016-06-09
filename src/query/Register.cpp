#include "Register.h"

/// <summary>
/// Gets the name of the attribute this register contains;
/// </summary>
/// <returns></returns>
std::string Register::GetAttributeName()
{
	return mAttrName;
}

/// <summary>
/// Gets the integer.
/// </summary>
/// <returns></returns>
Integer Register::GetInteger()
{
	if (this->mType == SchemaTypes::Tag::Char)
	{
		return INT_MIN;
	}
	return mIntVar;
}

/// <summary>
/// Gets the string.
/// </summary>
/// <returns></returns>
std::string Register::GetString()
{
	if( this->mType == SchemaTypes::Tag::Integer )
	{
		return "";
	}
	return mStringVar;
}

/// <summary>
/// Gets the type.
/// </summary>
/// <returns></returns>
SchemaTypes::Tag Register::GetType()
{
	return mType;
}

bool Register::operator!=( const Register& other ) const
{
	return !(*this == other);
}

bool Register::operator==( const Register& other ) const
{
	if ( this->mType == other.mType )
	{
		if( this->mType == SchemaTypes::Tag::Char )
		{
			return this->mStringVar == other.mStringVar;
		}
		else if( this->mType == SchemaTypes::Tag::Integer )
		{
			return this->mIntVar == other.mIntVar;
		}
	}
	return false;
}

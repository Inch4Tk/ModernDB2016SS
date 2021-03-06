#include "SchemaParser.h"

#include <iostream>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <cstdlib>

namespace keyword {
	const std::string Primary = "primary";
	const std::string Key = "key";
	const std::string Create = "create";
	const std::string Table = "table";
	const std::string Integer = "integer";
	const std::string Numeric = "numeric";
	const std::string Not = "not";
	const std::string Null = "null";
	const std::string Char = "char";
}

namespace literal {
	const char ParenthesisLeft = '(';
	const char ParenthesisRight = ')';
	const char Comma = ',';
	const char Semicolon = ';';
}

std::unique_ptr<Schema> SchemaParser::parse()
{
	std::string token;
	unsigned line = 1;
	std::unique_ptr<Schema> s( new Schema() );

	if (fromSqlString)
	{
		std::stringstream sstream( sqlOrFile );
		while ( sstream >> token )
		{
			std::string::size_type pos;
			std::string::size_type prevPos = 0;

			while ( (pos = token.find_first_of( ",)(;", prevPos )) != std::string::npos )
			{
				nextToken( line, token.substr( prevPos, pos - prevPos ), *s );
				nextToken( line, token.substr( pos, 1 ), *s );
				prevPos = pos + 1;
			}
			nextToken( line, token.substr( prevPos ), *s );
			if ( token.find( "\n" ) != std::string::npos )
				++line;
		}
	}
	else
	{
		std::ifstream infile;
		infile.open( sqlOrFile.c_str() );
		if ( !infile.is_open() )
			throw SchemaParserError( line, "cannot open file '" + sqlOrFile + "'" );
		while ( infile >> token )
		{
			std::string::size_type pos;
			std::string::size_type prevPos = 0;

			while ( (pos = token.find_first_of( ",)(;", prevPos )) != std::string::npos )
			{
				nextToken( line, token.substr( prevPos, pos - prevPos ), *s );
				nextToken( line, token.substr( pos, 1 ), *s );
				prevPos = pos + 1;
			}
			nextToken( line, token.substr( prevPos ), *s );
			if ( token.find( "\n" ) != std::string::npos )
				++line;
		}
		infile.close();
	}
	
	return std::move( s );
}

static bool isIdentifier( const std::string& str )
{
	if (
		str == keyword::Primary ||
		str == keyword::Key ||
		str == keyword::Table ||
		str == keyword::Create ||
		str == keyword::Integer ||
		str == keyword::Numeric ||
		str == keyword::Not ||
		str == keyword::Null ||
		str == keyword::Char
		)
		return false;
	return str.find_first_not_of( "abcdefghijklmnopqrstuvwxyz_1234567890" ) == std::string::npos;
}

static bool isInt( const std::string& str )
{
	return str.find_first_not_of( "0123456789" ) == std::string::npos;
}

void SchemaParser::nextToken( unsigned line, const std::string& token, Schema& schema )
{
	if ( token.empty() )
		return;
	std::string tok;
	std::transform( token.begin(), token.end(), std::back_inserter( tok ), tolower );
	switch ( state )
	{
		case State::Semicolon: /* fallthrough */
		case State::Init:
			if ( tok == keyword::Create )
				state = State::Create;
			else
				throw SchemaParserError( line, "Expected 'CREATE', found '" + token + "'" );
			break;
		case State::Create:
			if ( tok == keyword::Table )
				state = State::Table;
			else
				throw SchemaParserError( line, "Expected 'TABLE', found '" + token + "'" );
			break;
		case State::Table:
			if ( isIdentifier( tok ) )
			{
				state = State::TableName;
				schema.relations.push_back( Schema::Relation( token ) );
			}
			else
			{
				throw SchemaParserError( line, "Expected TableName, found '" + token + "'" );
			}
			break;
		case State::TableName:
			if ( tok.size() == 1 && tok[0] == literal::ParenthesisLeft )
				state = State::CreateTableBegin;
			else
				throw SchemaParserError( line, "Expected '(', found '" + token + "'" );
			break;
		case State::Separator: /* fallthrough */
		case State::CreateTableBegin:
			if ( tok.size() == 1 && tok[0] == literal::ParenthesisRight )
			{
				state = State::CreateTableEnd;
			}
			else if ( tok == keyword::Primary )
			{
				state = State::Primary;
			}
			else if ( isIdentifier( tok ) )
			{
				schema.relations.back().attributes.push_back( Schema::Relation::Attribute() );
				schema.relations.back().attributes.back().name = token;
				state = State::AttributeName;
			}
			else
			{
				throw SchemaParserError( line, "Expected attribute definition, primary key definition or ')', found '" + token + "'" );
			}
			break;
		case State::CreateTableEnd:
			if ( tok.size() == 1 && tok[0] == literal::Semicolon )
				state = State::Semicolon;
			else
				throw SchemaParserError( line, "Expected ';', found '" + token + "'" );
			break;
		case State::Primary:
			if ( tok == keyword::Key )
				state = State::Key;
			else
				throw SchemaParserError( line, "Expected 'KEY', found '" + token + "'" );
			break;
		case State::Key:
			if ( tok.size() == 1 && tok[0] == literal::ParenthesisLeft )
				state = State::KeyListBegin;
			else
				throw SchemaParserError( line, "Expected list of key attributes, found '" + token + "'" );
			break;
		case State::KeyListBegin:
			if ( isIdentifier( tok ) )
			{
				struct AttributeNamePredicate
				{
					const std::string& name;
					AttributeNamePredicate( const std::string& name ) : name( name )
					{
					}
					bool operator()( const Schema::Relation::Attribute& attr ) const
					{
						return attr.name == name;
					}
				};
				const auto& attributes = schema.relations.back().attributes;
				AttributeNamePredicate p( token );
				auto it = std::find_if( attributes.begin(), attributes.end(), p );
				if ( it == attributes.end() )
					throw SchemaParserError( line, "'" + token + "' is not an attribute of '" + schema.relations.back().name + "'" );
				schema.relations.back().indices.push_back( Schema::Relation::Index() ); // Autocreate index for primary keys
				schema.relations.back().indices.back().attrName = it->name;
				schema.relations.back().primaryKey.push_back( static_cast<unsigned int>(std::distance( attributes.begin(), it )) );
				state = State::KeyName;
			}
			else
			{
				throw SchemaParserError( line, "Expected key attribute, found '" + token + "'" );
			}
			break;
		case State::KeyName:
			if ( tok.size() == 1 && tok[0] == literal::Comma )
				state = State::KeyListBegin;
			else if ( tok.size() == 1 && tok[0] == literal::ParenthesisRight )
				state = State::KeyListEnd;
			else
				throw SchemaParserError( line, "Expected ',' or ')', found '" + token + "'" );
			break;
		case State::KeyListEnd:
			if ( tok.size() == 1 && tok[0] == literal::Comma )
				state = State::Separator;
			else if ( tok.size() == 1 && tok[0] == literal::ParenthesisRight )
				state = State::CreateTableEnd;
			else
				throw SchemaParserError( line, "Expected ',' or ')', found '" + token + "'" );
			break;
		case State::AttributeName:
			if ( tok == keyword::Integer )
			{
				schema.relations.back().attributes.back().type = SchemaTypes::Tag::Integer;
				state = State::AttributeTypeInt;
			}
			else if ( tok == keyword::Char )
			{
				schema.relations.back().attributes.back().type = SchemaTypes::Tag::Char;
				state = State::AttributeTypeChar;
			}
			else if ( tok == keyword::Numeric )
			{
				//schema.relations.back().attributes.back().type=Types::Tag::Numeric;
				state = State::AttributeTypeNumeric;
			}
			else throw SchemaParserError( line, "Expected type after attribute name, found '" + token + "'" );
			break;
		case State::AttributeTypeChar:
			if ( tok.size() == 1 && tok[0] == literal::ParenthesisLeft )
				state = State::CharBegin;
			else
				throw SchemaParserError( line, "Expected '(' after 'CHAR', found'" + token + "'" );
			break;
		case State::CharBegin:
			if ( isInt( tok ) )
			{
				schema.relations.back().attributes.back().len = std::atoi( tok.c_str() );
				state = State::CharValue;
			}
			else
			{
				throw SchemaParserError( line, "Expected integer after 'CHAR(', found'" + token + "'" );
			}
			break;
		case State::CharValue:
			if ( tok.size() == 1 && tok[0] == literal::ParenthesisRight )
				state = State::CharEnd;
			else
				throw SchemaParserError( line, "Expected ')' after length of CHAR, found'" + token + "'" );
			break;
		case State::AttributeTypeNumeric:
			if ( tok.size() == 1 && tok[0] == literal::ParenthesisLeft )
				state = State::NumericBegin;
			else
				throw SchemaParserError( line, "Expected '(' after 'NUMERIC', found'" + token + "'" );
			break;
		case State::NumericBegin:
			if ( isInt( tok ) )
			{
				//schema.relations.back().attributes.back().len1=std::atoi(tok.c_str());
				state = State::NumericValue1;
			}
			else
			{
				throw SchemaParserError( line, "Expected integer after 'NUMERIC(', found'" + token + "'" );
			}
			break;
		case State::NumericValue1:
			if ( tok.size() == 1 && tok[0] == literal::Comma )
				state = State::NumericSeparator;
			else
				throw SchemaParserError( line, "Expected ',' after first length of NUMERIC, found'" + token + "'" );
			break;
		case State::NumericValue2:
			if ( tok.size() == 1 && tok[0] == literal::ParenthesisRight )
				state = State::NumericEnd;
			else
				throw SchemaParserError( line, "Expected ')' after second length of NUMERIC, found'" + token + "'" );
			break;
		case State::NumericSeparator:
			if ( isInt( tok ) )
			{
				//schema.relations.back().attributes.back().len2=std::atoi(tok.c_str());
				state = State::NumericValue2;
			}
			else
			{
				throw SchemaParserError( line, "Expected second length for NUMERIC type, found'" + token + "'" );
			}
			break;
		case State::CharEnd: /* fallthrough */
		case State::NumericEnd: /* fallthrough */
		case State::AttributeTypeInt:
			if ( tok.size() == 1 && tok[0] == literal::Comma )
				state = State::Separator;
			else if ( tok == keyword::Not )
				state = State::Not;
			else if ( tok.size() == 1 && tok[0] == literal::ParenthesisRight )
				state = State::CreateTableEnd;
			else throw SchemaParserError( line, "Expected ',' or 'NOT NULL' after attribute type, found '" + token + "'" );
			break;
		case State::Not:
			if ( tok == keyword::Null )
			{
				schema.relations.back().attributes.back().notNull = true;
				state = State::Null;
			}
			else throw SchemaParserError( line, "Expected 'NULL' after 'NOT' name, found '" + token + "'" );
			break;
		case State::Null:
			if ( tok.size() == 1 && tok[0] == literal::Comma )
				state = State::Separator;
			else if ( tok.size() == 1 && tok[0] == literal::ParenthesisRight )
				state = State::CreateTableEnd;
			else throw SchemaParserError( line, "Expected ',' or ')' after attribute definition, found '" + token + "'" );
			break;
		default:
			throw;
	}
}

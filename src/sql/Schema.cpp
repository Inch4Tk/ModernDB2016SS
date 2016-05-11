#include "Schema.h"

#include "utility/helpers.h"

#include <algorithm>
#include <sstream>
#include <set>
#include <unordered_map>
#include <cassert>

static std::string type(const Schema::Relation::Attribute& attr) {
   SchemaTypes::Tag type = attr.type;
   switch(type) {
      case SchemaTypes::Tag::Integer:
         return "Integer";
      /*case Types::Tag::Numeric: {
         std::stringstream ss;
         ss << "Numeric(" << attr.len1 << ", " << attr.len2 << ")";
         return ss.str();
      }*/
      case SchemaTypes::Tag::Char: {
         std::stringstream ss;
         ss << "Char(" << attr.len << ")";
         return ss.str();
      }
   }
   throw;
}

std::string Schema::toString() const {
   std::stringstream out;
   for (const Schema::Relation& rel : relations) {
      out << rel.name << std::endl;
	  out << "\tSegmentId:" << rel.segmentId;
	  out << "\tPagecount:" << rel.pagecount;
      out << "\tPrimary Key:";
      for (unsigned keyId : rel.primaryKey)
         out << ' ' << rel.attributes[keyId].name;
      out << std::endl;
      for (const auto& attr : rel.attributes)
         out << '\t' << attr.name << '\t' << type(attr) << (attr.notNull ? " not null" : "") << std::endl;
   }
   return out.str();
}


/// <summary>
/// Serializes the schema into the data.
/// </summary>
/// <param name="data">The data.</param>
/// <returns>Success</returns>
void Schema::Serialize( std::vector<uint8_t>& data )
{
	AppendToData( relations.size(), data );
	for (Relation& r : relations)
	{
		SerializeRelation( r, data );
	}
}

/// <summary>
/// Serializes a relation. Will append to data.
/// </summary>
/// <param name="data">The data.</param>
/// <returns></returns>
void Schema::SerializeRelation( Relation& r, std::vector<uint8_t>& data )
{
	AppendToData( r.segmentId, data );
	AppendToData( r.pagecount, data );
	AppendToData( r.name, data );
	AppendToData( r.attributes.size(), data );
	for ( Relation::Attribute& a : r.attributes )
	{
		SerializeAttribute( a, data );
	}
	AppendToData( r.primaryKey.size(), data );
	for ( uint32_t pk : r.primaryKey )
	{
		AppendToData( pk, data );
	}
}

/// <summary>
/// Serializes a attribute. Will append to data.
/// </summary>
/// <param name="data">The output data.</param>
/// <returns></returns>
void Schema::SerializeAttribute( Relation::Attribute& a, std::vector<uint8_t>& data )
{
	AppendToData( a.name, data );
	AppendToData( static_cast<uint32_t>(a.type), data );
	AppendToData( a.len, data );
	AppendToData( a.notNull, data );
}

/// <summary>
/// Appends to data.
/// </summary>
/// <param name="toappend">The item to append</param>
/// <param name="data">The data.</param>
void Schema::AppendToData( uint16_t toappend, std::vector<uint8_t>& data )
{
	uint8_t* ptr = reinterpret_cast<uint8_t*>(&toappend);
	data.push_back( ptr[0] );
	data.push_back( ptr[1] );
}

/// <summary>
/// Appends to data.
/// </summary>
/// <param name="toappend">The item to append</param>
/// <param name="data">The data.</param>
void Schema::AppendToData( uint32_t toappend, std::vector<uint8_t>& data )
{
	uint8_t* ptr = reinterpret_cast<uint8_t*>(&toappend);
	data.push_back( ptr[0] );
	data.push_back( ptr[1] );
	data.push_back( ptr[2] );
	data.push_back( ptr[3] );
}

/// <summary>
/// Appends to data.
/// </summary>
/// <param name="toappend">The item to append</param>
/// <param name="data">The data.</param>
void Schema::AppendToData( uint64_t toappend, std::vector<uint8_t>& data )
{
	uint8_t* ptr = reinterpret_cast<uint8_t*>(&toappend);
	data.push_back( ptr[0] );
	data.push_back( ptr[1] );
	data.push_back( ptr[2] );
	data.push_back( ptr[3] );
	data.push_back( ptr[4] );
	data.push_back( ptr[5] );
	data.push_back( ptr[6] );
	data.push_back( ptr[7] );
}


/// <summary>
/// Appends to data.
/// </summary>
/// <param name="toappend">The item to append</param>
/// <param name="data">The data.</param>
void Schema::AppendToData( const std::string& toappend, std::vector<uint8_t>& data )
{
	AppendToData( toappend.size(), data );
	const char* ptr = toappend.c_str();
	for ( uint32_t i = 0; i < toappend.size(); ++i )
	{
		data.push_back( ptr[i] );
	}
}

/// <summary>
/// Appends to data.
/// </summary>
/// <param name="toappend">The item to append</param>
/// <param name="data">The data.</param>
void Schema::AppendToData( bool toappend, std::vector<uint8_t>& data )
{
	data.push_back( toappend ? static_cast<uint8_t>(1u) : static_cast<uint8_t>(0u) );
}

/// <summary>
/// Deserializes the specified data into the schema.
/// </summary>
/// <param name="data">The data.</param>
/// <returns>Success</returns>
void Schema::Deserialize( const std::vector<uint8_t>& data )
{

}

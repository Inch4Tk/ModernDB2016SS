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
/// Merges the other schema to this schema. This can be used to add new schemas to the masterschema. (e.g. Insert new relations)
/// </summary>
/// <param name="other">The other.</param>
void Schema::MergeSchema( Schema& other )
{
	// TODO: locking
	// Build two sets, one for names one for segments
	std::set<std::string> names;
	std::set<uint64_t> usedSegments;
	for ( Relation& r : relations )
	{
		auto ins = names.insert( r.name );
		assert( ins.second ); // Make sure all used names are unique
		auto ins2 = usedSegments.insert( r.segmentId );
		assert( ins2.second ); // Make sure all used segments are unique
	}

	// Go over other schema and insert every relation, give out new segment id
	// make sure to only insert relations with new names.
	uint32_t curUnused = 0;
	for ( Relation& ro : other.relations )
	{
		// Check name
		if ( names.find( ro.name ) != names.end() )
		{
			LogError( "Schema, Relation with name: " + ro.name + " already exists, skipping." );
			continue;
		}
		// Go to next unused segment
		while ( usedSegments.find( curUnused ) != usedSegments.end() )
		{
			++curUnused;
		}
		// Insert into used names/segments
		auto ins = names.insert( ro.name );
		assert( ins.second ); // Make sure all used names are unique
		auto ins2 = usedSegments.insert( curUnused );
		assert( ins2.second ); // Make sure all used segments are unique
							  // Do the actual merge
		ro.segmentId = curUnused;
		relations.push_back( ro );
	}
}

/// <summary>
/// Serializes the schema into the data.
/// </summary>
/// <param name="data">The data.</param>
/// <returns>Success</returns>
void Schema::Serialize( std::vector<uint8_t>& data )
{
	AppendToData( static_cast<uint32_t>(relations.size()), data );
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
	AppendToData( static_cast<uint32_t>(r.attributes.size()), data );
	for ( Relation::Attribute& a : r.attributes )
	{
		SerializeAttribute( a, data );
	}
	AppendToData( static_cast<uint32_t>(r.primaryKey.size()), data );
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
	AppendToData( static_cast<uint32_t>(toappend.size()), data );
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
/// Deserializes the specified data into the schema. Discards all other data in the schema.
/// </summary>
/// <param name="data">The data.</param>
/// <returns>Success</returns>
void Schema::Deserialize( const std::vector<uint8_t>& data )
{
	// Make sure schema is empty
	relations.clear();
	// Read relations
	uint32_t numRelations = 0;
	const uint8_t* dataptr = &data[0];
	ReadFromData( numRelations, dataptr );
	for ( uint32_t i = 0; i < numRelations; ++i )
	{
		DeserializeRelation( dataptr );
	}
}

/// <summary>
/// Deserializes the next relation (starting at next data). Appends the relation to schema.
/// </summary>
/// <param name="data">The data.</param>
void Schema::DeserializeRelation( const uint8_t* data )
{
	relations.push_back( Relation("") );
	Relation& r = relations.back();
	ReadFromData( r.segmentId, data );
	ReadFromData( r.pagecount, data );
	ReadFromData( r.name, data );
	// Read attributes
	uint32_t numAttributes = 0;
	ReadFromData( numAttributes, data );
	for ( uint32_t i = 0; i < numAttributes; ++i )
	{
		DeserializeAttribute( r, data );
	}
}

/// <summary>
/// Deserializes the next attribute (starting at next data). Appends attribute to relation.
/// </summary>
/// <param name="r">The r.</param>
/// <param name="data">The data.</param>
void Schema::DeserializeAttribute( Relation& r, const uint8_t* data )
{
	r.attributes.push_back( Relation::Attribute() );
	Relation::Attribute& a = r.attributes.back();
	ReadFromData( a.name, data );
	uint32_t tag = 0;
	ReadFromData( tag, data );
	a.type = static_cast<SchemaTypes::Tag>(tag);
	ReadFromData( a.len, data );
	ReadFromData( a.notNull, data );
}

/// <summary>
/// Reads from data.
/// </summary>
/// <param name="toread">The toread.</param>
/// <param name="data">The data.</param>
void Schema::ReadFromData( bool& toread, const uint8_t* data )
{
	toread = (*data == 0) ? false : true;
	++data;
}

/// <summary>
/// Reads from data.
/// </summary>
/// <param name="toread">The toread.</param>
/// <param name="data">The data.</param>
void Schema::ReadFromData( uint16_t& toread, const uint8_t* data )
{
	toread = *reinterpret_cast<const uint16_t*>(data);
	data += 2;
}

/// <summary>
/// Reads from data.
/// </summary>
/// <param name="toread">The toread.</param>
/// <param name="data">The data.</param>
void Schema::ReadFromData( uint32_t& toread, const uint8_t* data )
{
	toread = *reinterpret_cast<const uint32_t*>(data);
	data += 4;
}

/// <summary>
/// Reads from data.
/// </summary>
/// <param name="toread">The toread.</param>
/// <param name="data">The data.</param>
void Schema::ReadFromData( uint64_t& toread, const uint8_t* data )
{
	toread = *reinterpret_cast<const uint64_t*>(data);
	data += 8;
}

/// <summary>
/// Reads from data.
/// </summary>
/// <param name="toread">The toread.</param>
/// <param name="data">The data.</param>
void Schema::ReadFromData( std::string& toread, const uint8_t* data )
{
	uint32_t strlen = 0;
	ReadFromData( strlen, data );
	toread = std::string( reinterpret_cast<const char*>(data), strlen );
	data += strlen;
}

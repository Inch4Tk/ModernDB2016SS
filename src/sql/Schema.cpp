#include "Schema.h"

#include <sstream>

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
bool Schema::Serialize( std::vector<uint8_t>& data )
{

}

/// <summary>
/// Deserializes the specified data into the schema.
/// </summary>
/// <param name="data">The data.</param>
/// <returns>Success</returns>
bool Schema::Deserialize( const std::vector<uint8_t>& data )
{

}

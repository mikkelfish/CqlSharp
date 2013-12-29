// CqlSharp - CqlSharp
// Copyright (c) 2013 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Numerics;

namespace CqlSharp
{
    public enum CqlType
    {
        Custom = 0x0000,

        Ascii = 0x0001,

        Bigint = 0x0002,

        Blob = 0x0003,

        Boolean = 0x0004,

        Counter = 0x0005,

        Decimal = 0x0006,

        Double = 0x0007,

        Float = 0x0008,

        Int = 0x0009,

        Text = 0x000A,

        Timestamp = 0x000B,

        Uuid = 0x000C,

        Varchar = 0x000D,

        Varint = 0x000E,

        Timeuuid = 0x000F,

        Inet = 0x0010,

        List = 0x0020,

        Map = 0x0021,

        Set = 0x0022
    }

    internal static class CqlTypeExtensions
    {
        private static readonly Dictionary<CqlType, Type> CqlType2Type = new Dictionary<CqlType, Type>
                                                                             {
                                                                                 {CqlType.Ascii, typeof (string)},
                                                                                 {CqlType.Text, typeof (string)},
                                                                                 {CqlType.Varchar, typeof (string)},
                                                                                 {CqlType.Blob, typeof (byte[])},
                                                                                 {CqlType.Double, typeof (double)},
                                                                                 {CqlType.Float, typeof (float)},
                                                                                 {CqlType.Bigint, typeof (long)},
                                                                                 {CqlType.Counter, typeof (long)},
                                                                                 {CqlType.Int, typeof (int)},
                                                                                 {CqlType.Boolean, typeof (bool)},
                                                                                 {CqlType.Uuid, typeof (Guid)},
                                                                                 {CqlType.Timeuuid, typeof (Guid)},
                                                                                 {CqlType.Inet, typeof (IPAddress)},
                                                                                 {CqlType.Varint, typeof (BigInteger)},
                                                                                 {CqlType.Timestamp, typeof (DateTime)}
                                                                             };

        private static readonly Dictionary<Type, CqlType> Type2CqlType = new Dictionary<Type, CqlType>
                                                                             {
                                                                                 {typeof (string), CqlType.Varchar},
                                                                                 {typeof (byte[]), CqlType.Blob},
                                                                                 {typeof (double), CqlType.Double},
                                                                                 {typeof (float), CqlType.Float},
                                                                                 {typeof (long), CqlType.Bigint},
                                                                                 {typeof (int), CqlType.Int},
                                                                                 {typeof (bool), CqlType.Boolean},
                                                                                 {typeof (Guid), CqlType.Uuid},
                                                                                 {typeof (IPAddress), CqlType.Inet},
                                                                                 {typeof (BigInteger), CqlType.Varint},
                                                                                 {typeof (DateTime), CqlType.Timestamp}
                                                                             };

        private static readonly Dictionary<CqlType, DbType> CqlType2DbType = new Dictionary<CqlType, DbType>
                                                                                 {
                                                                                     {CqlType.Ascii, DbType.AnsiString},
                                                                                     {CqlType.Text, DbType.String},
                                                                                     {CqlType.Varchar, DbType.String},
                                                                                     {CqlType.Blob, DbType.Binary},
                                                                                     {CqlType.Double, DbType.Double},
                                                                                     {CqlType.Float, DbType.Single},
                                                                                     {CqlType.Bigint, DbType.Int64},
                                                                                     {CqlType.Counter, DbType.Int64},
                                                                                     {CqlType.Int, DbType.Int32},
                                                                                     {CqlType.Boolean, DbType.Boolean},
                                                                                     {CqlType.Uuid, DbType.Guid},
                                                                                     {CqlType.Timeuuid, DbType.Guid},
                                                                                     {CqlType.Varint, DbType.VarNumeric},
                                                                                     {
                                                                                         CqlType.Timestamp, DbType.DateTime
                                                                                     }
                                                                                 };

        private static readonly Dictionary<DbType, CqlType> DbType2CqlType = new Dictionary<DbType, CqlType>
                                                                                 {
                                                                                     {DbType.AnsiString, CqlType.Ascii},
                                                                                     {DbType.Int64, CqlType.Bigint},
                                                                                     {DbType.Guid, CqlType.Uuid},
                                                                                     {DbType.Binary, CqlType.Blob},
                                                                                     {
                                                                                         DbType.DateTime, CqlType.Timestamp
                                                                                     },
                                                                                     {DbType.Single, CqlType.Float},
                                                                                     {DbType.Double, CqlType.Double},
                                                                                     {DbType.Int32, CqlType.Int},
                                                                                     {DbType.Boolean, CqlType.Boolean},
                                                                                     {DbType.VarNumeric, CqlType.Varint},
                                                                                     {DbType.String, CqlType.Varchar},
                                                                                 };

        

        /// <summary>
        ///   Gets the .Net type that represents the given CqlType
        /// </summary>
        /// <param name="cqlType"> Type of the CQL. </param>
        /// <param name="valueType"> Type of the values if the CqlType is a collection. </param>
        /// <param name="keyType"> Type of the key if the type is a map. </param>
        /// <returns> .NET type representing the CqlType </returns>
        /// <exception cref="System.ArgumentException">Unsupported type</exception>
        public static Type ToType(this CqlType cqlType, CqlType? keyType = null, CqlType? valueType = null)
        {
            Type type;
            switch (cqlType)
            {
                case CqlType.Map:
                    Type genericMapType = typeof (Dictionary<,>);

                    Debug.Assert(keyType.HasValue, "a map should have a Key type");
                    Debug.Assert(valueType.HasValue, "a map should have a Value type");

                    type = genericMapType.MakeGenericType(keyType.Value.ToType(),
                                                          valueType.Value.ToType());
                    break;

                case CqlType.Set:
                    Type genericSetType = typeof (HashSet<>);
                    Debug.Assert(valueType.HasValue, "a set should have a Value type");

                    type = genericSetType.MakeGenericType(valueType.Value.ToType());
                    break;

                case CqlType.List:
                    Type genericListType = typeof (List<>);
                    Debug.Assert(valueType.HasValue, "a list should have a Value type");

                    type = genericListType.MakeGenericType(valueType.Value.ToType());
                    break;

                default:
                    if (!CqlType2Type.TryGetValue(cqlType, out type))
                        throw new ArgumentException("Unsupported type");
                    break;
            }

            return type;
        }

        /// <summary>
        ///   gets the corresponding the CqlType.
        /// </summary>
        /// <param name="type"> The type. </param>
        /// <returns> </returns>
        /// <exception cref="System.NotSupportedException">Type +type.Name+ is not supported for deserialization</exception>
        internal static CqlType ToCqlType(this Type type)
        {
            CqlType cqlType;

            if (!Type2CqlType.TryGetValue(type, out cqlType))
                throw new NotSupportedException("Type " + type.Name + " is not supported for deserialization");

            return cqlType;
        }

        /// <summary>
        ///   gets the corresponding the DbType
        /// </summary>
        /// <param name="colType"> Type of the col. </param>
        /// <returns> </returns>
        public static DbType ToDbType(this CqlType colType)
        {
            DbType type;

            if (CqlType2DbType.TryGetValue(colType, out type))
            {
                return type;
            }


            return DbType.Object;
        }

        /// <summary>
        ///   gets the corresponding the CqlType
        /// </summary>
        /// <param name="colType"> Type of the col. </param>
        /// <returns> </returns>
        /// <exception cref="System.ArgumentOutOfRangeException">cqlType;DbType is not supported</exception>
        public static CqlType ToCqlType(this DbType colType)
        {
            CqlType type;

            if (DbType2CqlType.TryGetValue(colType, out type))
            {
                return type;
            }

            throw new ArgumentOutOfRangeException("colType", colType, "DbType is not supported");
        }

        private static bool implementsInterface(Type genericType, Type genericInterface)
        {
            if (genericType.IsInterface && genericType.IsGenericType &&
                genericType.GetGenericTypeDefinition() == genericInterface)
                return true;

            foreach (var i in genericType.GetInterfaces())
                if (i.IsGenericType && i.GetGenericTypeDefinition() == genericInterface)
                    return true;

            return false;
        }

        /// <summary>
        /// Gets the corresponding column definition string for a type
        /// </summary>
        /// <param name="type">Type of the column</param>
        /// <returns></returns>
        public static string ToCqlColumnType(this Type type, bool mustBeSimple)
        {
            string cqlType;
            if (!type.IsGenericType)
                return type.ToCqlType().ToString().ToLower();
            
            if (mustBeSimple)
                throw new InvalidOperationException("The type is not valid for this column because it is a collection. Are you trying to set the type on a clustering or partition key?");

            if (implementsInterface(type, typeof(IList<>)))
            {
                cqlType = "list<" + type.GetGenericArguments()[0] + ">";
            }
            else if (implementsInterface(type, typeof(ISet<>)))
            {
                cqlType = "set<" + type.GetGenericArguments()[0] + ">";
            }
            else if (implementsInterface(type, typeof(IDictionary<,>)))
            {
                cqlType = "map<" + type.GetGenericArguments()[0] + "," + type.GetGenericArguments()[1] + ">";
            }
            else throw new InvalidOperationException("Do not know how to convert " + type.FullName + " to column type");
            return cqlType;

        }
    }
}
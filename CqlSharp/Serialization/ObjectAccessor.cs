﻿// CqlSharp - CqlSharp
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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using CqlSharp.Network.Partition;

namespace CqlSharp.Serialization
{
    /// <summary>
    ///   Provides access to object fields and properties based on columnn descriptions.
    /// </summary>
    /// <typeparam name="T"> </typeparam>
    internal class ObjectAccessor<T>
    {
        

        /// <summary>
        ///   Singleton instance
        /// </summary>
        public static readonly ObjectAccessor<T> Instance = new ObjectAccessor<T>();

        private readonly Func<T, object>[] _partitionKeyReadFuncs;
        private readonly CqlType[] _partitionKeyTypes;
        private readonly string[] _partitionKeyNames;
        private readonly string[] _clusteringKeyNames;

        /// <summary>
        ///   Read functions to used to read member or property values
        /// </summary>
        private readonly Dictionary<string, Func<T, object>> _readFuncs;

        /// <summary>
        ///   Write functions to use to set fields or property values.
        /// </summary>
        private readonly Dictionary<string, Action<T, object>> _writeFuncs;

        /// <summary>
        ///   Prevents a default instance of the <see cref="ObjectAccessor{T}" /> class from being created.
        /// </summary>
        private ObjectAccessor()
        {
            //init fields
            _writeFuncs = new Dictionary<string, Action<T, object>>();
            _readFuncs = new Dictionary<string, Func<T, object>>();

            var keyMembers = new List<Tuple<int, string, Func<T, object>, CqlType>>();
            var clusteringMembers = new List<Tuple<int, string>>();

            //set default keyspace and table name to empty strings (nothing)
            string keyspace = null;
            string table = null;

            //set default table name to class name if table is not anonymous
            Type type = typeof (T);
            IsTableSet = !type.IsAnonymous();
            if (IsTableSet)
                table = type.Name.ToLower();

            //check for CqlTable attribute
            var tableAttribute = Attribute.GetCustomAttribute(type, typeof (CqlTableAttribute)) as CqlTableAttribute;
            if (tableAttribute != null)
            {
                //overwrite keyspace if any
                IsKeySpaceSet = tableAttribute.Keyspace != null;
                if (IsKeySpaceSet)
                    keyspace = tableAttribute.Keyspace;

                //set default table name
                table = tableAttribute.Table ?? table;
            }

            this.TableName = table;
            this.Keyspace = keyspace;

            //go over all properties
            foreach (PropertyInfo prop in type.GetProperties())
            {
                //get the column name of the property
                string name = GetColumnName(prop);

                //check if we get a proper name
                if (string.IsNullOrEmpty(name))
                    continue;

                //add the read func if we can read the property
                if (prop.CanRead && !prop.GetMethod.IsPrivate)
                {
                    var getter = MakeGetterDelegate(prop);
                    AddReadFunc(getter, name, table, keyspace);
                    SetPartitionKeyMember(keyMembers, prop, getter);
                }

                //add write func if we can write the property
                if (prop.CanWrite && !prop.SetMethod.IsPrivate)
                {
                    var setter = MakeSetterDelegate(prop);
                    AddWriteFunc(setter, name, table, keyspace);
                }

                var clustering = GetClusteringKeyIndex(prop);
                if (clustering > 0)
                    clusteringMembers.Add(new Tuple<int, string>(clustering, name));
            }

            //go over all fields
            foreach (FieldInfo field in type.GetFields())
            {
                //get the column name of the field
                string name = GetColumnName(field);

                //check if we get a proper name
                if (string.IsNullOrEmpty(name))
                    continue;

                //set getter functions
                var getter = MakeFieldGetterDelegate(field);
                AddReadFunc(getter, name, table, keyspace);
                SetPartitionKeyMember(keyMembers, field, getter);

                //set setter functions if not readonly
                if (!field.IsInitOnly)
                {
                    var setter = MakeFieldSetterDelegate(field);
                    AddWriteFunc(setter, name, table, keyspace);
                }

                var clustering = GetClusteringKeyIndex(field);
                if (clustering > 0)
                    clusteringMembers.Add(new Tuple<int, string>(clustering, name));
            }

            //sort keyMembers on partitionIndex
            keyMembers.Sort((a, b) => a.Item1.CompareTo(b.Item1));
            _partitionKeyNames = keyMembers.Select(km => km.Item2).ToArray();
            _partitionKeyReadFuncs = keyMembers.Select(km => km.Item3).ToArray();
            _partitionKeyTypes = keyMembers.Select(km => km.Item4).ToArray();
            _clusteringKeyNames = clusteringMembers.OrderBy(cm => cm.Item1).Select(cm => cm.Item2).ToArray();
        }

        public bool IsKeySpaceSet { get; private set; }
        public bool IsTableSet { get; private set; }
        public string Keyspace { get; private set; }
        public string TableName { get; private set; }

        public string KeySpaceTableAndName
        {
            get
            {
                 return (Keyspace != null ? Keyspace + "." : string.Empty) + this.TableName;
            }
        }

        public string[] PartitionKeys 
        {
            get { return this._partitionKeyNames; }
        }

        public string[] ClusteringKeys
        {
            get { return this._clusteringKeyNames; }
        }

        private void AddWriteFunc(Action<T, object> setter, string column, string table, string keyspace)
        {
            _writeFuncs[column] = setter;
            if (table != null)
            {
                _writeFuncs[table + "." + column] = setter;
                if (keyspace != null)
                    _writeFuncs[keyspace + "." + table + "." + column] = setter;
            }
        }

        private void AddReadFunc(Func<T, object> getter, string column, string table, string keyspace)
        {
            _readFuncs[column] = getter;
            if (table != null)
            {
                _readFuncs[table + "." + column] = getter;
                if (keyspace != null)
                    _readFuncs[keyspace + "." + table + "." + column] = getter;
            }
        }

        /// <summary>
        ///   Sets the partition key member.
        /// </summary>
        /// <param name="keyMembers"> The key members. </param>
        /// <param name="member"> The member. </param>
        /// <param name="reader"> The reader. </param>
        /// <exception cref="System.ArgumentException">CqlType must be set on ColumnAttribute if PartitionKeyIndex is set.</exception>
        private void SetPartitionKeyMember(List<Tuple<int, string, Func<T, object>, CqlType>> keyMembers, MemberInfo member,
                                           Func<T, object> reader)
        {
            //check for column attribute
            var columnAttribute =
                Attribute.GetCustomAttribute(member, typeof (CqlColumnAttribute)) as CqlColumnAttribute;

            if (columnAttribute != null)
            {
                if (columnAttribute.PartitionKeyIndex >= 0)
                {
                    //if (!columnAttribute.CqlType.HasValue)
                    //    throw new ArgumentException("CqlType must be set on ColumnAttribute if PartitionKeyIndex is set.");

                    //add the member
                    keyMembers.Add(new Tuple<int, string, Func<T, object>, CqlType>(columnAttribute.PartitionKeyIndex, GetColumnName(member), reader, columnAttribute.CqlType));
                }
            }
        }

        /// <summary>
        ///   Gets the name of the column of the specified member.
        /// </summary>
        /// <param name="member"> The member. </param>
        /// <returns> </returns>
        private static string GetColumnName(MemberInfo member)
        {
            //check for ignore attribute
            var ignoreAttribute =
                Attribute.GetCustomAttribute(member, typeof (CqlIgnoreAttribute)) as CqlIgnoreAttribute;

            //return null if ignore attribute is set
            if (ignoreAttribute != null)
                return null;

            //check for column attribute
            var columnAttribute =
                Attribute.GetCustomAttribute(member, typeof (CqlColumnAttribute)) as CqlColumnAttribute;

            return columnAttribute != null ? columnAttribute.Column : member.Name.ToLower();
        }

        private static int GetClusteringKeyIndex(MemberInfo member)
        {
            //check for column attribute
            var columnAttribute =
                Attribute.GetCustomAttribute(member, typeof(CqlColumnAttribute)) as CqlColumnAttribute;

            return columnAttribute != null ? columnAttribute.ClusteringKeyIndex : -1;
        }

        /// <summary>
        ///   Tries to get a value from the source, based on the column description
        /// </summary>
        /// <param name="column"> The column. </param>
        /// <param name="source"> The source. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true, if the value could be distilled from the source </returns>
        /// <exception cref="System.ArgumentNullException">column</exception>
        /// <exception cref="System.ArgumentException">Source is not of the correct type!;source</exception>
        public bool TryGetValue(string column, T source, out object value)
        {
            if (column == null)
                throw new ArgumentNullException("column");

            // ReSharper disable CompareNonConstrainedGenericWithNull
            if (source == null)
                // ReSharper restore CompareNonConstrainedGenericWithNull
                throw new ArgumentNullException("source");

            Func<T, object> func;

            if (_readFuncs.TryGetValue(column, out func))
            {
                value = func(source);
                return true;
            }

            value = null;
            return false;
        }

        /// <summary>
        ///   Tries to set a property or field of the specified object, based on the column description
        /// </summary>
        /// <param name="column"> The column name. </param>
        /// <param name="target"> The target. </param>
        /// <param name="value"> The value. </param>
        /// <returns> true if the property or field value is set </returns>
        /// <exception cref="System.ArgumentNullException">column or target are null</exception>
        public bool TrySetValue(string column, T target, object value)
        {
            if (column == null)
                throw new ArgumentNullException("column");

            // ReSharper disable CompareNonConstrainedGenericWithNull
            if (target == null)
                // ReSharper restore CompareNonConstrainedGenericWithNull
                throw new ArgumentNullException("target");


            Action<T, object> func;

            if (_writeFuncs.TryGetValue(column, out func))
            {
                func(target, value);
                return true;
            }

            return false;
        }

        public void SetPartitionKey(PartitionKey key, T value)
        {
            int length = _partitionKeyReadFuncs.Length;
            if (length > 0)
            {
                var values = new object[length];
                for (int i = 0; i < length; i++)
                {
                    values[i] = _partitionKeyReadFuncs[i](value);
                }

                key.Set(_partitionKeyTypes, values);
            }
        }


        private static Func<T, object> MakeGetterDelegate(PropertyInfo property)
        {
            MethodInfo getMethod = property.GetGetMethod();
            var target = Expression.Parameter(typeof (T));
            var body = Expression.Convert(Expression.Call(target, getMethod), typeof (object));
            return Expression.Lambda<Func<T, object>>(body, target)
                .Compile();
        }

        private static Action<T, object> MakeSetterDelegate(PropertyInfo property)
        {
            MethodInfo setMethod = property.GetSetMethod();
            var target = Expression.Parameter(typeof (T));
            var value = Expression.Parameter(typeof (object));
            var valueOrDefault = Expression.Condition(
                Expression.Equal(value, Expression.Constant(null)),
                Expression.Default(property.PropertyType),
                Expression.Convert(value, property.PropertyType));
            var body = Expression.Call(target, setMethod, valueOrDefault);
            return Expression.Lambda<Action<T, object>>(body, target, value)
                .Compile();
        }

        private static Func<T, object> MakeFieldGetterDelegate(FieldInfo property)
        {
            var target = Expression.Parameter(typeof (T));
            var body = Expression.Convert(Expression.Field(target, property), typeof (object));
            return Expression.Lambda<Func<T, object>>(body, target).Compile();
        }

        private static Action<T, object> MakeFieldSetterDelegate(FieldInfo property)
        {
            var target = Expression.Parameter(typeof (T));
            var field = Expression.Field(target, property);
            var value = Expression.Parameter(typeof (object));
            var valueOrDefault = Expression.Condition(
                Expression.Equal(value, Expression.Constant(null)),
                Expression.Default(property.FieldType),
                Expression.Convert(value, property.FieldType));
            var body = Expression.Assign(field, valueOrDefault);
            return Expression.Lambda<Action<T, object>>(body, target, value).Compile();
        }
    }
}
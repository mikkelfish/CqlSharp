﻿// CqlSharp - CqlSharp.Test
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

using CqlSharp.Protocol;
using CqlSharp.Serialization;
using CqlSharp.Tracing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Test
{
    [TestClass]
    public class QueryTests
    {
        private const string ConnectionString =
            "server=localhost;throttle=256;MaxConnectionIdleTime=3600;ConnectionStrategy=Exclusive;loggerfactory=debug;loglevel=verbose;username=cassandra;password=cassandra";

        [ClassInitialize]
        public static void Init(TestContext context)
        {
            const string createKsCql =
                @"CREATE KEYSPACE Test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = 'false';";
            const string createTableCql = @"create table Test.BasicFlow (id int primary key, value text, ignored text);";


            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.SetConnectionTimeout(0);
                connection.Open();

                try
                {
                    var createKs = new CqlCommand(connection, createKsCql);
                    createKs.ExecuteNonQuery();
                }
                catch (AlreadyExistsException)
                {
                    //ignore
                }

                try
                {
                    var createTable = new CqlCommand(connection, createTableCql);
                    createTable.ExecuteNonQuery();
                }
                catch (AlreadyExistsException)
                {
                }
            }
        }

        [ClassCleanup]
        public static void Cleanup()
        {
            const string dropCql = @"drop keyspace Test;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                try
                {
                    var drop = new CqlCommand(connection, dropCql);
                    drop.ExecuteNonQuery();
                }
                catch (InvalidException)
                {
                    //ignore
                }
            }

            CqlConnection.ShutdownAll();
        }

        [TestInitialize]
        public void PrepareTest()
        {
            const string truncateTableCql = @"truncate Test.BasicFlow;";

            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();
                var truncTable = new CqlCommand(connection, truncateTableCql);
                truncTable.ExecuteNonQuery();
            }
        }


        [TestMethod]
        public async Task BasicPrepareInsertSelect()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (?,?);";
            const string retrieveCql = @"select * from Test.BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd.PrepareAsync();

                var b = new BasicFlowData { Id = 123, Data = "Hallo", Ignored = "none" };
                cmd.PartitionKey.Set(b);
                cmd.Parameters.Set(b);

                await cmd.ExecuteNonQueryAsync();

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One) { EnableTracing = true };
                await selectCmd.PrepareAsync();

                CqlDataReader<BasicFlowData> reader = await selectCmd.ExecuteReaderAsync<BasicFlowData>();
                Assert.AreEqual(1, reader.Count);
                if (await reader.ReadAsync())
                {
                    BasicFlowData row = reader.Current;
                    Assert.AreEqual(123, row.Id);
                    Assert.AreEqual("Hallo", row.Data);
                    Assert.IsNull(row.Ignored);
                }
                else
                {
                    Assert.Fail("Read should have succeeded");
                }
            }
        }

        [TestMethod]
        // ReSharper disable InconsistentNaming
        public void Issue19_PrepareAndSelectCountStar()
        // ReSharper restore InconsistentNaming
        {
            //Assume
            const string insertCql = @"select count(*) from system.schema_keyspaces where keyspace_name = 'system';";

            long count;

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                //known issue in Cassandra 2.0.0 and 2.0.1. Prepare returns wrong results for count(*) queries
                Assert.IsNotNull(connection.ServerVersion);
                if (connection.ServerVersion.Equals("2.0.0") || connection.ServerVersion.Equals("2.0.1"))
                    return;

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.Quorum);
                cmd.Prepare();

                count = (long)(cmd.ExecuteScalar());
            }

            Assert.IsTrue(count > 0, "Count should be larger than zero");
        }

        [TestMethod]
        public async Task BasicInsertSelect()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (789,'Hallo 789');";
            const string retrieveCql = @"select * from Test.BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd.ExecuteNonQueryAsync();

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One);
                await selectCmd.PrepareAsync();

                CqlDataReader reader = await selectCmd.ExecuteReaderAsync();
                Assert.AreEqual(1, reader.Count);
                if (await reader.ReadAsync())
                {
                    Assert.AreEqual(789, reader["id"]);
                    Assert.AreEqual("Hallo 789", reader["value"]);
                    Assert.AreEqual(DBNull.Value, reader["ignored"]);
                }
                else
                {
                    Assert.Fail("Read should have succeeded");
                }
            }
        }

        [TestMethod]
        public async Task InsertSelectCustomParameters()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (?,?);";
            const string retrieveCql = @"select * from Test.BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                cmd.Parameters.Add("myKey", CqlType.Int);
                cmd.Parameters.Add("myValue", CqlType.Text);
                await cmd.PrepareAsync();

                cmd.Parameters["myKey"].Value = 1234;
                cmd.Parameters["myValue"].Value = "Hallo 1234";
                await cmd.ExecuteNonQueryAsync();

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One);

                CqlDataReader reader = await selectCmd.ExecuteReaderAsync();
                Assert.AreEqual(1, reader.Count);
                if (await reader.ReadAsync())
                {
                    Assert.AreEqual(1234, reader["id"]);
                    Assert.AreEqual("Hallo 1234", reader["value"]);
                    Assert.AreEqual(DBNull.Value, reader["ignored"]);
                }
                else
                {
                    Assert.Fail("Read should have succeeded");
                }
            }
        }

        [TestMethod]
        public async Task BatchPreparedWithNamedParameters()
        {
            //Assume
            const string insertCql = @"begin batch insert into Test.BasicFlow (id,value) values (:id1,:value1); insert into Test.BasicFlow (id,value) values (:id2,:value2); apply batch;";
            const string retrieveCql = @"select * from Test.BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd.PrepareAsync();

                cmd.Parameters["id1"].Value = 0;
                cmd.Parameters["value1"].Value = "Hello 0";
                cmd.Parameters["id2"].Value = 1;
                cmd.Parameters["value2"].Value = "Hello 1";
                await cmd.ExecuteNonQueryAsync();

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One);

                CqlDataReader reader = await selectCmd.ExecuteReaderAsync();
                Assert.AreEqual(2, reader.Count);

                var results = new bool[2];
                for (int i = 0; i < 2; i++)
                {
                    if (await reader.ReadAsync())
                    {
                        results[(int)reader["id"]] = true;
                        Assert.AreEqual("Hello " + reader["id"], reader["value"]);
                    }
                    else
                    {
                        Assert.Fail("Read should have succeeded");
                    }
                }

                Assert.IsTrue(results.All(p => p));
            }
        }

        [TestMethod]
        public async Task ChangeDatabaseThenInsertSelect()
        {
            //Assume
            const string insertCql = @"insert into BasicFlow (id,value) values (901,'Hallo 901');";
            const string retrieveCql = @"select * from BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                Assert.AreEqual("", connection.Database);

                await connection.OpenAsync();

                //change database
                connection.ChangeDatabase("test");

                Assert.AreEqual("test", connection.Database);

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd.ExecuteNonQueryAsync();

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One);
                CqlDataReader reader = await selectCmd.ExecuteReaderAsync();

                Assert.AreEqual(1, reader.Count);
                if (await reader.ReadAsync())
                {
                    Assert.AreEqual(901, reader["id"]);
                    Assert.AreEqual("Hallo 901", reader["value"]);
                    Assert.AreEqual(DBNull.Value, reader["ignored"]);
                }
                else
                {
                    Assert.Fail("Read should have succeeded");
                }
            }
        }

        [TestMethod]
        public async Task CASInsertSelect()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (901,'Hallo 901') if not exists;";
            const string insertCql2 = @"insert into Test.BasicFlow (id,value) values (901,'Hallo 901.2') if not exists;";

            const string retrieveCql = @"select * from Test.BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //skip when server version is below 2.0.0
                if (String.Compare(connection.ServerVersion, "2.0.0", StringComparison.Ordinal) < 0)
                    return;

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.Any);
                cmd.UseCASLocalSerial = true;
                await cmd.ExecuteNonQueryAsync();
                var result = cmd.LastQueryResult as CqlDataReader;
                Assert.IsNotNull(result);
                Assert.IsTrue(await result.ReadAsync());
                Assert.IsTrue((bool)result["[applied]"]);

                var cmd2 = new CqlCommand(connection, insertCql2, CqlConsistency.Any);
                await cmd2.ExecuteNonQueryAsync();
                var result2 = cmd2.LastQueryResult as CqlDataReader;
                Assert.IsNotNull(result2);
                Assert.IsTrue(await result2.ReadAsync());
                Assert.IsFalse((bool)result2["[applied]"]);
                Assert.AreEqual("Hallo 901", result2["value"]);

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One);
                await selectCmd.PrepareAsync();

                CqlDataReader reader = await selectCmd.ExecuteReaderAsync();
                Assert.AreEqual(1, reader.Count);
                if (await reader.ReadAsync())
                {
                    Assert.AreEqual(901, reader["id"]);
                    Assert.AreEqual("Hallo 901", reader["value"]);
                    Assert.AreEqual(DBNull.Value, reader["ignored"]);
                }
                else
                {
                    Assert.Fail("Read should have succeeded");
                }
            }
        }


        [TestMethod]
        public async Task SelectWithPaging()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (?,?);";
            const string retrieveCql = @"select * from Test.BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd.PrepareAsync();

                for (int i = 0; i < 100; i++)
                {
                    cmd.Parameters[0].Value = i;
                    cmd.Parameters[1].Value = "Hello " + i;
                    await cmd.ExecuteNonQueryAsync();
                }

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One);
                selectCmd.PageSize = 10;

                CqlDataReader reader = await selectCmd.ExecuteReaderAsync();

                //no paging when version < 2.0.0 is used...
                if (String.Compare(connection.ServerVersion, "2.0.0", StringComparison.Ordinal) < 0)
                    Assert.AreEqual(100, reader.Count);
                else
                    Assert.AreEqual(10, reader.Count);

                var results = new bool[100];
                for (int i = 0; i < 100; i++)
                {
                    if (await reader.ReadAsync())
                    {
                        results[(int)reader["id"]] = true;
                        Assert.AreEqual("Hello " + reader["id"], reader["value"]);
                    }
                    else
                    {
                        Assert.Fail("Read should have succeeded");
                    }
                }

                Assert.IsTrue(results.All(p => p));
            }
        }

        [TestMethod]
        public async Task PrepareNoArguments()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (2, 'Hallo 2');";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //prepare command
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd.PrepareAsync();

                Assert.IsTrue(cmd.IsPrepared);
                Assert.AreEqual(0, cmd.Parameters.Count);
                Assert.IsTrue(cmd.Parameters.IsReadOnly);
                Assert.IsTrue(cmd.Parameters.IsFixedSize);
                Assert.IsInstanceOfType(cmd.LastQueryResult, typeof(CqlPrepared));
                var prepareResult = (CqlPrepared)cmd.LastQueryResult;
                Assert.IsFalse(prepareResult.FromCache);
            }
        }

        [TestMethod]
        public async Task PrepareAndReprepare()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (2, ?);";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //prepare command
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd.PrepareAsync();

                Assert.IsTrue(cmd.IsPrepared);
                Assert.AreEqual(1, cmd.Parameters.Count);
                Assert.IsTrue(cmd.Parameters.IsReadOnly);
                Assert.IsTrue(cmd.Parameters.IsFixedSize);
                Assert.IsInstanceOfType(cmd.LastQueryResult, typeof(CqlPrepared));
                var prepareResult = (CqlPrepared)cmd.LastQueryResult;
                Assert.IsFalse(prepareResult.FromCache);

                //reprepare
                var cmd2 = new CqlCommand(connection, insertCql, CqlConsistency.One);
                await cmd2.PrepareAsync();

                Assert.IsTrue(cmd2.IsPrepared);
                Assert.AreEqual(1, cmd2.Parameters.Count);
                Assert.IsTrue(cmd2.Parameters.IsReadOnly);
                Assert.IsTrue(cmd2.Parameters.IsFixedSize);
                Assert.IsInstanceOfType(cmd2.LastQueryResult, typeof(CqlPrepared));
                var prepareResult2 = (CqlPrepared)cmd2.LastQueryResult;
                Assert.IsTrue(prepareResult2.FromCache);
            }
        }


        [TestMethod]
        public async Task InsertTracing()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (567,'Hallo 567');";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                cmd.EnableTracing = true;
                await cmd.ExecuteNonQueryAsync();

                Assert.IsInstanceOfType(cmd.LastQueryResult, typeof(CqlVoid));
                Assert.IsTrue(cmd.LastQueryResult.TracingId.HasValue, "Expected a tracing id");

                var tracer = new QueryTraceCommand(connection, cmd.LastQueryResult.TracingId.Value);
                TracingSession session = await tracer.GetTraceSessionAsync(CancellationToken.None);

                Assert.IsNotNull(session);
            }
        }


        [TestMethod]
        public async Task PrepareTracing()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (542,'Hallo 542');";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                cmd.EnableTracing = true;
                await cmd.PrepareAsync();

                Assert.IsInstanceOfType(cmd.LastQueryResult, typeof(CqlPrepared));
                Assert.IsTrue(cmd.LastQueryResult.TracingId.HasValue, "Expected a tracing id");

                var tracer = new QueryTraceCommand(connection, cmd.LastQueryResult.TracingId.Value);
                TracingSession session = await tracer.GetTraceSessionAsync(CancellationToken.None);

                Assert.IsNotNull(session);
            }
        }

        [TestMethod]
        public async Task BasicInsertSelectOnSynchronizationContext()
        {
            SynchronizationContext.SetSynchronizationContext(new STASynchronizationContext());
            await BasicPrepareInsertSelect();
        }

        [TestMethod]
        public async Task QueryWithBoundParameters()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (?,?);";
            const string retrieveCql = @"select * from Test.BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();

                //define command
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);

                //add parameters, infer Cql types
                cmd.Parameters.Add("id", 123);
                cmd.Parameters.Add("value", "Hallo");

                try
                {
                    //execute
                    await cmd.ExecuteNonQueryAsync();
                }
                catch (InvalidException)
                {
                    Assert.IsNotNull(connection.ServerVersion);
                    if (String.Compare(connection.ServerVersion, "2.0.0", StringComparison.Ordinal) < 0)
                    {
                        return;
                    }

                    throw;
                }

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One);

                CqlDataReader<BasicFlowData> reader = await selectCmd.ExecuteReaderAsync<BasicFlowData>();
                Assert.AreEqual(1, reader.Count);
                if (await reader.ReadAsync())
                {
                    BasicFlowData row = reader.Current;
                    Assert.AreEqual(123, row.Id);
                    Assert.AreEqual("Hallo", row.Data);
                    Assert.IsNull(row.Ignored);
                }
                else
                {
                    Assert.Fail("Read should have succeeded");
                }
            }
        }

        [TestMethod]
        public void BasicFlowAdo()
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (?,?);";
            const string retrieveCql = @"select id,value,ignored from Test.BasicFlow;";

            //Act

            using (IDbConnection connection = new CqlConnection(ConnectionString))
            {
                connection.Open();

                IDbCommand cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                cmd.Parameters.Add(new CqlParameter("id", CqlType.Int));
                cmd.Parameters.Add(new CqlParameter("value", CqlType.Text));

                cmd.Prepare();

                ((IDbDataParameter)cmd.Parameters["id"]).Value = 456;
                ((IDbDataParameter)cmd.Parameters["value"]).Value = "Hallo 456";

                cmd.ExecuteNonQuery();

                IDbCommand selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One) { EnableTracing = true };
                IDataReader reader = selectCmd.ExecuteReader();

                DataTable schema = reader.GetSchemaTable();
                Assert.AreEqual(3, schema.Rows.Count);
                Assert.IsTrue(
                    schema.Rows.Cast<DataRow>().Any(row => row[CqlSchemaTableColumnNames.ColumnName].Equals("ignored")));

                if (reader.Read())
                {
                    int id = reader.GetInt32(0);
                    string value = reader.GetString(1);
                    Assert.AreEqual(456, id);
                    Assert.IsTrue(reader.IsDBNull(2));
                    Assert.AreEqual("Hallo 456", value);
                }
                else
                {
                    Assert.Fail("Read should have succeeded");
                }
            }
        }

        [TestMethod]
        public async Task BatchInsertLogged()
        {
            await BatchInsertInternal(CqlBatchType.Logged);
        }

        [TestMethod]
        public async Task BatchInsertUnlogged()
        {
            await BatchInsertInternal(CqlBatchType.Unlogged);
        }

        private async Task BatchInsertInternal(CqlBatchType batchType)
        {
            //Assume
            const string insertCql = @"insert into Test.BasicFlow (id,value) values (?,?);";
            const string retrieveCql = @"select id,value,ignored from Test.BasicFlow;";

            //Act
            using (var connection = new CqlConnection(ConnectionString))
            {
                await connection.OpenAsync();
                var transaction = connection.BeginTransaction();
                transaction.BatchType = batchType;

                //insert data
                var cmd = new CqlCommand(connection, insertCql, CqlConsistency.One);
                cmd.Transaction = transaction;

                await cmd.PrepareAsync();

                for (int i = 0; i < 10; i++)
                {
                    cmd.Parameters[0].Value = i;
                    cmd.Parameters[1].Value = "Hello " + i;
                    await cmd.ExecuteNonQueryAsync();
                }

                var cmd2 = new CqlCommand(connection, insertCql, CqlConsistency.One);
                cmd2.Transaction = transaction;
                cmd2.Parameters.Add("id", CqlType.Int);
                cmd2.Parameters.Add("value", CqlType.Text);

                for (int i = 10; i < 20; i++)
                {
                    cmd2.Parameters[0].Value = i;
                    cmd2.Parameters[1].Value = "Hello " + i;
                    await cmd2.ExecuteNonQueryAsync();
                }

                try
                {
                    await transaction.CommitAsync();
                }
                catch (ProtocolException pex)
                {
                    //skip when server version is below 2.0.0
                    if (pex.Code == ErrorCode.Protocol && String.Compare(connection.ServerVersion, "2.0.0", StringComparison.Ordinal) < 0)
                        return;

                    throw;
                }

                //select data
                var selectCmd = new CqlCommand(connection, retrieveCql, CqlConsistency.One);
                CqlDataReader reader = await selectCmd.ExecuteReaderAsync();
                Assert.AreEqual(20, reader.Count);

                var results = new bool[20];
                for (int i = 0; i < 20; i++)
                {
                    if (await reader.ReadAsync())
                    {
                        results[(int)reader["id"]] = true;
                        Assert.AreEqual("Hello " + reader["id"], reader["value"]);
                    }
                    else
                    {
                        Assert.Fail("Read should have succeeded");
                    }
                }

                Assert.IsTrue(results.All(p => p));

                Assert.IsNotNull(transaction.LastBatchResult);

            }
        }


    }

    #region Nested type: BasicFlowData

    [CqlTable("basicflow", Keyspace = "test")]
    public class BasicFlowData
    {
        [CqlColumn("id", PartitionKeyIndex = 0, CqlType = CqlType.Int)]
        public int Id;

        [CqlColumn("value")]
        public string Data { get; set; }

        [CqlIgnore]
        public string Ignored { get; set; }
    }

    #endregion
}
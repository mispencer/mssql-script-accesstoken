using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.Data.SqlClient;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.Services.AppAuthentication;
using System.Runtime.CompilerServices;

namespace SQLScriptExecutor {
    class Program {
        async static Task<int> Main(string[] args) {
            var rootCommand = new RootCommand {
                /*
                new Option<bool>(
                    "--use-azure-access-token",
                    getDefaultValue: () => false,
                    description: "Whether to connect with a Azure AD access token"),
                new Option<bool>(
                    "--verbose",
                    getDefaultValue: () => true,
                    description: "Whether to write logs to console"),
                new Option<string>(
                    "--connection-string",
                    description: "the database connection string") {
                        Required = true 
                    },
                */
            };

            var scriptCommand = new Command("script") {
                new Option<FileInfo>(
                    "--sql-file",
                    description: "Path to a batch sql file") {
                        Required = true
                    },
                new Option<bool>(
                    "--use-azure-access-token",
                    getDefaultValue: () => false,
                    description: "Whether to connect with a Azure AD access token"),
                new Option<bool>(
                    "--verbose",
                    getDefaultValue: () => true,
                    description: "Whether to write logs to console"),
                new Option<string>(
                    "--connection-string",
                    description: "the database connection string") {
                        Required = true 
                    },
            };
            scriptCommand.Description = "Run a sql script against a MSSQL database using an azure access token";
            scriptCommand.Handler = CommandHandler.Create<bool, bool, string, FileInfo>(ExecuteScript);
            rootCommand.Add(scriptCommand);

            var addUserCommand = new Command("add-ad-user") {
                new Option<string>(
                    "--user-display-name",
                    description: "Display Name of user") {
                        Required = true
                    },
                new Option<Guid>(
                    "--user-object-id",
                    description: "Object id of user") {
                        Required = true
                    },
                new Option<string>(
                    "--user-type",
                    getDefaultValue: () => "E",
                    description: "Type of user"),
                new Option<bool>(
                    "--use-azure-access-token",
                    getDefaultValue: () => false,
                    description: "Whether to connect with a Azure AD access token"),
                new Option<bool>(
                    "--verbose",
                    getDefaultValue: () => true,
                    description: "Whether to write logs to console"),
                new Option<string>(
                    "--connection-string",
                    description: "the database connection string") {
                        Required = true 
                    },
            };
            addUserCommand.Handler = CommandHandler.Create<bool, bool,string, string, Guid, string>(AddUser);
            addUserCommand.Description = "Add AD user by Object ID";
            rootCommand.Add(addUserCommand);

            return await rootCommand.InvokeAsync(args);
        }

        static async Task AddUser(bool verbose, bool useAzureAccessToken, string connectionString, string userDisplayName, Guid userObjectId, string userType) {
            WriteVerbose(verbose, "Starting");
            using var connection = await GetConnection(verbose, useAzureAccessToken, connectionString);

            WriteVerbose(verbose, "Checking if user already exists");
            using var countCmd = connection.CreateCommand();
            countCmd.CommandText = "SELECT COUNT(*) FROM sys.database_principals WHERE name = @displayName";
            countCmd.Parameters.AddWithValue("displayName", userDisplayName);
            var count = (int)countCmd.ExecuteScalar();
            await countCmd.DisposeAsync();
            if (count > 0) {
                WriteVerbose(verbose, "User already exists");
                return;
            }

            WriteVerbose(verbose, "Fetching SID");
            using var guidStringCmd = connection.CreateCommand();
            guidStringCmd.CommandText = "SELECT CONVERT(VARCHAR(1000), CAST(CAST(@objectId AS UNIQUEIDENTIFIER) AS VARBINARY(16)),1) SID";
            guidStringCmd.Parameters.AddWithValue("objectId", userObjectId.ToString());
            var guidString = await guidStringCmd.ExecuteScalarAsync();
            await guidStringCmd.DisposeAsync();

            WriteVerbose(verbose, "Creating User SID");
            using var addUserCommand = connection.CreateCommand();
            guidStringCmd.CommandText = $"CREATE USER [{userDisplayName}] WITH SID={guidString}, TYPE={userType};";
            guidStringCmd.ExecuteNonQuery();
            WriteVerbose(verbose, "Done");
        }

        private static async Task ExecuteScript(bool verbose, bool useAzureAccessToken, string connectionString, FileInfo sqlFile) {
            WriteVerbose(verbose, "Starting");
            using var batchReader = sqlFile.OpenText();
            var batchSql = batchReader.ReadToEnd();

            var connection = await GetConnection(verbose, useAzureAccessToken, connectionString);
            WriteVerbose(verbose, "Executing sql");
            await connection.ExecuteSqlScript(batchSql, message => WriteVerbose(verbose, message));
            WriteVerbose(verbose, "Done");
        }

        private static void WriteVerbose(bool verbose, string message) {
            if (verbose) {
                Console.WriteLine(message);
            }

        }

        private static async Task<SqlConnection> GetConnection(bool verbose, bool useAzureAccessToken, string connectionString) {
            var connection = new SqlConnection(connectionString);
            if (useAzureAccessToken) {
                var tokenProvider = new AzureServiceTokenProvider();
                WriteVerbose(verbose, "Fetching token");
                connection.AccessToken = await tokenProvider.GetAccessTokenAsync("https://database.windows.net/");
                WriteVerbose(verbose, "Fetched token");
            }
            WriteVerbose(verbose, "Opening connection");
            await connection.OpenAsync();
            return connection;
        }
    }

    //Based on https://stackoverflow.com/a/52443620
    internal static class SqlCommandExtensions {
        private const string BatchTerminator = "GO";
        public static async Task ExecuteSqlScript(this SqlConnection sqlConnection, string sqlBatch, Action<string> writeVerbose)
        {
            // Handle backslash utility statement (see http://technet.microsoft.com/en-us/library/dd207007.aspx)
            sqlBatch = Regex.Replace(sqlBatch, @"\\(\r\n|\r|\n)", string.Empty);

            // Handle batch splitting utility statement (see http://technet.microsoft.com/en-us/library/ms188037.aspx)
            var batches = Regex.Split(
                sqlBatch,
                string.Format(CultureInfo.InvariantCulture, @"^\s*({0}[ \t]+[0-9]+|{0})(?:\s+|$)", BatchTerminator),
                RegexOptions.IgnoreCase | RegexOptions.Multiline);

            writeVerbose($"Batch count: {batches.Length}");

            for (int i = 0; i < batches.Length; ++i)
            {
                // Skip batches that merely contain the batch terminator
                if (batches[i].StartsWith(BatchTerminator, StringComparison.OrdinalIgnoreCase) ||
                    (i == batches.Length - 1 && string.IsNullOrWhiteSpace(batches[i])))
                {
                    writeVerbose($"Skipping batch {i}");
                    continue;
                }

                async Task RunCommand(string sql)
                {
                    writeVerbose("Running:");
                    writeVerbose(sql);
                    var command = sqlConnection.CreateCommand();
                    command.CommandText = sql; ;
                    var resultCount = await command.ExecuteNonQueryAsync();
                    writeVerbose($"Result Count: {resultCount}");
                }

                // Include batch terminator if the next element is a batch terminator
                if (batches.Length > i + 1 &&
                    batches[i + 1].StartsWith(BatchTerminator, StringComparison.OrdinalIgnoreCase))
                {
                    int repeatCount = 1;

                    // Handle count parameter on the batch splitting utility statement
                    if (!string.Equals(batches[i + 1], BatchTerminator, StringComparison.OrdinalIgnoreCase))
                    {
                        repeatCount = int.Parse(Regex.Match(batches[i + 1], @"([0-9]+)").Value, CultureInfo.InvariantCulture);
                    }
                    writeVerbose($"Repeat Count for {i}: {repeatCount}");

                    for (int j = 0; j < repeatCount; ++j)
                    {
                        await RunCommand(batches[i]);
                    }
                }
                else
                {
                    await RunCommand(batches[i]);
                }
            }
        }
    }
}

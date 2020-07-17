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
        static int Main(string[] args) {
            // Create a root command with some options
            var rootCommand = new RootCommand {
                new Option<string>(
                    "--connection-string",
                    description: "The database connection string"),
                new Option<FileInfo>(
                    "--sql-file",
                    description: "Path to a batch sql file"),
            };

            rootCommand.Description = "Run a sql script against a MSSQL database using an azure access token";

            // Note that the parameters of the handler method are matched according to the names of the options
            rootCommand.Handler = CommandHandler.Create<string, FileInfo>(Run);

            // Parse the incoming args and invoke the handler
            return rootCommand.InvokeAsync(args).Result;
        }

        static async Task Run(string connectionString, FileInfo batchFile) {
            using var batchReader = batchFile.OpenText();
            var batchSql = batchReader.ReadToEnd();

            var tokenProvider = new AzureServiceTokenProvider();
            var connection = new SqlConnection(connectionString) {
                AccessToken = await tokenProvider.GetAccessTokenAsync("https://database.windows.net/")
            };
            await connection.OpenAsync();
            await connection.ExecuteSqlScript(batchSql);
        }
    }

    //Based on https://stackoverflow.com/a/52443620
    internal static class SqlCommandExtensions {
        private const string BatchTerminator = "GO";
        public static async Task ExecuteSqlScript(this SqlConnection sqlConnection, string sqlBatch)
        {
            // Handle backslash utility statement (see http://technet.microsoft.com/en-us/library/dd207007.aspx)
            sqlBatch = Regex.Replace(sqlBatch, @"\\(\r\n|\r|\n)", string.Empty);

            // Handle batch splitting utility statement (see http://technet.microsoft.com/en-us/library/ms188037.aspx)
            var batches = Regex.Split(
                sqlBatch,
                string.Format(CultureInfo.InvariantCulture, @"^\s*({0}[ \t]+[0-9]+|{0})(?:\s+|$)", BatchTerminator),
                RegexOptions.IgnoreCase | RegexOptions.Multiline);

            for (int i = 0; i < batches.Length; ++i)
            {
                // Skip batches that merely contain the batch terminator
                if (batches[i].StartsWith(BatchTerminator, StringComparison.OrdinalIgnoreCase) ||
                    (i == batches.Length - 1 && string.IsNullOrWhiteSpace(batches[i])))
                {
                    continue;
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

                    for (int j = 0; j < repeatCount; ++j)
                    {
                       var command = sqlConnection.CreateCommand();
                       command.CommandText = batches[i];
                       await command.ExecuteNonQueryAsync();
                    }
                }
                else
                {
                    var command = sqlConnection.CreateCommand();
                    command.CommandText = batches[i];
                    await command.ExecuteNonQueryAsync();
                }
            }
        }
    }
}

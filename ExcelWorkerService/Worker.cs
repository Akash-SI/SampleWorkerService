using ExcelDataReader;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ExcelWorkerService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            return base.StartAsync(cancellationToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            // no need to dispose the http client here since its lifetime is tracked by HttpClientFactory
            // https://docs.microsoft.com/en-us/aspnet/core/fundamentals/http-requests?view=aspnetcore-2.2#httpclient-and-lifetime-management

            _logger.LogInformation("The service has been stopped");
            return base.StopAsync(cancellationToken);
        }

        private void CancelSourceTask(CancellationToken sourceToken)
        {
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            sourceToken = tokenSource.Token;
            tokenSource.Cancel();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            string DirectoryPath = Path.Combine(Environment.CurrentDirectory, @"Sample Excel Files\");
            string[] TotalFilePathsFound = Directory.GetFiles(DirectoryPath, "*.xls");

            if (TotalFilePathsFound.Count() == 0)
            {
                _logger.LogInformation("No excel files found at {path}. Exiting... ", DirectoryPath);
                CancelSourceTask(stoppingToken);
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                DataSet Result = new DataSet();

                for (int FoundFileIndex = 0; FoundFileIndex < TotalFilePathsFound.Count(); FoundFileIndex++)
                {
                    using FileStream stream = File.Open(TotalFilePathsFound[FoundFileIndex], FileMode.Open, FileAccess.Read);
                    using IExcelDataReader reader = ExcelReaderFactory.CreateReader(stream);

                    Result = reader.AsDataSet(new ExcelDataSetConfiguration()
                    {
                        // Gets or sets a callback to obtain configuration options for a DataTable. 
                        ConfigureDataTable = (tableReader) => new ExcelDataTableConfiguration()
                        {
                            // Gets or sets a value indicating whether to use a row from the data as column names.
                            UseHeaderRow = true
                        }
                    });
                }

                string SQLScriptForCreateTable = GetSQLScriptForCreateTABLE("xnewgl", Result.Tables[0]);

                RUNSQLCOMMANDS(SQLScriptForCreateTable);
                //Once all file(s) are processed, cancel the task.
                CancelSourceTask(stoppingToken);
            }

            return Task.CompletedTask;
        }

        public void RUNSQLCOMMANDS(string qry)
        {
            try
            {
                //SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

                //builder.DataSource = "ANIKA\SQLEXPRESS01";
                //builder.UserID = "<your_username>";
                //builder.Password = "<your_password>";
                //builder.InitialCatalog = "<your_database>";

                string connString = @"Server= ANIKA\SQLEXPRESS01; Database= TestDB; Integrated Security=True;";

                using (SqlConnection connection = new SqlConnection(connString))
                {
                    Console.WriteLine("\nQuery data example:");
                    Console.WriteLine("=========================================\n");

                    connection.Open();

                    String sql = qry;

                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        command.ExecuteNonQuery();
                        //using (SqlDataReader reader = command.ExecuteReader())
                        //{
                        //    while (reader.Read())
                        //    {
                        //        Console.WriteLine("{0} {1}", reader.GetString(0), reader.GetString(1));
                        //    }
                        //}
                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine(e.ToString());
            }
            Console.WriteLine("\nDone. Press enter.");
            Console.ReadLine();
        }

        public static string GetSQLScriptForCreateTABLE(string tableName, DataTable table)
        {
            string sqlsc;
            sqlsc = "CREATE TABLE " + tableName + "(";
            for (int i = 0; i < table.Columns.Count; i++)
            {
                sqlsc += "\n [" + table.Columns[i].ColumnName + "] ";
                string columnType = table.Columns[i].DataType.ToString();
                switch (columnType.ToUpper())
                {
                    case "SYSTEM.INT16":
                        sqlsc += " smallint";
                        break;
                    case "SYSTEM.INT32":
                        sqlsc += " int ";
                        break;
                    case "SYSTEM.INT64":
                        sqlsc += " bigint ";
                        break;
                    case "SYSTEM.BYTE":
                        sqlsc += " tinyint";
                        break;
                    case "SYSTEM.DECIMAL":
                        sqlsc += " decimal ";
                        break;
                    case "SYSTEM.SINGLE":
                        sqlsc += " single";
                        break;
                    case "SYSTEM.DOUBLE":
                        sqlsc += " FLOAT";
                        break;
                    case "SYSTEM.DATETIME":
                        sqlsc += " datetime ";
                        break;
                    case "SYSTEM.STRING":
                    default:
                        sqlsc += string.Format(" nvarchar({0}) ", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString());
                        break;
                }
                if (table.Columns[i].AutoIncrement)
                    sqlsc += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                if (!table.Columns[i].AllowDBNull)
                    sqlsc += " NOT NULL ";
                sqlsc += ",";
            }
            return sqlsc.Substring(0, sqlsc.Length - 1) + "\n)";
        }
    }
}

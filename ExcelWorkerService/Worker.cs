using ExcelDataReader;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
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
            string[] TotalFilePathsFound = Directory.GetFiles(DirectoryPath, "*.xlsx");

            if (TotalFilePathsFound.Count() == 0)
            {
                _logger.LogInformation("No excel files found at {path}. Exiting... ", DirectoryPath);
                CancelSourceTask(stoppingToken);
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                for (int FoundFileIndex = 0; FoundFileIndex < TotalFilePathsFound.Count(); FoundFileIndex++)
                {
                    using FileStream stream = File.Open(TotalFilePathsFound[FoundFileIndex], FileMode.Open, FileAccess.Read);
                    using IExcelDataReader reader = ExcelReaderFactory.CreateReader(stream);

                    using System.Data.DataSet result = reader.AsDataSet(new ExcelDataSetConfiguration()
                    {
                        // Gets or sets a callback to obtain configuration options for a DataTable. 
                        ConfigureDataTable = (tableReader) => new ExcelDataTableConfiguration()
                        {
                            // Gets or sets a value indicating whether to use a row from the data as column names.
                            UseHeaderRow = true
                        }
                    });
                }

                //Once all file(s) are processed, cancel the task.
                CancelSourceTask(stoppingToken);

                //_logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                //await Task.Delay(1000, stoppingToken);
            }

            return Task.CompletedTask;
        }
    }
}

using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Text.Json;
using TicketHubIsolated.Models;

namespace TicketHubIsolated
{
    public class QueueTriggerFunction
    {
        private readonly ILogger _logger;
        private readonly string _connectionString;

        public QueueTriggerFunction(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<QueueTriggerFunction>();
            _connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
        }

        [Function("QueueTriggerFunction")]
        public void Run([QueueTrigger("tickethub", Connection = "AzureWebJobsStorage")] string queueMessage)
        {
            _logger.LogInformation($"Processing queue message: {queueMessage}");

            try
            {
                var purchase = JsonSerializer.Deserialize<Purchase>(queueMessage);

                using var connection = new SqlConnection(_connectionString);
                connection.Open();

                var command = new SqlCommand(@"
                    INSERT INTO TicketPurchases (
                        ConcertId, Email, Name, Phone, Quantity,
                        CreditCard, Expiration, SecurityCode, Address,
                        City, Province, PostalCode, Country
                    ) VALUES (
                        @ConcertId, @Email, @Name, @Phone, @Quantity,
                        @CreditCard, @Expiration, @SecurityCode, @Address,
                        @City, @Province, @PostalCode, @Country
                    )", connection);

                command.Parameters.AddWithValue("@ConcertId", purchase.ConcertId);
                command.Parameters.AddWithValue("@Email", purchase.Email);
                command.Parameters.AddWithValue("@Name", purchase.Name);
                command.Parameters.AddWithValue("@Phone", purchase.Phone ?? "");
                command.Parameters.AddWithValue("@Quantity", purchase.Quantity);
                command.Parameters.AddWithValue("@CreditCard", purchase.CreditCard ?? "");
                command.Parameters.AddWithValue("@Expiration", purchase.Expiration ?? "");
                command.Parameters.AddWithValue("@SecurityCode", purchase.SecurityCode ?? "");
                command.Parameters.AddWithValue("@Address", purchase.Address ?? "");
                command.Parameters.AddWithValue("@City", purchase.City ?? "");
                command.Parameters.AddWithValue("@Province", purchase.Province ?? "");
                command.Parameters.AddWithValue("@PostalCode", purchase.PostalCode ?? "");
                command.Parameters.AddWithValue("@Country", purchase.Country ?? "");

                command.ExecuteNonQuery();

                _logger.LogInformation("Successfully inserted into SQL DB.");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to process message: {ex.Message}");
            }
        }
    }
}

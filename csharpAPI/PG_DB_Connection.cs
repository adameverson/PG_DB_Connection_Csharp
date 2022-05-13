/*
using System;

namespace csharpAPI
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            AzurePostgresCreate teste = new AzurePostgresCreate();
        }
    }
}
*/

using System;
using Npgsql;

namespace Driver
{
    public class PG_DB_Connection
    {
        // Obtain connection string information from the portal
        //
        /*
        private static string Host = "mydemoserver.postgres.database.azure.com";
        private static string User = "mylogin@mydemoserver";
        private static string DBname = "mypgsqldb";
        private static string Password = "<server_admin_password>";
        private static string Port = "5432";
        */

        private static string Host = "localhost";
        private static string User = "postgres";
        private static string DBname = "SESGO_27042022";
        private static string Password = "postgres";
        private static string Port = "5432";

        private static int qtdParaAnalise = 0;

        static void Main(string[] args)
        {
            var txtPrestacoes = "81500, 81541, 81598, 81641, 81692, 81713, 81792";

            /*
            Select("SELECT * FROM \"ContratosFornecedores\"");
            Select("SELECT * FROM \"ContratosFornecedores\" WHERE \"IdentificacaoRegistro\" IS NOT NULL AND \"IdentificacaoRegistro\" != ''");
            Select("SELECT * FROM \"ContratosFornecedores\" WHERE \"IdentificacaoRegistro\" IS NOT NULL AND \"IdentificacaoRegistro\" = ''");
            Select("SELECT * FROM \"ContratosFornecedores\" WHERE \"IdentificacaoRegistro\" IS NULL");
            */

            SelectCustomizado1($"SELECT COUNT(*) FROM \"ContratosFornecedores\" WHERE \"PrestacaoContaId\" IN ({txtPrestacoes}) AND \"IdentificacaoRegistro\" IS NOT NULL AND \"IdentificacaoRegistro\" != ''");
            SelectCustomizado2($"SELECT \"PrestacaoContaId\", \"NumeroContrato\", \"IdentificacaoRegistro\" FROM \"ContratosFornecedores\" WHERE \"PrestacaoContaId\" IN ({txtPrestacoes}) AND \"IdentificacaoRegistro\" IS NOT NULL AND \"IdentificacaoRegistro\" != ''");
            SelectCustomizado1($"SELECT COUNT(*) FROM \"ContratosFornecedores\" WHERE \"PrestacaoContaId\" IN ({txtPrestacoes}) AND \"Ativo\" = 'TRUE' AND \"IdentificacaoRegistro\" IS NULL");

            Console.WriteLine(
                string.Format(
                    "Quantidade Sem IdentificacaoRegistro = ({0})",
                    qtdParaAnalise
                    )
                );

            /*Insert();
            Select();
            Update();
            Select();
            Delete();
            Select();*/
        }

        static void Insert()
        {
            // Build connection string using parameters from portal
            //
            string connString =
                String.Format(
                    "Server={0};Username={1};Database={2};Port={3};Password={4};SSLMode=Prefer",
                    Host,
                    User,
                    DBname,
                    Port,
                    Password);


            using (var conn = new NpgsqlConnection(connString))

            {
                Console.Out.WriteLine("Opening connection");
                conn.Open();

                using (var command = new NpgsqlCommand("DROP TABLE IF EXISTS inventory", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished dropping table (if existed)");

                }

                using (var command = new NpgsqlCommand("CREATE TABLE inventory(id serial PRIMARY KEY, name VARCHAR(50), quantity INTEGER)", conn))
                {
                    command.ExecuteNonQuery();
                    Console.Out.WriteLine("Finished creating table");
                }

                using (var command = new NpgsqlCommand("INSERT INTO inventory (name, quantity) VALUES (@n1, @q1), (@n2, @q2), (@n3, @q3)", conn))
                {
                    command.Parameters.AddWithValue("n1", "banana");
                    command.Parameters.AddWithValue("q1", 150);
                    command.Parameters.AddWithValue("n2", "orange");
                    command.Parameters.AddWithValue("q2", 154);
                    command.Parameters.AddWithValue("n3", "apple");
                    command.Parameters.AddWithValue("q3", 100);

                    int nRows = command.ExecuteNonQuery();
                    Console.Out.WriteLine(String.Format("Number of rows inserted={0}", nRows));
                }
            }

            Console.WriteLine("Press RETURN to exit");
            Console.ReadLine();
        }

        static void SelectCustomizado1(string txtQuery)
        {
            // Build connection string using parameters from portal
            //
            string connString =
                String.Format(
                    "Server={0}; User Id={1}; Database={2}; Port={3}; Password={4};SSLMode=Prefer",
                    Host,
                    User,
                    DBname,
                    Port,
                    Password);

            using (var conn = new NpgsqlConnection(connString))
            {

                Console.Out.WriteLine("Opening connection");
                conn.Open();


                using (var command = new NpgsqlCommand(txtQuery, conn))
                {

                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.WriteLine(
                            string.Format(
                                "Quantidade a analisar = ({0})",
                                reader.GetInt32(0).ToString()
                                )
                            );
                        qtdParaAnalise = reader.GetInt32(0);
                    }
                    reader.Close();
                }
            }

            Console.WriteLine("Press RETURN to exit");
            Console.ReadLine();
        }

        static void SelectCustomizado2(string txtQuery)
        {
            // Build connection string using parameters from portal
            //
            string connString =
                String.Format(
                    "Server={0}; User Id={1}; Database={2}; Port={3}; Password={4};SSLMode=Prefer",
                    Host,
                    User,
                    DBname,
                    Port,
                    Password);

            using (var conn = new NpgsqlConnection(connString))
            {

                Console.Out.WriteLine("Opening connection");
                conn.Open();


                using (var command = new NpgsqlCommand(txtQuery, conn))
                {

                    var qtd = 0;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        using (var conn2 = new NpgsqlConnection(connString))
                        {

                            //Console.Out.WriteLine("Opening connection 2");
                            conn2.Open();

                            using (var command2 = new NpgsqlCommand("SELECT \"Id\" FROM \"ContratosFornecedores\" WHERE \"PrestacaoContaId\" = @p1 AND \"NumeroContrato\" = @n1 AND \"IdentificacaoRegistro\" IS NULL", conn2))
                            {
                                command2.Parameters.AddWithValue("p1", reader.GetInt32(0));
                                command2.Parameters.AddWithValue("n1", reader.GetString(1));

                                var reader2 = command2.ExecuteReader();
                                while (reader2.Read())
                                {
                                    Console.WriteLine(
                                        string.Format(
                                            "Id = ({0})",
                                            reader2.GetInt32(0)
                                            )
                                        );
                                    UpdateCustomizado2(reader.GetString(2), reader2.GetInt32(0));
                                }
                                reader2.Close();
                            }
                        }
                        qtd++;
                        var progresso = ((qtd * 100) / qtdParaAnalise);

                        //Console.Clear();
                        Console.WriteLine(
                            string.Format(
                                "Progresso = {1}%, Quantidade Analisada = ({0} / {2})",
                                qtd,
                                progresso,
                                qtdParaAnalise
                                )
                            );
                    }
                    reader.Close();
                }
            }

            Console.WriteLine("Press RETURN to exit");
            Console.ReadLine();
        }

        static void UpdateCustomizado2(string auxIR, int auxId)
        {
            // Build connection string using parameters from portal
            //
            string connString =
                String.Format(
                    "Server={0}; User Id={1}; Database={2}; Port={3}; Password={4};SSLMode=Prefer",
                    Host,
                    User,
                    DBname,
                    Port,
                    Password);

            using (var conn = new NpgsqlConnection(connString))
            {

                //Console.Out.WriteLine("Opening connection");
                conn.Open();

                using (var command = new NpgsqlCommand("UPDATE \"ContratosFornecedores\" SET \"IdentificacaoRegistro\" = @ir WHERE \"Id\" = @id", conn))
                {
                    command.Parameters.AddWithValue("ir", auxIR);
                    command.Parameters.AddWithValue("id", auxId);
                    int nRows = command.ExecuteNonQuery();
                    //Console.Out.WriteLine(String.Format("Number of rows updated={0}", nRows));
                }
            }

            //Console.WriteLine("Press RETURN to exit");
            //Console.ReadLine();
        }

        static void Delete()
        {
            // Build connection string using parameters from portal
            //
            string connString =
                String.Format(
                    "Server={0}; User Id={1}; Database={2}; Port={3}; Password={4};SSLMode=Prefer",
                    Host,
                    User,
                    DBname,
                    Port,
                    Password);

            using (var conn = new NpgsqlConnection(connString))
            {
                Console.Out.WriteLine("Opening connection");
                conn.Open();

                using (var command = new NpgsqlCommand("DELETE FROM inventory WHERE name = @n", conn))
                {
                    command.Parameters.AddWithValue("n", "orange");

                    int nRows = command.ExecuteNonQuery();
                    Console.Out.WriteLine(String.Format("Number of rows deleted={0}", nRows));
                }
            }

            Console.WriteLine("Press RETURN to exit");
            Console.ReadLine();
        }
    }
}


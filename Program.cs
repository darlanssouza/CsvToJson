using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CsvToJson
{
    class Program
    {
        static void Main()
        {
            var operacao = new ExecutandoOperacaoCsvParaJson();
            operacao.Main();
        }
    }

    public class ExecutandoOperacaoCsvParaJson
    {
        public void Main()
        {
            try
            {
                string caminhoAreaTrabalho = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
                string[] linhas = File.ReadAllLines($"{caminhoAreaTrabalho}//arquivo.csv");
                string[] cabecalho = linhas[0].Split(',');

                linhas = linhas.Where((source, index) => index != 0).ToArray();

                ProcessarArquivoCsv(cabecalho, linhas, caminhoAreaTrabalho);
            }
            catch (Exception e)
            {
                Console.WriteLine($"EXCECAO: {e.Message} - {DateTime.Now}");
            }
        }

        private void ProcessarArquivoCsv(string[] cabecalho, string[] linhas, string caminhoAreaTrabalho)
        {
            var contador = 1;
            var concurrentBagSucesso = new ConcurrentBag<string>();
            var concurrentBagErro = new ConcurrentBag<string>();
            var client = new HttpClient();
            var dataInicio = DateTime.Now;

            Parallel.ForEach(linhas, new ParallelOptions { MaxDegreeOfParallelism = 8 }, linha =>
             {
                 try
                 {
                     var objeto = new Dictionary<string, string>();
                     var valorLinha = linha.Split(',');

                     for (int j = 0; j < cabecalho.Length; j++)
                         objeto.Add(cabecalho[j], valorLinha[j]);

                     var resposta = ChamarPost(objeto, client);
                     Task.WaitAll(resposta);

                     if (resposta.Result.IsSuccessStatusCode)
                     {
                         concurrentBagSucesso.Add(linha);
                         Console.WriteLine($"SUCESSO: {Interlocked.Increment(ref contador)} - {DateTime.Now}");
                     }
                     else
                     {
                         concurrentBagErro.Add(linha);
                         Console.WriteLine($"ERRO: {Interlocked.Increment(ref contador)} - {DateTime.Now}");
                     }
                 }
                 catch
                 {
                     concurrentBagErro.Add(linha);
                 }
             });

            Console.WriteLine($"FIM!!! {DateTime.Now - dataInicio}");

            CriarArquivoDeSucesso(concurrentBagSucesso, caminhoAreaTrabalho);
            CriarArquivoDeErro(concurrentBagErro, caminhoAreaTrabalho);
        }

        private async Task<HttpResponseMessage> ChamarPost(Dictionary<string, string> objeto, HttpClient client)
        {
            string json = JsonConvert.SerializeObject(objeto);
            var requisicao = new StringContent(json, Encoding.UTF8, "application/json");
            return await client.PostAsync(new Uri("rota"), requisicao);
        }

        private void CriarArquivoDeSucesso(ConcurrentBag<string> linhas, string caminhoAreaTrabalho)
        {
            try
            {
                using (StreamWriter arquivoSaida = new StreamWriter(Path.Combine(caminhoAreaTrabalho, "SucessosProcessamento.csv"), true))
                {
                    foreach (var linha in linhas)
                        arquivoSaida.WriteLine(linha);
                }
            }
            catch
            {
                Console.WriteLine("SUCESSOS!!!");
                foreach (var linha in linhas)
                    Console.WriteLine(linha);
            }
        }

        private void CriarArquivoDeErro(ConcurrentBag<string> linhas, string caminhoAreaTrabalho)
        {
            try
            {
                using (StreamWriter arquivoSaida = new StreamWriter(Path.Combine(caminhoAreaTrabalho, "ErrosProcessamento.csv"), true))
                {
                    foreach (var linha in linhas)
                        arquivoSaida.WriteLine(linha);
                }
            }
            catch
            {
                Console.WriteLine("ERROS!!!");
                foreach (var linha in linhas)
                    Console.WriteLine(linha);
            }
        }
    }
}

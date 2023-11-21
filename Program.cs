using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SQSReceiveMessages;

class Program
{
  private const int MaxMessages = 1;
  private const int WaitTime = 2;
  private const string MaxReceiveCount = "10";
  private const string ReceiveMessageWaitTime = "2";
  private const int MaxArgs = 3;
  
  // Exemplos de mensagens para enviar na fila
  private const string JsonMessage = "{\"product\":[{\"name\":\"Product A\",\"price\": \"32\"},{\"name\": \"Product B\",\"price\": \"27\"}]}";
  private const string XmlMessage = "<products><product name=\"Product A\" price=\"32\" /><product name=\"Product B\" price=\"27\" /></products>";
  private const string CustomMessage = "||product|Product A|32||product|Product B|27||";
  private const string TextMessage = "Just a plain text message.";

  // Credenciais AWS
  private const string AccessKey = "AccessKey";
  private const string SecretKey = "SecretKey";
  
  static async Task Main(string[] args)
  {
    // Cria o Client do SQS
    var credentials = new BasicAWSCredentials(AccessKey, SecretKey);
    var sqsClient = new AmazonSQSClient(credentials);

    var queueUrl = await StartQueueCreation(sqsClient, args);

    await StartSendingExampleMessages(sqsClient, queueUrl);
    
    // (Aqui poderia verificar se a fila existe)
    // Lê as mensagens da fila e realiza as ações
    Console.WriteLine($"Lendo mensagens da fila\n {queueUrl}");
    Console.WriteLine("Pressione qualquer botão para parar. (Resposta pode ter delay.)");
    do
    {
      var msg = await GetMessage(sqsClient, queueUrl, WaitTime);
      if (msg.Messages.Count == 0) continue;
      if (ProcessMessage(msg.Messages[0]))
        await DeleteMessage(sqsClient, msg.Messages[0], queueUrl);
    } while(!Console.KeyAvailable);
  }

  private static async Task<string> StartQueueCreation(AmazonSQSClient sqsClient, string[] args)
  {
    // Faz parse na linha de comando e exibe ajuda se necessário
    var parsedArgs = CommandLine.Parse(args);
    if (parsedArgs.Count > MaxArgs)
      CommandLine.ErrorExit(
        "\nMuitos argumentos na linha de comando.\n  Execute o comando sem argumentos para exibir a documentação de ajuda.");

    // Caso não tenha argumentos de linha de comando, exibe a documentação de ajuda e as filas existentes
    if(parsedArgs.Count == 0)
    {
      PrintHelp();
      Console.WriteLine("\nNenhum argumento especificado.");
      Console.Write("Você quer ver a lista de filas atual? ((s) or n): ");
      string response = Console.ReadLine();
      if((string.IsNullOrEmpty(response)) || (response.ToLower() == "s"))
        await ShowQueues(sqsClient);
      return string.Empty;
    }
    
    // Busca os argumentos da aplicação da lista parseada
    string queueName =
      CommandLine.GetArgument(parsedArgs, null, "-q", "--queue-name");
    string deadLetterQueueUrl =
      CommandLine.GetArgument(parsedArgs, null, "-d", "--dead-letter-queue");
    string maxReceiveCount =
      CommandLine.GetArgument(parsedArgs, MaxReceiveCount, "-m", "--max-receive-count");
    string receiveWaitTime =
      CommandLine.GetArgument(parsedArgs, ReceiveMessageWaitTime, "-w", "--wait-time");

    if(string.IsNullOrEmpty(queueName))
      CommandLine.ErrorExit(
        "\nVocê deve inserir um nome para a fila.\nExecute o comando sem argumentos para exibir a documentação de ajuda.");
    
    // Se uma fila de dead-letter não foi fornecida, cria uma
    if(string.IsNullOrEmpty(deadLetterQueueUrl))
    {
      Console.WriteLine("\nNenhuma fila de dead-letter foi especificada. Criando uma...");
      deadLetterQueueUrl = await CreateQueue(sqsClient, queueName + "__dlq");
      Console.WriteLine($"Sua nova fila de dead-letter:");
      await ShowAllAttributes(sqsClient, deadLetterQueueUrl);
    }

    // Cria uma fila de mensagens
    string messageQueueUrl = await CreateQueue(
      sqsClient, queueName, deadLetterQueueUrl, maxReceiveCount, receiveWaitTime);
    Console.WriteLine($"Sua nova fila de mensagens:");
    await ShowAllAttributes(sqsClient, messageQueueUrl);

    return messageQueueUrl;
  }
    
  //
  // Método para criar a fila e retornar a URL
  private static async Task<string> CreateQueue(
    IAmazonSQS sqsClient, string qName, string deadLetterQueueUrl=null,
    string maxReceiveCount=null, string receiveWaitTime=null)
  {
    var attrs = new Dictionary<string, string>();

    // Se uma fila de dead-letter é fornecida, cria uma fila de mensagem
    if(!string.IsNullOrEmpty(deadLetterQueueUrl))
    {
      attrs.Add(QueueAttributeName.ReceiveMessageWaitTimeSeconds, receiveWaitTime);
      attrs.Add(QueueAttributeName.RedrivePolicy,
        $"{{\"deadLetterTargetAmazonResourceName\":\"{await GetQueueArn(sqsClient, deadLetterQueueUrl)}\"," +
        $"\"maxReceiveCount\":\"{maxReceiveCount}\"}}");
      // Adicione outros atributos para fila de mensagem como VisibilityTimeout
    }

    // Cria a Fila
    CreateQueueResponse responseCreate = await sqsClient.CreateQueueAsync(
      new CreateQueueRequest{QueueName = qName, Attributes = attrs});
    return responseCreate.QueueUrl;
  }
  
  // Método para iniciar o envio de mensagens
  private static async Task StartSendingExampleMessages(AmazonSQSClient sqsClient, string queueUrl)
  {
    await SendMessage(sqsClient, queueUrl, JsonMessage);

    // Enviar um bloco de mensagens
    var batchMessages = new List<SendMessageBatchRequestEntry>{
      new("xmlMsg", XmlMessage),
      new("customeMsg", CustomMessage),
      new("textMsg", TextMessage)};
    await SendMessageBatch(sqsClient, queueUrl, batchMessages);

    // Permitir o usuário enviar sua própria mensagem ou sair da linha de comando
    await InteractWithUser(sqsClient, queueUrl);

    // Deleta todas as mensagens que ainda estão na fila
    //await DeleteAllMessages(sqsClient, queueUrl);
  }
    
  //
  // Método para inserir uma mensagem em uma fila
  // Poderia ser exapdandido para incluir atributos da mensagem, etc., em um objeto SendMessageRequest
  private static async Task SendMessage(
    IAmazonSQS sqsClient, string qUrl, string messageBody)
  {
    SendMessageResponse responseSendMsg =
      await sqsClient.SendMessageAsync(qUrl, messageBody);
    Console.WriteLine($"Mensagem adicionada à fila\n  {qUrl}");
    Console.WriteLine($"HttpStatusCode: {responseSendMsg.HttpStatusCode}");
  }


  //
  // Método para inserir um grupo de mensagens em uma fila
  // Poderia ser exapdandido para incluir atributos da mensagem, etc., no objeto SendMessageBatchRequestEntry
  private static async Task SendMessageBatch(
    IAmazonSQS sqsClient, string qUrl, List<SendMessageBatchRequestEntry> messages)
  {
    Console.WriteLine($"\nEnviando um pacote de mensagens para a fila\n  {qUrl}");
    SendMessageBatchResponse responseSendBatch =
      await sqsClient.SendMessageBatchAsync(qUrl, messages);
    // Poderia testar responseSendBatch.Failed aqui
    foreach(SendMessageBatchResultEntry entry in responseSendBatch.Successful)
      Console.WriteLine($"Mensagem {entry.Id} enfileirada com sucesso.");
  }


  //
  // Método para receber o input do usuário
  // O usuário pode fornecer mensagens para inserir na fila ou sair da aplicação
  private static async Task InteractWithUser(IAmazonSQS sqsClient, string qUrl)
  {
    string response;
    while (true)
    {
      // Pega o input do usuário
      Console.WriteLine("\nDigite uma mensagem para a fila ou \"exit\" para sair:");
      response = Console.ReadLine();
      if (response.ToLower() == "exit") break;

      // Coloca a mensagem do usuário na fila
      await SendMessage(sqsClient, qUrl, response);
    }
  }
  
  //
  // Método para deletar todas as mensagens da fila
  private static async Task DeleteAllMessages(IAmazonSQS sqsClient, string qUrl)
  {
    Console.WriteLine($"\nPurging messages from queue\n  {qUrl}...");
    PurgeQueueResponse responsePurge = await sqsClient.PurgeQueueAsync(qUrl);
    Console.WriteLine($"HttpStatusCode: {responsePurge.HttpStatusCode}");
  }
  
  //
  // Método para ler uma mensagem em uma fila
  // Neste exemplo, pegando uma mensagem por vez
  private static async Task<ReceiveMessageResponse> GetMessage(
    IAmazonSQS sqsClient, string qUrl, int waitTime=0)
  {
    return await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest{
      QueueUrl=qUrl,
      MaxNumberOfMessages=MaxMessages,
      WaitTimeSeconds=waitTime
      // (Pode solicitar atributos, definir timeout da visibilidade, etc)
    });
  }

  //
  // Método para processar a mensagem
  // Neste exemplo, somente exibe a mensagem no console
  private static bool ProcessMessage(Message message)
  {
    Console.WriteLine($"\nO corpo da mensagem é {message.MessageId}:");
    Console.WriteLine($"{message.Body}");
    return true;
  }


  //
  // Método para deletar a mensagem de uma fila
  private static async Task DeleteMessage(
    IAmazonSQS sqsClient, Message message, string qUrl)
  {
    Console.WriteLine($"\nDeleting message {message.MessageId} from queue...");
    await sqsClient.DeleteMessageAsync(qUrl, message.ReceiptHandle);
  }
    
  //
  // Método para pegar o ARN (Amazon Resource Name) de uma fila
  private static async Task<string> GetQueueArn(IAmazonSQS sqsClient, string qUrl)
  {
    GetQueueAttributesResponse responseGetAtt = await sqsClient.GetQueueAttributesAsync(
      qUrl, new List<string>{QueueAttributeName.QueueArn});
    return responseGetAtt.QueueARN;
  }
    
  //
  // Método para exibir a lista de filas existentes
  private static async Task ShowQueues(IAmazonSQS sqsClient)
  {
    ListQueuesResponse responseList = await sqsClient.ListQueuesAsync("");
    Console.WriteLine();
    foreach(string qUrl in responseList.QueueUrls)
    {
      // Obtenha e mostre todos os atributos. Também pode obter um subconjunto.
      await ShowAllAttributes(sqsClient, qUrl);
    }
  }
    
  //
  // Método para exibir todos os atributos de uma fila
  private static async Task ShowAllAttributes(IAmazonSQS sqsClient, string qUrl)
  {
    var attributes = new List<string>{ QueueAttributeName.All };
    GetQueueAttributesResponse responseGetAtt =
      await sqsClient.GetQueueAttributesAsync(qUrl, attributes);
    Console.WriteLine($"Fila: {qUrl}");
    foreach(var att in responseGetAtt.Attributes)
      Console.WriteLine($"\t{att.Key}: {att.Value}");
  }
    
  private static void PrintHelp()
  {
    Console.WriteLine(
      "\nUsage: SQSCreateQueue -q <queue-name> [-d <dead-letter-queue>]" +
      " [-m <max-receive-count>] [-w <wait-time>]" +
      "\n  -q, --queue-name: The name of the queue you want to create." +
      "\n  -d, --dead-letter-queue: The URL of an existing queue to be used as the dead-letter queue."+
      "\n      If this argument isn't supplied, a new dead-letter queue will be created." +
      "\n  -m, --max-receive-count: The value for maxReceiveCount in the RedrivePolicy of the queue." +
      $"\n      Default is {MaxReceiveCount}." +
      "\n  -w, --wait-time: The value for ReceiveMessageWaitTimeSeconds of the queue for long polling." +
      $"\n      Default is {ReceiveMessageWaitTime}.");
  }
}
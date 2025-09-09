public class TradingDataStreamClient(string serverAddress)
{
private readonly string _serverAddress = serverAddress;
private NetworkStream _stream;
private Thread _receiveThread;
public event EventHandler<ProtobufMarketData> OnMarketDataReceived;

    public void StartStreaming()
{
    // Connect to your protobuf stream source
    var parts = _serverAddress.Split(':');
    var tcpClient = new TcpClient(parts[0], int.Parse(parts[1]));
    _stream = tcpClient.GetStream();
    
    _receiveThread = new Thread(ReceiveLoop) { IsBackground = true };
    _receiveThread.Start();
}

private void ReceiveLoop()
{
    var buffer = ArrayPool<byte>.Shared.Rent(65536);
    try
    {
        while (_stream != null && _stream.CanRead)
        {
            // Read message length prefix (assuming length-prefixed messages)
            var lengthBuffer = new byte[4];
            _stream.Read(lengthBuffer, 0, 4);
            var messageLength = BitConverter.ToInt32(lengthBuffer, 0);
            
            // Read the protobuf message
            var bytesRead = 0;
            while (bytesRead < messageLength)
            {
                bytesRead += _stream.Read(buffer, bytesRead, messageLength - bytesRead);
            }

                // Deserialize protobuf message
                using var memoryStream = new MemoryStream(buffer, 0, messageLength);
                var data = ProtobufMarketData.Parser.ParseFrom(memoryStream);
                OnMarketDataReceived?.Invoke(this, data);
            }
    }
    finally
    {
        ArrayPool<byte>.Shared.Return(buffer);
    }
}
}


public class ProtobufStreamProcessor : IDisposable
{
    private readonly TradingDataStreamClient _streamClient;
    private readonly ConcurrentQueue<ProtobufMarketData> _messageQueue;
    private readonly Timer _processingTimer;
    private readonly CancellationTokenSource _cancellationTokenSource;

    public event EventHandler<TheoBar> OnTheoBarReceived;

    public ProtobufStreamProcessor(string serverAddress)
    {
        _messageQueue = new ConcurrentQueue<ProtobufMarketData>();
        _cancellationTokenSource = new CancellationTokenSource();
        _streamClient = new TradingDataStreamClient(serverAddress);

        // Process queued messages every 500ms to maintain timing
        _processingTimer = new Timer(ProcessMessageBatch, null, 0, 500);

        _streamClient.OnMarketDataReceived += OnProtobufDataReceived;
    }

    private void OnProtobufDataReceived(object sender, ProtobufMarketData data)
    {
        // Queue message for batch processing to avoid blocking stream
        _messageQueue.Enqueue(data);
    }

    private void ProcessMessageBatch(object state)
    {
        var processedCount = 0;
        var stopwatch = Stopwatch.StartNew();

        // Process messages with time budget (leave 50ms buffer)
        while (_messageQueue.TryDequeue(out var protobufData) &&
               stopwatch.ElapsedMilliseconds < 450 &&
               processedCount < 1000) // Max batch size
        {
            try
            {
                var theoBar = ConvertToTheoBar(protobufData);
                OnTheoBarReceived?.Invoke(this, theoBar);
                processedCount++;
            }
            catch (Exception ex)
            {
                Log.Error($"Error processing protobuf data: {ex.Message}");
            }
        }
    }

    private TheoBar ConvertToTheoBar(ProtobufMarketData data)
    {
        var symbol = Symbol.CreateOption(data.UnderlyingSymbol, Market.USA,
                                        OptionStyle.European, data.OptionRight,
                                        data.Strike, data.Expiry);

        var bid = new Bar(data.BidPrice, data.BidPrice, data.BidPrice, data.BidPrice);
        var ask = new Bar(data.AskPrice, data.AskPrice, data.AskPrice, data.AskPrice);

        var greeks = new EnhancedGreeks(
            data.Delta, data.Gamma, data.Vega, data.Theta, data.Rho
        );

        return new TheoBar(
            data.Timestamp.ToDateTime(),
            symbol,
            bid, data.BidSize,
            ask, data.AskSize,
            data.TheoreticalValue,
            greeks,
            data.ImpliedVolatility,
            data.OpenInterest
        );
    }
}
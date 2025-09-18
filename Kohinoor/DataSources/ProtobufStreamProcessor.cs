using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using QuantConnect;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using Kohinoor.Data;
using Kohinoor.Protos;  // For OptionTheo
using QuantConnect.Logging;

namespace Kohinoor.DataSources
{
    // Processes protobuf stream messages into TheoBar instances
    public class ProtobufStreamProcessor : IDisposable
    {
        private readonly TradingDataStreamClient _streamClient;
        private readonly ConcurrentQueue<OptionTheo> _messageQueue;
        private readonly Timer _processingTimer;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public event EventHandler<TheoBar> OnTheoBarReceived;
        public bool IsConnected => _streamClient?.IsConnected ?? false;

        public void StartStreaming()
        {
            Log.Trace("ProtobufStreamProcessor: Starting stream");
            _streamClient.StartStreaming();
        }

        public ProtobufStreamProcessor(string serverAddress)
        {
            _messageQueue = new ConcurrentQueue<OptionTheo>();
            _cancellationTokenSource = new CancellationTokenSource();
            _streamClient = new TradingDataStreamClient(serverAddress);
            // Process queued messages every 500ms to maintain timing
            _processingTimer = new Timer(ProcessMessageBatch, null, 0, 500);

            _streamClient.OnMarketDataReceived += OnProtobufDataReceived;
        }

        public void Dispose()
        {
            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.Dispose();
            _processingTimer?.Dispose();
            _streamClient?.Dispose();
        }

        private void OnProtobufDataReceived(object sender, OptionTheo data)
        {
            // Queue message for batch processing to avoid blocking stream
            _messageQueue.Enqueue(data);
        }

        private void ProcessMessageBatch(object state)
        {
            var processedCount = 0;
            var stopwatch = Stopwatch.StartNew();

            while (_messageQueue.TryDequeue(out var optionTheo) &&
                stopwatch.ElapsedMilliseconds < 450 &&
                processedCount < 1000)
            {
                try
                {
                    // Now returns a list (call and put)
                    var theoBars = ConvertToTheoBars(optionTheo);
                    foreach (var theoBar in theoBars)
                    {
                        // Log.Debug($"Emitting TheoBar: {theoBar}");
                        OnTheoBarReceived?.Invoke(this, theoBar);
                    }
                    processedCount++;
                }
                catch (Exception ex)
                {
                    Log.Error($"Error processing protobuf data: {ex.Message}");
                }
            }
        }
        private List<TheoBar> ConvertToTheoBars(OptionTheo data)
        {
            var result = new List<TheoBar>();
            
            // Convert nanosecond epoch to DateTime
            var timestamp = DateTimeOffset.FromUnixTimeMilliseconds((long)(data.FrameworkTime / 1_000_000)).DateTime;
            var expiry = DateTimeOffset.FromUnixTimeMilliseconds((long)(data.Expiration / 1_000_000)).DateTime;

            // Create CALL TheoBar
            var callSymbol = Symbol.CreateOption(data.Underlying, Market.USA, 
                                                OptionStyle.European, OptionRight.Call, 
                                                (decimal)data.Strike, expiry);
            // var callSymbol = QuantConnect.Symbol.CreateBase("TestSymbol1", Market.USA);
            // var callSymbol = Symbol.Create("TestSymbol1", SecurityType.Base, Market.USA);

            var callBid = new Bar((decimal)data.BidPriceCall, (decimal)data.BidPriceCall, 
                                (decimal)data.BidPriceCall, (decimal)data.BidPriceCall);
            var callAsk = new Bar((decimal)data.AskPriceCall, (decimal)data.AskPriceCall, 
                                (decimal)data.AskPriceCall, (decimal)data.AskPriceCall);
            
            // Greeks is abstract - create instance properly
            var callGreeks = new KohinoorGreeks(
                delta: (decimal)data.FitDeltaCall,
                gamma: (decimal)data.FitGammaCall,
                vega: (decimal)data.FitVegaCall,
                theta: (decimal)data.FitThetaCall
            );  
            
            var callTheoBar = new TheoBar(
                timestamp,
                callSymbol,
                callBid, data.BidQuantityCall,
                callAsk, data.AskQuantityCall,
                (decimal)data.EstPriceCall,  // Theoretical value
                callGreeks,
                (decimal)data.Vol,  // Implied volatility
                0  // Open interest - not in proto
            );
            
            result.Add(callTheoBar);

            // Create PUT TheoBar (similar structure)
            var putSymbol = Symbol.CreateOption(data.Underlying, Market.USA, 
                                            OptionStyle.European, OptionRight.Put, 
                                            (decimal)data.Strike, expiry);
            // var putSymbol = QuantConnect.Symbol.CreateBase("TheoBar", "TestSymbol2", Market.USA);
            // var putSymbol = Symbol.Create("TestSymbol2", SecurityType.Base, Market.USA);

            var putBid = new Bar((decimal)data.BidPricePut, (decimal)data.BidPricePut, 
                                (decimal)data.BidPricePut, (decimal)data.BidPricePut);
            var putAsk = new Bar((decimal)data.AskPricePut, (decimal)data.AskPricePut, 
                                (decimal)data.AskPricePut, (decimal)data.AskPricePut);
            
            var putGreeks = new KohinoorGreeks( 
                delta : (decimal)data.FitDeltaPut,
                gamma : (decimal)data.FitGammaPut,
                vega : (decimal)data.FitVegaPut,
                theta : (decimal)data.FitThetaPut
            );

            var putTheoBar = new TheoBar(
                timestamp,
                putSymbol,
                putBid, data.BidQuantityPut,
                putAsk, data.AskQuantityPut,
                (decimal)data.EstPricePut,  // Theoretical value
                putGreeks,
                (decimal)data.Vol,  // Implied volatility
                0  // Open interest
            );
            
            result.Add(putTheoBar);
            
            return result;
        }
    }
}

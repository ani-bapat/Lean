// Version 1

// using System;
// using System.Collections.Concurrent;
// using System.Collections.Generic;
// using System.Threading;
// using QuantConnect;
// using QuantConnect.Data;
// using QuantConnect.Interfaces;
// using QuantConnect.Lean.Engine.DataFeeds;
// using QuantConnect.Packets;
// using QuantConnect.Data.Consolidators;
// using Kohinoor.Data;
// using Kohinoor.DataSources;
// using QuantConnect.Logging;
// using QuantConnect.Configuration;

// namespace Kohinoor.DataFeeds 
// {
//     public class OptionsDataQueueHandler : IDataQueueHandler, IDisposable
//     {
//         private readonly ProtobufStreamProcessor _streamProcessor;
//         private readonly ConcurrentDictionary<Symbol, SubscriptionDataConfig> _subscriptions;
//         // private readonly ConcurrentDictionary<Symbol, IDataConsolidator> _consolidators;
//         private readonly ConcurrentDictionary<Symbol, Queue<TheoBar>> _dataQueues;

//         public bool IsConnected => _streamProcessor != null && _streamProcessor.IsConnected;
        
//         // Implement Dispose
//         public void Dispose()
//         {
//             _streamProcessor?.Dispose();
            
//             // foreach (var consolidator in _consolidators.Values)
//             // {
//             //     consolidator?.Dispose();
//             // }
//             // _consolidators.Clear();
//             _subscriptions.Clear();
//             _dataQueues.Clear();
//         }

//         public OptionsDataQueueHandler()
//         {
//             Console.WriteLine("OptionsDataQueueHandler: Constructor called");
//             Log.Trace("OptionsDataQueueHandler: Initializing...");
            
//             _subscriptions = new ConcurrentDictionary<Symbol, SubscriptionDataConfig>();
//             // _consolidators = new ConcurrentDictionary<Symbol, IDataConsolidator>();
//             _dataQueues = new ConcurrentDictionary<Symbol, Queue<TheoBar>>();
            
//             var theo_server = Config.Get("theo-server", "localhost:50051");
//             Log.Trace($"OptionsDataQueueHandler: Connecting to {theo_server}");
            
//             try
//             {
//                 _streamProcessor = new ProtobufStreamProcessor(theo_server);
//                 _streamProcessor.OnTheoBarReceived += OnRawTheoBarReceived;
//                 Log.Trace("OptionsDataQueueHandler: Successfully initialized");
//             }
//             catch (Exception ex)
//             {
//                 Log.Error($"OptionsDataQueueHandler: Failed to initialize - {ex.Message}");
//                 throw;
//             }
//         }        

//         public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, 
//                                             EventHandler newDataAvailableHandler)
//         {
//             _subscriptions.TryAdd(dataConfig.Symbol, dataConfig);
//             Log.Debug($"Received data config {dataConfig}");
//             // Create consolidator for this symbol with the specified resolution
//             // var consolidator = new TheoBarConsolidator(dataConfig.Resolution.ToTimeSpan());
//             // consolidator.DataConsolidated += (sender, consolidated) =>
//             // {
//             //     var queue = _dataQueues.GetOrAdd(dataConfig.Symbol, _ => new Queue<TheoBar>());
//             //     lock (queue)
//             //     {
//             //         queue.Enqueue((TheoBar)consolidated);
//             //     }
//             //     newDataAvailableHandler?.Invoke(this, EventArgs.Empty);
//             // };
            
//             // _consolidators.TryAdd(dataConfig.Symbol, consolidator);
//             _dataQueues.TryAdd(dataConfig.Symbol, new Queue<TheoBar>());
//             // Log.Debug($"Consolidator {_consolidators}");
//             return GetDataEnumerator(dataConfig.Symbol);
//         }
        
//         private void OnRawTheoBarReceived(object sender, TheoBar theoBar)
//         {
//             Log.Debug($"Received RawTheoBar: {theoBar}");
//             // Send raw data to appropriate consolidator
//             if (_consolidators.TryGetValue(theoBar.Symbol, out var consolidator))
//             {
//                 consolidator.Update(theoBar);
//             }
//         }
        
//         private IEnumerator<BaseData> GetDataEnumerator(Symbol symbol)
//         {
//             while (true)
//             {
//                 if (_dataQueues.TryGetValue(symbol, out var queue))
//                 {
//                     lock (queue)
//                     {
//                         while (queue.Count > 0)
//                         {
//                             yield return queue.Dequeue();
//                         }
//                     }
//                 }
//                 Thread.Sleep(10); // Small delay to prevent tight loop
//             }
//         }
        
//         public void Unsubscribe(SubscriptionDataConfig dataConfig)
//         {
//             _subscriptions.TryRemove(dataConfig.Symbol, out _);
//             if (_consolidators.TryRemove(dataConfig.Symbol, out var consolidator))
//             {
//                 consolidator.Dispose();
//             }
//             _dataQueues.TryRemove(dataConfig.Symbol, out _);
//         }
        
//         public void SetJob(LiveNodePacket job)
//         {
//             // Configure any job-specific parameters
//             // _streamProcessor.StartStreaming();
//         }
//     }
// }

// Version 2

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Packets;
using QuantConnect.Data.Consolidators;
using Kohinoor.Data;
using Kohinoor.DataSources;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using System.IO;
using System.ComponentModel.Composition;  // <-- Add this line

namespace Kohinoor.DataFeeds
{
    [InheritedExport(typeof(IDataQueueHandler))]
    public class OptionsDataQueueHandler : IDataQueueHandler, IDisposable
    {
        private readonly ProtobufStreamProcessor _streamProcessor;
        private readonly ConcurrentDictionary<string, SubscriptionDataConfig> _contractSubscriptions;
        private readonly ConcurrentDictionary<string, EventHandler> _eventHandlers;
        private readonly ConcurrentDictionary<string, Queue<TheoBar>> _dataQueues;

        public bool IsConnected => _streamProcessor != null && _streamProcessor.IsConnected;

        public void Dispose()
        {
            _streamProcessor?.Dispose();
            _contractSubscriptions.Clear();
            _eventHandlers.Clear();
            _dataQueues.Clear();
        }

        public OptionsDataQueueHandler()
        {
            Console.WriteLine("OptionsDataQueueHandler: Constructor called");
            Log.Trace("OptionsDataQueueHandler: Initializing...");

            _contractSubscriptions = new ConcurrentDictionary<string, SubscriptionDataConfig>();
            _eventHandlers = new ConcurrentDictionary<string, EventHandler>();
            _dataQueues = new ConcurrentDictionary<string, Queue<TheoBar>>();

            var theo_server = Config.Get("theo-server", "localhost:50051");
            Log.Trace($"OptionsDataQueueHandler: Connecting to {theo_server}");

            try
            {
                _streamProcessor = new ProtobufStreamProcessor(theo_server);
                _streamProcessor.OnTheoBarReceived += OnRawTheoBarReceived;
                Log.Trace("OptionsDataQueueHandler: Successfully initialized");
            }
            catch (Exception ex)
            {
                Log.Error($"OptionsDataQueueHandler: Failed to initialize - {ex.Message}");
                throw;
            }
        }

        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig,
                                            EventHandler newDataAvailableHandler)
        {
            Log.Debug($"Subscribe called for {dataConfig.Symbol} with resolution {dataConfig.Resolution}");

            // Start streaming
            if (!_streamProcessor.IsConnected)
            {
                _streamProcessor.StartStreaming();
            }

            if (dataConfig == null)
            {
                Log.Error("Subscribe called with null dataConfig");
                throw new ArgumentNullException(nameof(dataConfig));
            }

            if (newDataAvailableHandler == null)
            {
                Log.Error("Subscribe called with null newDataAvailableHandler");
                throw new ArgumentNullException(nameof(newDataAvailableHandler));
            }

            Log.Debug($"Subscribe called for {dataConfig.Symbol} with resolution {dataConfig.Resolution}");


            if ((dataConfig.Symbol.SecurityType == SecurityType.Option || dataConfig.Symbol.SecurityType == SecurityType.IndexOption) && dataConfig.Symbol.HasUnderlying)
            {
                // Universe subscription
                Log.Debug($"Subscribing to options universe for underlying {dataConfig.Symbol.Underlying}");

                var universe = new KohinoorOptionUniverse()
                {
                    Symbol = dataConfig.Symbol,
                    Time = DateTime.Now
                };
                var file_source = universe.GetSource(dataConfig, DateTime.Today, true);
                Log.Debug($"Universe source: {file_source.Source}");

                _eventHandlers.TryAdd(dataConfig.Symbol.Value, newDataAvailableHandler);

                return GetKohinoorOptionUniverseEnumerator(dataConfig, universe);
            }

            else
            {
                // Individual contract subscription
                // Store subscription info and event handler
                _contractSubscriptions.TryAdd(dataConfig.Symbol.Value, dataConfig);
                _eventHandlers.TryAdd(dataConfig.Symbol.Value, newDataAvailableHandler);
                _dataQueues.TryAdd(dataConfig.Symbol.Value, new Queue<TheoBar>());

                Log.Debug($"Successfully subscribed to {dataConfig.Symbol} with {dataConfig.Symbol.Value} and string {dataConfig.Symbol.ToString()} and underlying {dataConfig.Symbol.Underlying.ToString()}, {dataConfig.Symbol.Underlying.Value}. Total subscriptions: {_contractSubscriptions.Count}");

                return GetDataEnumerator(dataConfig.Symbol.Value);
            }
        }

        private void OnRawTheoBarReceived(object sender, TheoBar theoBar)
        {
            var underlying_key = theoBar.Symbol.Value;
            Log.Debug($"Received RawTheoBar: {theoBar.Symbol} with underlying {underlying_key} at {theoBar.Time}");
            // Check if we have subscribers for this symbol
            if (_contractSubscriptions.ContainsKey(underlying_key))
            {
                // Add to queue
                if (_dataQueues.TryGetValue(underlying_key, out var queue))
                {
                    lock (queue)
                    {
                        queue.Enqueue(theoBar);
                    }

                    // Notify that new data is available
                    if (_eventHandlers.TryGetValue(underlying_key, out var handler))
                    {
                        Log.Debug($"Invoking event handler for {underlying_key}");
                        handler?.Invoke(this, EventArgs.Empty);
                    }
                }
            }
            else
            {
                // Log that we received data for unsubscribed symbol (for debugging)

                Log.Debug($"Received data for unsubscribed symbol: {theoBar.Symbol}");
            }
        }

        private IEnumerator<BaseData> GetDataEnumerator(string symbol)
        {
            while (true)
            {
                if (_dataQueues.TryGetValue(symbol, out var queue))
                {
                    lock (queue)
                    {
                        while (queue.Count > 0)
                        {
                            var theoBar = queue.Dequeue();
                            Log.Debug($"Yielding TheoBar for {symbol}: {theoBar.Close} at {theoBar.Time}");
                            yield return theoBar;
                        }
                    }
                }
                Thread.Sleep(1); // Small delay to prevent tight loop
            }
        }

        private IEnumerator<BaseData> GetKohinoorOptionUniverseEnumerator(SubscriptionDataConfig dataConfig, KohinoorOptionUniverse universe)
        {
            while (true)
            {
                var fileSource = universe.GetSource(dataConfig, DateTime.Today, true);
                if (File.Exists(fileSource.Source))
                {
                    using var stream = File.OpenRead(fileSource.Source);
                    using var reader = new StreamReader(stream);

                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (string.IsNullOrWhiteSpace(line)) continue;

                        var data = universe.Reader(dataConfig, line, DateTime.Today, true);
                        if (data != null)
                        {
                            Log.Debug($"Yielding universe data for {dataConfig.Symbol}: {data}");
                            yield return data;
                        }
                    }
                }
                Thread.Sleep(1000); // Wait before checking for new data
            }
        }

        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            Log.Debug($"Unsubscribing from {dataConfig.Symbol}");

            _contractSubscriptions.TryRemove(dataConfig.Symbol.Value, out _);
            _eventHandlers.TryRemove(dataConfig.Symbol.Value, out _);
            _dataQueues.TryRemove(dataConfig.Symbol.Value, out _);

            Log.Debug($"Successfully unsubscribed from {dataConfig.Symbol}. Remaining subscriptions: {_contractSubscriptions.Count}");
        }

        public void SetJob(LiveNodePacket job)
        {
            Log.Debug("SetJob called - starting stream processor if needed");
            // You might want to start streaming here
            // _streamProcessor.StartStreaming();
        }
    }
}

public class KohinoorOptionUniverse : OptionUniverse
{
    public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
    {
        var path = "/Lean/Data/test_option_contracts_" + config.Symbol.Underlying.Value + ".csv";
        return new SubscriptionDataSource(path, SubscriptionTransportMedium.LocalFile, FileFormat.Csv);
    }

    // use the default Reader from OptionUniverse
}

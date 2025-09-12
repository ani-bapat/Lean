using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Securities;

namespace Kohinoor.Data
{
    public class TheoBar : QuoteBar
    {
        // Standard QuoteBar contains Bid/Ask OHLC data

        // Extended properties for options with Greeks
        public decimal TheoreticalValue { get; set; }
        public decimal ImpliedVolatility { get; set; }
        public Greeks Greeks { get; set; }
        public decimal OpenInterest { get; set; }
        public decimal Forward { get; set; }
        public decimal Discount { get; set; }
        public decimal TradingTimeToExpiry { get; set; }

        // VIX/SPX specific properties
        public decimal Settlement { get; set; }
        public decimal Moneyness { get; set; }

        public TheoBar(DateTime time, Symbol symbol, IBar bid, decimal lastBidSize,
                    IBar ask, decimal lastAskSize, decimal theo, Greeks greeks,
                    decimal iv, decimal oi, TimeSpan? period = null)
            : base(time, symbol, bid, lastBidSize, ask, lastAskSize, period)
        {
            TheoreticalValue = theo;
            Greeks = greeks;
            ImpliedVolatility = iv;
            OpenInterest = oi;
            Value = theo; // Use theoretical value as primary value
        }

        public override BaseData Clone()
        {
            return new TheoBar(Time, Symbol, Bid, LastBidSize, Ask, LastAskSize,
                            TheoreticalValue, Greeks, ImpliedVolatility, OpenInterest, Period)
            {
                EndTime = EndTime,
                Forward = Forward,
                Discount = Discount,
                Settlement = Settlement,
                Moneyness = Moneyness,
                TradingTimeToExpiry = TradingTimeToExpiry
            };
        }

        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config,
                                                        DateTime date, bool isLiveMode)
        {
            if (isLiveMode)
            {
                // Live mode: data comes from IDataQueueHandler (protobuf stream)
                return new SubscriptionDataSource("", SubscriptionTransportMedium.LiveStream);
            }

            // Historical mode: read from CSV files or Iceberg once implemented
            return GetHistoricalDataSource(config, date);
        }

        private SubscriptionDataSource GetHistoricalDataSource(SubscriptionDataConfig config,
                                                            DateTime date)
        {
            // Your file naming convention
            var source = $"{config.Symbol.Value.ToLower()}_theobars_{date:yyyyMMdd}.csv";
            var dataPath = Path.Combine(Globals.DataFolder,
                                        config.Symbol.Underlying.ToLower() + "_options",
                                        date.Year.ToString(),
                                        date.Month.ToString("00"),
                                        date.Day.ToString("00"),
                                        source);

            return new SubscriptionDataSource(dataPath,
                SubscriptionTransportMedium.LocalFile,
                FileFormat.Csv);
            
            // TODO: Implement Iceberg data source
        }

    }
}
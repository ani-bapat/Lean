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
        public KohinoorGreeks Greeks { get; set; }
        public decimal OpenInterest { get; set; }
        public decimal Forward { get; set; }
        public decimal Discount { get; set; }
        public decimal TradingTimeToExpiry { get; set; }

        // VIX/SPX specific properties
        public decimal Settlement { get; set; }
        public decimal Moneyness { get; set; }

        public TheoBar()
        {
            Symbol = Symbol.Empty;
            Time = new DateTime();
            Bid = new Bar();
            Ask = new Bar();
            Value = 0;
            Period = QuantConnect.Time.OneMinute;
            DataType = MarketDataType.QuoteBar;
            
            // Initialize TheoBar-specific properties
            TheoreticalValue = 0;
            ImpliedVolatility = 0;
            Greeks = new KohinoorGreeks(0, 0, 0, 0);
            OpenInterest = 0;
            Forward = 0;
            Discount = 0;
            TradingTimeToExpiry = 0;
            Settlement = 0;
            Moneyness = 0;
        }

        public TheoBar(DateTime time, Symbol symbol, IBar bid, decimal lastBidSize,
                    IBar ask, decimal lastAskSize, decimal theo, KohinoorGreeks greeks,
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
                return new SubscriptionDataSource("", SubscriptionTransportMedium.Streaming);
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
                                        config.Symbol.Underlying.Value.ToLower() + "_options",
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

    public class KohinoorGreeks : Greeks
    {
        public override decimal Delta { get; }
        public override decimal Gamma { get; }
        public override decimal Vega { get; }
        public override decimal Theta { get; }
        public override decimal Rho { get; }
        public override decimal Lambda { get; }

        public KohinoorGreeks(decimal delta, decimal gamma, decimal vega, decimal theta)
        {
            Delta = delta;
            Gamma = gamma;
            Vega = vega;
            Theta = theta;
            Rho = 0;
            Lambda = 0;
        }
    }
}
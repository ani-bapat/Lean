using System;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using Kohinoor.Protos;

namespace Kohinoor.Data
{
    /// <summary>
    /// Custom data type for receiving TheoBar data from Kohinoor
    /// </summary>
    public class TheoBarData : BaseData
    {
        /// <summary>
        /// Bid TradeBar for call option
        /// </summary>
        public TradeBar Bid { get; set; }

        /// <summary>
        /// Ask TradeBar for call option
        /// </summary>
        public TradeBar Ask { get; set; }

        /// <summary>
        /// Bid TradeBar for put option
        /// </summary>
        public TradeBar BidPut { get; set; }

        /// <summary>
        /// Ask TradeBar for put option
        /// </summary>
        public TradeBar AskPut { get; set; }

        /// <summary>
        /// Last price of the option
        /// </summary>
        public decimal LastPrice { get; set; }

        /// <summary>
        /// Theoretical value of the option
        /// </summary>
        public decimal TheoreticalValue { get; set; }

        /// <summary>
        /// Implied volatility
        /// </summary>
        public decimal ImpliedVolatility { get; set; }

        /// <summary>
        /// Underlying asset price
        /// </summary>
        public decimal UnderlyingPrice { get; set; }

        /// <summary>
        /// Greeks for call option
        /// </summary>
        public KohinoorGreeks GreeksCall { get; set; }

        /// <summary>
        /// Greeks for put option
        /// </summary>
        public KohinoorGreeks GreeksPut { get; set; }

        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            // Return the URL or file path to your data source
            return new SubscriptionDataSource("tcp://localhost:50051", SubscriptionTransportMedium.Streaming);
        }

        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            // Parse protobuf OptionTheo message into a TheoBar object
            try
            {
                byte[] protoBytes;
                
                // Assuming 'line' contains the serialized protobuf bytes
                if (line is string)
                {
                    protoBytes = System.Text.Encoding.GetEncoding("ISO-8859-1").GetBytes(line);
                }
                else
                {
                    // Handle other input types as needed
                    return null;
                }

                // Parse the protobuf message
                var optionTheo = OptionTheo.Parser.ParseFrom(protoBytes);

                // Create TheoBarData from OptionTheo data
                var theoBarData = new TheoBarData
                {
                    Symbol = config.Symbol,
                    // Convert framework_time (nanoseconds) to DateTime
                    Time = DateTimeOffset.FromUnixTimeMilliseconds((long)(optionTheo.FrameworkTime / 1000000)).DateTime
                };

                // Create bid/ask TradeBars for call option
                theoBarData.Bid = new TradeBar(
                    theoBarData.Time,
                    config.Symbol,
                    (decimal)optionTheo.BidPriceCall, // Open
                    (decimal)optionTheo.BidPriceCall, // High
                    (decimal)optionTheo.BidPriceCall, // Low
                    (decimal)optionTheo.BidPriceCall, // Close
                    (decimal)optionTheo.BidQuantityCall // Volume
                );

                theoBarData.Ask = new TradeBar(
                    theoBarData.Time,
                    config.Symbol,
                    (decimal)optionTheo.AskPriceCall, // Open
                    (decimal)optionTheo.AskPriceCall, // High
                    (decimal)optionTheo.AskPriceCall, // Low
                    (decimal)optionTheo.AskPriceCall, // Close
                    (decimal)optionTheo.AskQuantityCall // Volume
                );

                // Create bid/ask TradeBars for put option
                theoBarData.BidPut = new TradeBar(
                    theoBarData.Time,
                    config.Symbol,
                    (decimal)optionTheo.BidPricePut, // Open
                    (decimal)optionTheo.BidPricePut, // High
                    (decimal)optionTheo.BidPricePut, // Low
                    (decimal)optionTheo.BidPricePut, // Close
                    (decimal)optionTheo.BidQuantityPut // Volume
                );

                theoBarData.AskPut = new TradeBar(
                    theoBarData.Time,
                    config.Symbol,
                    (decimal)optionTheo.AskPricePut, // Open
                    (decimal)optionTheo.AskPricePut, // High
                    (decimal)optionTheo.AskPricePut, // Low
                    (decimal)optionTheo.AskPricePut, // Close
                    (decimal)optionTheo.AskQuantityPut // Volume
                );

                // Set TheoBarData specific fields from protobuf
                theoBarData.LastPrice = (decimal)optionTheo.EstPriceCall;
                theoBarData.TheoreticalValue = (decimal)optionTheo.FitPriceCall;
                theoBarData.ImpliedVolatility = (decimal)optionTheo.Vol;
                theoBarData.UnderlyingPrice = (decimal)optionTheo.Forward;

                // Set Greeks from protobuf
                theoBarData.GreeksCall = new KohinoorGreeks
                (
                    delta : (decimal)optionTheo.FitDeltaCall,
                    gamma : (decimal)optionTheo.FitGammaCall,
                    vega : (decimal)optionTheo.FitVegaCall,
                    theta : (decimal)optionTheo.FitThetaCall
                    // charm : (decimal)optionTheo.FitCharmCall,
                    // vanna : (decimal)optionTheo.FitVannaCall
                );

                // Set Greeks for put option from protobuf
                theoBarData.GreeksPut = new KohinoorGreeks
                (
                    delta : (decimal)optionTheo.FitDeltaPut,
                    gamma : (decimal)optionTheo.FitGammaPut,
                    vega : (decimal)optionTheo.FitVegaPut,
                    theta : (decimal)optionTheo.FitThetaPut
                    // charm : (decimal)optionTheo.FitCharmPut,
                    // vanna : (decimal)optionTheo.FitVannaPut
                );

                return theoBarData;
            }
            catch (Exception e)
            {
                // Log the error for debugging
                Console.WriteLine($"Error parsing OptionTheo protobuf: {e.Message}");
                return null;
            }
        }
    }
}

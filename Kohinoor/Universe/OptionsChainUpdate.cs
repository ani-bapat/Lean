using System;
using System.Collections.Generic;
using QuantConnect;
using Kohinoor.Data;

namespace Kohinoor.Universe
{
    public class OptionsChainUpdate : EventArgs
    {
        public string Underlying { get; set; }
        public List<Symbol> Contracts { get; set; }
        public TheoBar TheoBar { get; set; }
        public DateTime UpdateTime { get; set; }
        
        public OptionsChainUpdate()
        {
            Contracts = new List<Symbol>();
            UpdateTime = DateTime.UtcNow;
        }
    }
}
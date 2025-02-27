from AlgorithmImports import *
from scipy.stats import linregress
# endregion

class SwimmingBrownCaterpillar(QCAlgorithm):



    def Initialize(self):
        self.SetStartDate(2022, 1, 1)  # Set Start Date
        self.SetEndDate(2024, 1, 31)    # Set End Date
        self.SetCash(100000)            # Set Strategy Cash
        
        # List of ETF symbols to consider for pairs trading
        self.etf_symbols = [ 'SPY', 'DIA','IVV','VTI', 'QQQ', 'IWM', 'EFA', 'IEFA', 'EEM', 'VWO', 'AGG', 'VOO']
        for symbol in self.etf_symbols:
            self.AddEquity(symbol, Resolution.Daily)
        self.lookback = 50  # Lookback period for correlation calculation
        self.resolution = Resolution.Daily
        self.bb_window = 20  # Bollinger Bands window
        self.num_pairs = 3   # Number of pairs to select
        
        self.selected_pairs = []  # To store selected ETF pairs
        self.bb_dict = {}         # Placeholder for Bollinger Bands for each pair, keyed by pair tuple
        
        # Schedule the correlation analysis to run at the algorithm start and then every month
        self.Schedule.On(self.DateRules.MonthStart(), 
                         self.TimeRules.AfterMarketOpen("SPY", 5), 
                         self.SelectPairs)
    
    def SelectPairs(self):
        try:
            ''' Selects the top correlated pairs based on historical returns '''
            history = self.History(self.etf_symbols, self.lookback, self.resolution).close.unstack(level=0)
            returns = history.pct_change().dropna()
            
            correlations = returns.corr()
            pairs = []
            
            # Calculate correlations between each pair
            for i, symbol_i in enumerate(self.etf_symbols):
                for j in range(i+1, len(self.etf_symbols)):
                    symbol_j = self.etf_symbols[j]
                    corr = correlations.at[symbol_i, symbol_j]
                    pairs.append((symbol_i, symbol_j, corr))
            
            # Sort pairs based on correlation, select top N pairs
            pairs.sort(key=lambda x: x[2], reverse=True)
            selected_pairs_info = pairs[:self.num_pairs]
            
            self.selected_pairs = [(pair[0], pair[1]) for pair in selected_pairs_info]
            
            # Reinitialize Bollinger Bands for each selected pair
            for symbol_i, symbol_j in self.selected_pairs:
                self.AddEquity(symbol_i, self.resolution)
                self.AddEquity(symbol_j, self.resolution)
                self.bb_dict[(symbol_i, symbol_j)] = BollingerBands(self.bb_window, 2, MovingAverageType.Simple)
            
            self.Debug(f'Selected Pairs: {self.selected_pairs}')
        except Exception as e:
            self.Debug(f'Error retrieving historical data: {str(e)}')

    def OnData(self, data):
        '''Trading logic to be applied on selected pairs'''
        for symbol_i, symbol_j in self.selected_pairs:
            # Ensure we have data for both parts of the pair
            if not (data.ContainsKey(symbol_i) and data.ContainsKey(symbol_j)):
                continue
            
            price_i = data[symbol_i].Close
            price_j = data[symbol_j].Close
            
            # Calculate the spread or ratio
            spread = price_i - price_j
            
            # Update Bollinger Bands for the pair
            bb = self.bb_dict[(symbol_i, symbol_j)]
            bb.Update(self.Time, spread)
            
            # Check if Bollinger Bands are ready
            if not bb.IsReady:
                continue
            
            holdings_i = self.Portfolio[symbol_i].Quantity
            holdings_j = self.Portfolio[symbol_j].Quantity
            
            # Trading logic based on Bollinger Bands signals
            if spread < bb.LowerBand.Current.Value and (holdings_i <= 0 and holdings_j >= 0):
                # Spread is below the lower band: Buy i, Sell j
                self.SetHoldings(symbol_i, 0.5)  # Allocate half of the portfolio to i
                self.SetHoldings(symbol_j, -0.5)  # Short equal amount of j
                self.Debug(f"Buying {symbol_i} and selling {symbol_j} on {self.Time}")
            
            elif spread > bb.UpperBand.Current.Value and (holdings_i >= 0 and holdings_j <= 0):
                # Spread is above the upper band: Sell i, Buy j
                self.SetHoldings(symbol_i, -0.5)  # Short i
                self.SetHoldings(symbol_j, 0.5)  # Allocate half of the portfolio to j
                self.Debug(f"Selling {symbol_i} and buying {symbol_j} on {self.Time}")
            
            elif spread > bb.MiddleBand.Current.Value and holdings_i < 0 and holdings_j > 0:
                # Spread crossing back above the middle band from below: Close positions
                self.Liquidate(symbol_i)
                self.Liquidate(symbol_j)
                self.Debug(f"Closing positions for {symbol_i} and {symbol_j} on {self.Time}")
                
            elif spread < bb.MiddleBand.Current.Value and holdings_i > 0 and holdings_j < 0:
                # Spread crossing back below the middle band from above: Close positions
                self.Liquidate(symbol_i)
                self.Liquidate(symbol_j)
                self.Debug(f"Closing positions for {symbol_i} and {symbol_j} on {self.Time}")

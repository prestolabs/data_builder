import h5py
from data_builder.stock.reader.tw.product_info_reader import read_product_info

twse_interval_path = "/remote/iosg/data-2/buckets/feed.derived.interval_h5/navi/main/PT1M/TWSE/{date}/TWSE--ohlc.h5"
tpex_interval_path = "/remote/iosg/data-2/buckets/feed.derived.interval_h5/navi/main/PT1M/TPEX/{date}/TPEX--ohlc.h5"


class read_feed_interval_ohlc:
    def __init__(date):

        self.date = date
        rpi = read_product_info()
        twse_data = rpi.get_symbol_name(date)
        print(twse_data)
        self.symbol_list = twse_data
        # self.twse_symbol_list = 
        # self.tpex_symbol_list =
        self.twse_path = twse_interval_path.format(date=self.date)
        self.tpex_path = tpex_interval_path.format(date=self.date)

        with h5py.File(self.twse_path, "r") as h5file:
            self.twse_universe_data = h5file["universe"][:]  
            self.twse_universe_data = [item.decode("utf-8") for item in self.twse_universe_data]
            self.twse_open_data = h5file["OPEN_TRADE"][0, :]
            self.twse_high_data = h5file["HIGH_TRADE"][:]
            self.twse_low_data = h5file["LOW_TRADE"][:]
            self.twse_close_data = h5file["CLOSE_TRADE"][-1,:]
            self.timestamps = h5file["timestamps"][:]  

        with h5py.File(self.tpex_path, "r") as h5file:
            self.tpex_universe_data = h5file["universe"][:]  
            self.tpex_universe_data = [item.decode("utf-8") for item in self.tpex_universe_data]
            self.twse_open_data = h5file["OPEN_TRADE"][0, :]
            self.twse_high_data = h5file["HIGH_TRADE"][:]
            self.twse_low_data = h5file["LOW_TRADE"][:]
            self.twse_close_data = h5file["CLOSE_TRADE"][-1,:]  
            self.timestamps = h5file["timestamps"][:]  
            
    def get_open(self):
        pass
        



    def get_close(self):
        pass

    def get_high(self):
        high_array = np.full((1, len(symbol_list)), np.nan)
        path = twse_interval_path.format(date=self.date)

        twse_high_df = pd.DataFrame(self.twse_high_data, columns=self.twse_universe_data, index=pd.to_datetime(self.timestamps))
        twse_high_df.index = twse_high_df.index.date  
        twse_high_daily_df = twse_high_df.groupby(twse_high_df.index).max()  
        tpex_high_df = pd.DataFrame(self.tpex_high_data, columns=self.tpex_universe_data, index=pd.to_datetime(self.timestamps))
        tpex_high_df.index = tpex_high_df.index.date  
        tpex_high_daily_df = tpex_high_df.groupby(tpex_high_df.index).max()  
         

        combined_high = pd.concat([twse_high, tpex_high], axis=1)

        # Map to the symbol list and fill missing symbols with NaN
        combined_high = combined_high.reindex(columns=self.symbol_list, fill_value=np.nan)
        return combined_high





    def get_low(self):
        pass


def test():
    reader = read_feed_interval_ohlc()
    date = "20241120"
    twse_data = rpi.get_symbol_name(date)
    print(twse_data)
    print(len(twse_data))

if __name__ == "__main__":
    test()

import h5py
import numpy as np
import pandas as pd
from data_builder.stock.reader.tw.product_info_reader import read_product_info
from typing import Dict

base_path = "/remote/iosg/data-2/buckets/feed.derived.interval_h5/navi/main/PT1M"

twse_ohlc_interval_path = f"{base_path}/TWSE/{{date}}/TWSE--ohlc.h5"
tpex_ohlc_interval_path = f"{base_path}/TPEX/{{date}}/TPEX--ohlc.h5"
twse_volume_interval_path = f"{base_path}/TWSE/{{date}}/TWSE--volume.h5"
tpex_volume_interval_path = f"{base_path}/TPEX/{{date}}/TPEX--volume.h5"
twse_spread_interval_path = f"{base_path}/TWSE/{{date}}/TWSE--spread.h5"
tpex_spread_interval_path = f"{base_path}/TPEX/{{date}}/TPEX--spread.h5"
twse_vwap_interval_path = f"{base_path}/TWSE/{{date}}/TWSE--vwap.h5"
tpex_vwap_interval_path = f"{base_path}/TPEX/{{date}}/TPEX--vwap.h5"


class read_feed_interval_ohlc:
    def __init__(self, date: str):
        self.date: str = date
        rpi = read_product_info()
        twse_data: list[str] = rpi.get_symbol_name(date)
        self.symbol_list: list[str] = twse_data
        self.twse_symbol_list: list[str] = rpi.get_twse_symbol(date)
        self.tpex_symbol_list: list[str] = rpi.get_twse_symbol(date)
        self.twse_path: str = twse_ohlc_interval_path.format(date=self.date)
        self.tpex_path: str = tpex_ohlc_interval_path.format(date=self.date)

        with h5py.File(self.twse_path, "r") as h5file:
            self.twse_universe_data: list[str] = h5file["universe"][:]
            self.twse_universe_data: np.ndarray[np.float_] = [
                item.decode("utf-8") for item in self.twse_universe_data
            ]
            self.twse_open_trade_data: np.ndarray[np.float_] = h5file["OPEN_TRADE"][:]
            self.twse_open_mid_data: np.ndarray[np.float_] = h5file["OPEN_MID"][:]
            self.twse_high_trade_data: np.ndarray[np.float_] = h5file["HIGH_TRADE"][:]
            self.twse_high_mid_data: np.ndarray[np.float_] = h5file["HIGH_MID"][:]
            self.twse_low_trade_data: np.ndarray[np.float_] = h5file["LOW_TRADE"][:]
            self.twse_low_mid_data: np.ndarray[np.float_] = h5file["LOW_MID"][:]
            self.twse_close_trade_data: np.ndarray[np.float_] = h5file["CLOSE_TRADE"][:]
            self.twse_close_mid_data: np.ndarray[np.float_] = h5file["CLOSE_MID"][:]
            self.twse_timestamps: np.ndarray[np.int64] = h5file["timestamp"][:]

        with h5py.File(self.tpex_path, "r") as h5file:
            self.tpex_universe_data: list[str] = h5file["universe"][:]
            self.tpex_universe_data: np.ndarray[np.float_] = [
                item.decode("utf-8") for item in self.tpex_universe_data
            ]
            self.tpex_open_trade_data: np.ndarray[np.float_] = h5file["OPEN_TRADE"][:]
            self.tpex_open_mid_data: np.ndarray[np.float_] = h5file["OPEN_MID"][:]
            self.tpex_high_trade_data: np.ndarray[np.float_] = h5file["HIGH_TRADE"][:]
            self.tpex_high_mid_data: np.ndarray[np.float_] = h5file["HIGH_MID"][:]
            self.tpex_low_trade_data: np.ndarray[np.float_] = h5file["LOW_TRADE"][:]
            self.tpex_low_mid_data: np.ndarray[np.float_] = h5file["LOW_MID"][:]
            self.tpex_close_trade_data: np.ndarray[np.float_] = h5file["CLOSE_TRADE"][:]
            self.tpex_close_mid_data: np.ndarray[np.float_] = h5file["CLOSE_MID"][:]
            self.tpex_timestamps: np.ndarray[np.int64] = h5file["timestamp"][:]

        assert np.array_equal(self.tpex_timestamps, self.twse_timestamps), (
            "Timestamps mismatch: TPEX and TWSE timestamps are not equal."
        )
        self.timestamps: np.ndarray[np.int64] = self.tpex_timestamps

    def get_open_trade(self) -> pd.DataFrame:
        twse_open_trade_df: pd.DataFrame = pd.DataFrame(
            self.twse_open_trade_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_open_trade_df: pd.DataFrame = pd.DataFrame(
            self.tpex_open_trade_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_open_trade_daily_df: pd.DataFrame = twse_open_trade_df.groupby(
            twse_open_trade_df.index.date).apply(
                lambda df: df.apply(
                    lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan
                )
            )
        tpex_open_trade_daily_df: pd.DataFrame = tpex_open_trade_df.groupby(
            tpex_open_trade_df.index.date).apply(
                lambda df: df.apply(
                    lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan
                )
            )
        combined_open_trade: pd.DataFrame = pd.concat(
            [twse_open_trade_daily_df, tpex_open_trade_daily_df], axis=1
        )
        combined_open_trade = combined_open_trade.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        print(combined_open_trade)
        return combined_open_trade

    def get_open_mid(self) -> pd.DataFrame:
        twse_open_mid_df: pd.DataFrame = pd.DataFrame(
            self.twse_open_mid_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_open_mid_df: pd.DataFrame = pd.DataFrame(
            self.tpex_open_mid_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_open_mid_daily_df: pd.DataFrame = twse_open_mid_df.groupby(
            twse_open_mid_df.index.date).apply(
                lambda df: df.apply(
                    lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan
                )
            )
        tpex_open_mid_daily_df: pd.DataFrame = tpex_open_mid_df.groupby(
            tpex_open_mid_df.index.date).apply(
                lambda df: df.apply(
                    lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan
                )
            )
        combined_open_mid: pd.DataFrame = pd.concat(
            [twse_open_mid_daily_df, tpex_open_mid_daily_df], axis=1
        )
        combined_open_mid = combined_open_mid.reindex(columns=self.symbol_list, fill_value=np.nan)
        print(combined_open_mid)
        return combined_open_mid

    def get_close_trade(self) -> pd.DataFrame:
        twse_close_trade_df: pd.DataFrame = pd.DataFrame(
            self.twse_close_trade_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_close_trade_df: pd.DataFrame = pd.DataFrame(
            self.tpex_close_trade_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_close_trade_daily_df: pd.DataFrame = twse_close_trade_df.groupby(
            twse_close_trade_df.index.date).apply(
                lambda df: df.apply(
                    lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan
                )
            )
        tpex_close_trade_daily_df: pd.DataFrame = tpex_close_trade_df.groupby(
            tpex_close_trade_df.index.date).apply(
                lambda df: df.apply(
                    lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan
                )
            )
        combined_close_trade: pd.DataFrame = pd.concat(
            [twse_close_trade_daily_df, tpex_close_trade_daily_df], axis=1
        )
        combined_close_trade = combined_close_trade.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        print(combined_close_trade)
        return combined_close_trade

    def get_close_mid(self) -> pd.DataFrame:
        twse_close_mid_df: pd.DataFrame = pd.DataFrame(
            self.twse_close_mid_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_close_mid_df: pd.DataFrame = pd.DataFrame(
            self.tpex_close_mid_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_close_mid_daily_df: pd.DataFrame = twse_close_mid_df.groupby(
            twse_close_mid_df.index.date).apply(
            lambda df: df.apply(
                lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan
                )
            )
        tpex_close_mid_daily_df: pd.DataFrame = tpex_close_mid_df.groupby(
            tpex_close_mid_df.index.date).apply(
            lambda df: df.apply(
                lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan
                )
            )
        combined_close_mid: pd.DataFrame = pd.concat(
            [twse_close_mid_daily_df, tpex_close_mid_daily_df], axis=1
        )
        combined_close_mid = combined_close_mid.reindex(columns=self.symbol_list, fill_value=np.nan)
        print(combined_close_mid)
        return combined_close_mid

    def get_high_trade(self) -> pd.DataFrame:
        twse_high_trade_df: pd.DataFrame = pd.DataFrame(
            self.twse_high_trade_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_high_trade_df.index = twse_high_trade_df.index.date
        twse_high_trade_daily_df: pd.DataFrame = (
            twse_high_trade_df.groupby(twse_high_trade_df.index).max()
        )
        tpex_high_trade_df: pd.DataFrame = pd.DataFrame(
            self.tpex_high_trade_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_high_trade_df.index = tpex_high_trade_df.index.date
        tpex_high_trade_daily_df: pd.DataFrame = (
            tpex_high_trade_df.groupby(tpex_high_trade_df.index).max()
        )
        combined_high_trade: pd.DataFrame = pd.concat(
            [twse_high_trade_daily_df, tpex_high_trade_daily_df], axis=1
        )
        combined_high_trade = combined_high_trade.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        print(combined_high_trade)
        return combined_high_trade

    def get_high_mid(self) -> pd.DataFrame:
        twse_high_mid_df: pd.DataFrame = pd.DataFrame(
            self.twse_high_mid_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_high_mid_df.index = twse_high_mid_df.index.date
        twse_high_mid_daily_df: pd.DataFrame = (
            twse_high_mid_df.groupby(twse_high_mid_df.index).max()
        )
        tpex_high_mid_df: pd.DataFrame = pd.DataFrame(
            self.tpex_high_mid_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_high_mid_df.index = tpex_high_mid_df.index.date
        tpex_high_mid_daily_df: pd.DataFrame = (
            tpex_high_mid_df.groupby(tpex_high_mid_df.index).max()
        )
        combined_high_mid: pd.DataFrame = pd.concat(
            [twse_high_mid_daily_df, tpex_high_mid_daily_df], axis=1
        )
        combined_high_mid = combined_high_mid.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        print(combined_high_mid)
        return combined_high_mid

    def get_low_trade(self) -> pd.DataFrame:
        twse_low_trade_df: pd.DataFrame = pd.DataFrame(
            self.twse_low_trade_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_low_trade_df.index = twse_low_trade_df.index.date
        twse_low_trade_daily_df: pd.DataFrame = (
            twse_low_trade_df.groupby(twse_low_trade_df.index).min()
        )
        tpex_low_trade_df: pd.DataFrame = pd.DataFrame(
            self.tpex_low_trade_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_low_trade_df.index = tpex_low_trade_df.index.date
        tpex_low_trade_daily_df: pd.DataFrame = (
            tpex_low_trade_df.groupby(tpex_low_trade_df.index).min()
        )
        combined_low_trade: pd.DataFrame = pd.concat(
            [twse_low_trade_daily_df, tpex_low_trade_daily_df], axis=1
        )
        combined_low_trade = combined_low_trade.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        print(combined_low_trade)
        return combined_low_trade

    def get_low_mid(self) -> pd.DataFrame:
        twse_low_mid_df: pd.DataFrame = pd.DataFrame(
            self.twse_low_mid_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_low_mid_df.index = twse_low_mid_df.index.date
        twse_low_mid_daily_df: pd.DataFrame = (
            twse_low_mid_df.groupby(twse_low_mid_df.index).min()
        )
        tpex_low_mid_df: pd.DataFrame = pd.DataFrame(
            self.tpex_low_mid_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_low_mid_df.index = tpex_low_mid_df.index.date
        tpex_low_mid_daily_df: pd.DataFrame = (
            tpex_low_mid_df.groupby(tpex_low_mid_df.index).min()
        )
        combined_low_mid: pd.DataFrame = pd.concat(
            [twse_low_mid_daily_df, tpex_low_mid_daily_df], axis=1
        )
        combined_low_mid = combined_low_mid.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        print(combined_low_mid)
        return combined_low_mid

    def get_open_trade_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True)
            .tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_open_df: pd.DataFrame = pd.DataFrame(
            self.twse_open_trade_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_open_df: pd.DataFrame = pd.DataFrame(
            self.tpex_open_trade_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_open_df: pd.DataFrame = pd.concat(
            [twse_open_df, tpex_open_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFram = combined_open_df[
            (combined_open_df.index >= start_time) &
            (combined_open_df.index < end_time)
        ]
        resampled_data: pd.DataFram = trading_data.resample('10min').apply(
            lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan
        )
        resampled_data = resampled_data.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        return result_dict

    def get_open_mid_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True)
            .tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_open_df: pd.DataFrame = pd.DataFrame(
            self.twse_open_mid_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_open_df: pd.DataFrame = pd.DataFrame(
            self.tpex_open_mid_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_open_df: pd.DataFrame = pd.concat(
            [twse_open_df, tpex_open_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFrame = combined_open_df[
            (combined_open_df.index >= start_time) &
            (combined_open_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').apply(
            lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan
        )
        resampled_data = resampled_data.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict

    def get_close_trade_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_close_df: pd.DataFrame = pd.DataFrame(
            self.twse_close_trade_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_close_df: pd.DataFrame = pd.DataFrame(
            self.tpex_close_trade_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_close_df: pd.DataFrame = pd.concat(
            [twse_close_df, tpex_close_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFrame = combined_close_df[
            (combined_close_df.index >= start_time) &
            (combined_close_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').apply(
            lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan
        )
        resampled_data = resampled_data.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].tail(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict

    def get_close_mid_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_close_df: pd.DataFrame = pd.DataFrame(
            self.twse_close_mid_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_close_df: pd.DataFrame = pd.DataFrame(
            self.tpex_close_mid_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_close_df: pd.DataFrame = pd.concat(
            [twse_close_df, tpex_close_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFrame = combined_close_df[
            (combined_close_df.index >= start_time) &
            (combined_close_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').apply(
            lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan
        )
        resampled_data = resampled_data.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].tail(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict

    def get_low_trade_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_low_trade_df: pd.DataFrame = pd.DataFrame(
            self.twse_low_trade_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_low_trade_df: pd.DataFrame = pd.DataFrame(
            self.tpex_low_trade_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_low_df: pd.DataFrame = pd.concat(
            [twse_low_trade_df, tpex_low_trade_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFrame = combined_low_df[
            (combined_low_df.index >= start_time) &
            (combined_low_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').apply(
            lambda x: x.min() if not x.dropna().empty else np.nan
        )
        resampled_data = resampled_data.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict

    def get_low_mid_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_low_trade_df: pd.DataFrame = pd.DataFrame(
            self.twse_low_mid_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_low_trade_df: pd.DataFrame = pd.DataFrame(
            self.tpex_low_mid_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_low_df: pd.DataFrame = pd.concat(
            [twse_low_trade_df, tpex_low_trade_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        trading_data: pd.DataFrame = combined_low_df[
            (combined_low_df.index >= start_time) &
            (combined_low_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').apply(
            lambda x: x.min() if not x.dropna().empty else np.nan
        )
        resampled_data = resampled_data.reindex(columns=self.symbol_list, fill_value=np.nan)
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict

    def get_high_trade_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_high_trade_df: pd.DataFrame = pd.DataFrame(
            self.twse_high_trade_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_high_trade_df: pd.DataFrame = pd.DataFrame(
            self.tpex_high_trade_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_high_df: pd.DataFrame = pd.concat(
            [twse_high_trade_df, tpex_high_trade_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFrame = combined_high_df[
            (combined_high_df.index >= start_time) &
            (combined_high_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').apply(
            lambda x: x.min() if not x.dropna().empty else np.nan
        )
        resampled_data = resampled_data.reindex(columns=self.symbol_list, fill_value=np.nan)
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict

    def get_high_mid_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_high_mid_df: pd.DataFrame = pd.DataFrame(
            self.twse_high_mid_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_high_mid_df: pd.DataFrame = pd.DataFrame(
            self.tpex_high_mid_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_high_df: pd.DataFrame = pd.concat(
            [twse_high_mid_df, tpex_high_mid_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFrame = combined_high_df[
            (combined_high_df.index >= start_time) &
            (combined_high_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').apply(
            lambda x: x.max() if not x.dropna().empty else np.nan
        )
        resampled_data = resampled_data.reindex(
            columns=self.symbol_list,
            fill_value=np.nan
        )
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict


class read_feed_interval_volume:
    def __init__(self, date: str):
        self.date: str = date
        rpi = read_product_info()
        twse_data = rpi.get_symbol_name(date)
        self.symbol_list: list[str] = twse_data
        self.twse_symbol_list: list[str] = rpi.get_twse_symbol(date)
        self.tpex_symbol_list: list[str] = rpi.get_twse_symbol(date)
        self.twse_path: str = twse_volume_interval_path.format(date=self.date)
        self.tpex_path: str = tpex_volume_interval_path.format(date=self.date)

        with h5py.File(self.twse_path, "r") as h5file:
            self.twse_universe_data: list[str] = h5file["universe"][:]
            self.twse_universe_data = [item.decode("utf-8") for item in self.twse_universe_data]
            self.twse_volume_total_data: np.ndarray[np.float_] = h5file["VOLUME"][:]
            self.twse_volume_buy_data: np.ndarray[np.float_] = h5file["VOLUME_BUY"][:]
            self.twse_volume_sell_data: np.ndarray[np.float_] = h5file["VOLUME_SELL"][:]
            self.timestamps: np.ndarray[np.int64] = h5file["timestamp"][:]

        with h5py.File(self.tpex_path, "r") as h5file:
            self.tpex_universe_data: list[str] = h5file["universe"][:]
            self.tpex_universe_data = [item.decode("utf-8") for item in self.tpex_universe_data]
            self.tpex_volume_total_data: np.ndarray[np.float_] = h5file["VOLUME"][:]
            self.tpex_volume_buy_data: np.ndarray[np.float_] = h5file["VOLUME_BUY"][:]
            self.tpex_volume_sell_data: np.ndarray[np.float_] = h5file["VOLUME_SELL"][:]
            self.timestamps: np.ndarray[np.int64] = h5file["timestamp"][:]

    def _aggregate_daily_volume(
        self,
        twse_data: np.ndarray[np.float_],
        tpex_data: np.ndarray[np.float_]
        ) -> pd.DataFrame:

        twse_df: pd.DataFrame = pd.DataFrame(
            twse_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_df: pd.DataFrame = pd.DataFrame(
            tpex_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_daily: pd.DataFrame = twse_df.groupby(twse_df.index.date).sum()
        tpex_daily: pd.DataFrame = tpex_df.groupby(tpex_df.index.date).sum()
        combined_daily: pd.DataFrame = pd.concat([twse_daily, tpex_daily], axis=1)
        combined_daily = combined_daily.reindex(columns=self.symbol_list, fill_value=0)
        return combined_daily

    def get_volume_total(self) -> pd.DataFrame:
        return self._aggregate_daily_volume(
            self.twse_volume_total_data,
            self.tpex_volume_total_data
        )

    def get_volume_buy(self) -> pd.DataFrame:
        return self._aggregate_daily_volume(
            self.twse_volume_buy_data,
            self.tpex_volume_buy_data
        )

    def get_volume_sell(self) -> pd.DataFrame:
        return self._aggregate_daily_volume(
            self.twse_volume_sell_data,
            self.tpex_volume_sell_data
        )

    def get_volume_total_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_volume_df: pd.DataFrame = pd.DataFrame(
            self.twse_volume_total_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_volume_df: pd.DataFrame = pd.DataFrame(
            self.tpex_volume_total_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_volume_df: pd.DataFrame = pd.concat(
            [twse_volume_df, tpex_volume_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFrame = combined_volume_df[
            (combined_volume_df.index >= start_time) &
            (combined_volume_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').sum()
        resampled_data = resampled_data.reindex(columns=self.symbol_list, fill_value=np.nan)
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict


class read_feed_interval_spread:
    def __init__(self, date: str):
        self.date: str = date
        rpi = read_product_info()
        twse_data = rpi.get_symbol_name(date)
        self.symbol_list: list[str] = twse_data
        self.twse_symbol_list: list[str] = rpi.get_twse_symbol(date)
        self.tpex_symbol_list: list[str] = rpi.get_twse_symbol(date)
        self.twse_path: str = twse_spread_interval_path.format(date=self.date)
        self.tpex_path: str = tpex_spread_interval_path.format(date=self.date)

        with h5py.File(self.twse_path, "r") as h5file:
            self.twse_universe_data: list[str] = h5file["universe"][:]
            self.twse_universe_data = [item.decode("utf-8") for item in self.twse_universe_data]
            self.twse_spread_data: np.ndarray[np.float_] = h5file["time_weighted_spread"][:]
            self.timestamps: np.ndarray[np.int64] = h5file["timestamp"][:]

        with h5py.File(self.tpex_path, "r") as h5file:
            self.tpex_universe_data: list[str] = h5file["universe"][:]
            self.tpex_universe_data = [item.decode("utf-8") for item in self.tpex_universe_data]
            self.tpex_spread_data: np.ndarray[np.float_] = h5file["time_weighted_spread"][:]
            self.timestamps: np.ndarray[np.int64] = h5file["timestamp"][:]

    def get_spread(self) -> pd.DataFrame:
        twse_df: pd.DataFrame = pd.DataFrame(
            self.twse_spread_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_df: pd.DataFrame = pd.DataFrame(
            self.tpex_spread_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        combined_daily: pd.DataFrame = pd.concat([twse_df, tpex_df], axis=1)
        combined_daily = combined_daily.mean(axis=0, skipna=True)
        combined_daily = combined_daily.reindex(self.symbol_list, fill_value=0)

        combined_daily = pd.DataFrame(combined_daily).transpose()
        print(combined_daily)
        return combined_daily

    def get_spread_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_spread_df: pd.DataFrame = pd.DataFrame(
            self.twse_spread_data,
            columns=self.twse_universe_data,
            index=adjusted_timestamps
        )
        tpex_spread_df: pd.DataFrame = pd.DataFrame(
            self.tpex_spread_data,
            columns=self.tpex_universe_data,
            index=adjusted_timestamps
        )
        combined_spread_df: pd.DataFrame = pd.concat(
            [twse_spread_df, tpex_spread_df], axis=1
        )
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        trading_data: pd.DataFrame = combined_spread_df[
            (combined_spread_df.index >= start_time) &
            (combined_spread_df.index < end_time)
        ]
        resampled_data: pd.DataFrame = trading_data.resample('10min').mean()
        resampled_data = resampled_data.reindex(columns=self.symbol_list, fill_value=np.nan)
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict


class read_feed_interval_vwap:
    def __init__(self, date: str):
        self.date: str = date
        rpi = read_product_info()
        twse_data = rpi.get_symbol_name(date)
        self.symbol_list: list[str] = twse_data
        self.twse_symbol_list: list[str] = rpi.get_twse_symbol(date)
        self.tpex_symbol_list: list[str] = rpi.get_twse_symbol(date)
        self.twse_volume_path: str = twse_volume_interval_path.format(date=self.date)
        self.tpex_volume_path: str = tpex_volume_interval_path.format(date=self.date)
        self.twse_vwap_path: str = twse_vwap_interval_path.format(date=self.date)
        self.tpex_vwap_path: str = tpex_vwap_interval_path.format(date=self.date)

        with h5py.File(self.twse_volume_path, "r") as h5file:
            self.twse_universe_data: list[str] = h5file["universe"][:]
            self.twse_universe_data = [item.decode("utf-8") for item in self.twse_universe_data]
            self.twse_volume_total_data: np.ndarray[np.float_] = h5file["VOLUME"][:]
            self.timestamps: np.ndarray[np.int64] = h5file["timestamp"][:]

        with h5py.File(self.twse_vwap_path, "r") as h5file:
            self.twse_vwap_total_data: np.ndarray[np.float_] = h5file["VWAP"][:]

        with h5py.File(self.tpex_volume_path, "r") as h5file:
            self.tpex_universe_data: list[str] = h5file["universe"][:]
            self.tpex_universe_data = [item.decode("utf-8") for item in self.tpex_universe_data]
            self.tpex_volume_total_data: np.ndarray[np.float_] = h5file["VOLUME"][:]

        with h5py.File(self.tpex_vwap_path, "r") as h5file:
            self.tpex_vwap_total_data: np.ndarray[np.float_] = h5file["VWAP"][:]

    def get_vwap(self) -> pd.DataFrame:
        twse_volume_df: pd.DataFrame = pd.DataFrame(
            self.twse_volume_total_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        twse_vwap_df: pd.DataFrame = pd.DataFrame(
            self.twse_vwap_total_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_volume_df: pd.DataFrame = pd.DataFrame(
            self.tpex_volume_total_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        tpex_vwap_df: pd.DataFrame = pd.DataFrame(
            self.tpex_vwap_total_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(self.timestamps)
        )
        combined_volume_df: pd.DataFrame = pd.concat([twse_volume_df, tpex_volume_df], axis=1)
        combined_vwap_df: pd.DataFrame = pd.concat([twse_vwap_df, tpex_vwap_df], axis=1)
        numerator: pd.DataFrame = (combined_vwap_df * combined_volume_df).sum(axis=0, skipna=True)
        denominator: pd.DataFrame = combined_volume_df.sum(axis=0, skipna=True)
        daily_vwap: pd.DataFrame = numerator / denominator
        daily_vwap = daily_vwap.reindex(self.symbol_list, fill_value=np.nan)
        daily_vwap_df = pd.DataFrame(daily_vwap).transpose()
        daily_vwap_df.index = [self.date]
        return daily_vwap_df

    def get_vwap_10min_interval(self) -> Dict[str, pd.DataFrame]:
        timestamps: pd.DatetimeIndex = (
            pd.to_datetime(self.timestamps, unit='ns', utc=True).tz_convert('Asia/Taipei')
        )
        adjusted_timestamps: pd.DatetimeIndex = timestamps - pd.Timedelta(minutes=1)
        twse_volume_df: pd.DataFrame = pd.DataFrame(
            self.twse_volume_total_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(adjusted_timestamps)
        )
        twse_vwap_df: pd.DataFrame = pd.DataFrame(
            self.twse_vwap_total_data,
            columns=self.twse_universe_data,
            index=pd.to_datetime(adjusted_timestamps)
        )
        tpex_volume_df: pd.DataFrame = pd.DataFrame(
            self.tpex_volume_total_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(adjusted_timestamps)
        )
        tpex_vwap_df: pd.DataFrame = pd.DataFrame(
            self.tpex_vwap_total_data,
            columns=self.tpex_universe_data,
            index=pd.to_datetime(adjusted_timestamps)
        )
        combined_volume_df: pd.DataFrame = pd.concat([twse_volume_df, tpex_volume_df], axis=1)
        combined_vwap_df: pd.DataFrame = pd.concat([twse_vwap_df, tpex_vwap_df], axis=1)
        start_time: pd.Timestamp = (
            timestamps[0].replace(hour=9, minute=0, second=0, microsecond=0)
        )
        end_time: pd.Timestamp = (
            timestamps[0].replace(hour=13, minute=25, second=0, microsecond=0)
        )
        combined_volume_df: pd.DataFrame = combined_volume_df[
            (combined_volume_df.index >= start_time) &
            (combined_volume_df.index < end_time)
        ]
        combined_vwap_df: pd.DataFrame = combined_vwap_df[
            (combined_vwap_df.index >= start_time) &
            (combined_vwap_df.index < end_time)
        ]
        resampled_volume_df: pd.DataFrame = combined_volume_df.resample('10T').sum()
        resampled_vwap_df = (
            (combined_vwap_df * combined_volume_df)
            .resample('10T')
            .sum()
            / resampled_volume_df
        )
        resampled_data = resampled_vwap_df.reindex(columns=self.symbol_list, fill_value=np.nan)
        result_dict: Dict[str, pd.DataFrame] = {}
        for start, end in zip(resampled_data.index[:-1], resampled_data.index[1:]):
            key: str = f"T{start.strftime('%H%M%S')}_T{end.strftime('%H%M%S')}"
            result_dict[key] = resampled_data.loc[start:end].head(1)
        last_key: str = (
            f"T{resampled_data.index[-1].strftime('%H%M%S')}_"
            f"T{end_time.strftime('%H%M%S')}"
        )
        result_dict[last_key] = resampled_data.loc[resampled_data.index[-1]:]
        print(result_dict)
        return result_dict


def test():
    date = "20241120"
    reader = read_feed_interval_ohlc(date)
    reader.get_low_trade_10min_interval()

    # reader = read_feed_interval_ohlc(date)
    # reader.get_open_trade()


if __name__ == "__main__":
    test()

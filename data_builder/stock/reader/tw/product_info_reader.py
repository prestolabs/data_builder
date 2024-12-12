import h5py


twse_interval_path = (
    "/remote/iosg/data-2/buckets/feed.derived.interval_h5/navi/main/PT1M/TWSE/{date}/TWSE--ohlc.h5"
)
tpex_interval_path = (
    "/remote/iosg/data-2/buckets/feed.derived.interval_h5/navi/main/PT1M/TPEX/{date}/TPEX--ohlc.h5"
)


class read_product_info:

    def get_symbol_name(self, date) -> list[str]:
        tw_symbol_list: list[str] = []
        twse_symbol_list: list[str] = self.get_twse_symbol(date)
        tpex_symbol_list: list[str] = self.get_tpex_symbol(date)
        tw_symbol_list.extend(twse_symbol_list)
        tw_symbol_list.extend(tpex_symbol_list)
        return sorted(tw_symbol_list)

    def get_twse_symbol(self, date) -> list[str] | None:
        path: str = twse_interval_path.format(date=date)
        try:
            with h5py.File(path, "r") as h5file:
                data = h5file["universe"][:]
                string_data: list[str] = [item.decode("utf-8") for item in data]
                return string_data
        except FileNotFoundError:
            print(f"File not found: {path}")
            return None
        except Exception as e:
            print(f"An error occurred while accessing the file: {e}")
            return None

    def get_tpex_symbol(self, date) -> list[str] | None:
        path: str = tpex_interval_path.format(date=date)
        try:
            with h5py.File(path, "r") as h5file:
                data = h5file["universe"][:]
                string_data: list[str] = [item.decode("utf-8") for item in data]
                return string_data
        except FileNotFoundError:
            print(f"File not found: {path}")
            return None
        except Exception as e:
            print(f"An error occurred while accessing the file: {e}")
            return None


def test():
    rpi = read_product_info()
    date = "20241120"
    twse_data = rpi.get_symbol_name(date)
    print(twse_data)
    print(len(twse_data))


if __name__ == "__main__":
    test()

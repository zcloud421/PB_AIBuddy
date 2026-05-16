import sys


def main() -> int:
    try:
        import akshare as ak
    except ImportError:
        print("akshare is not installed. Run: pip3 install akshare")
        return 1

    print("== HK index spot sample ==")
    try:
        spot_df = ak.stock_hk_index_spot_sina()
        print(spot_df.head(20).to_string(index=False))
    except Exception as exc:
        print(f"stock_hk_index_spot_sina failed: {exc}")
        return 1

    print("\n== Candidate symbols containing 恒生 / Hang Seng / 科技 ==")
    name_col = None
    symbol_col = None
    for col in spot_df.columns:
        col_str = str(col)
        if col_str in {"名称", "name"}:
            name_col = col
        if col_str in {"代码", "symbol"}:
            symbol_col = col

    if name_col is not None:
        mask = spot_df[name_col].astype(str).str.contains("恒生|Hang Seng|科技", case=False, regex=True)
        print(spot_df.loc[mask].to_string(index=False))
    else:
        print("Could not find name column in spot dataframe.")

    test_symbols = ["HSI", "HSTECH", "HSTECHINDEX", "HSTECHI"]
    if symbol_col is not None:
        test_symbols.extend(spot_df.loc[
            spot_df[name_col].astype(str).str.contains("恒生指数|恒生科技", case=False, regex=True),
            symbol_col,
        ].astype(str).tolist())

    tested = set()
    print("\n== Daily history checks ==")
    for symbol in test_symbols:
        if not symbol or symbol in tested:
            continue
        tested.add(symbol)
        try:
            daily_df = ak.stock_hk_index_daily_sina(symbol=symbol)
            print(f"{symbol}: ok, rows={len(daily_df)}")
            print(daily_df.tail(3).to_string(index=False))
        except Exception as exc:
            print(f"{symbol}: failed ({exc})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

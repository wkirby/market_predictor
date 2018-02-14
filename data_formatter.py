import sys
import argparse
import logging
import pandas as pd
import numpy as np
import datetime
import math

WINDOW_INCREMENT_MINUTES = 10
WINDOW_INCREMENT_SECONDS = WINDOW_INCREMENT_MINUTES * 60


def safe_list_get(l, idx, default):
    try:
        return l[idx]
    except IndexError:
        return default


def roundTime(dt=None, roundTo=60):
    """Round a datetime object to any time laps in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
    """
    if dt is None: dt = datetime.datetime.now()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)


def toMinute(row):
    return roundTime(
      datetime.datetime.fromtimestamp(int(row)),
      roundTo=WINDOW_INCREMENT_SECONDS
    )


def getTrailingPrices(idxs, key, fn):
    output = {}
    for idx, val in enumerate(idxs):
        new_key = key + "_" + str(idx)
        output[new_key] = fn(val)
    return output


def getTrailingPrice(src, idx, key = 'PRICE'):
    if idx < 0:
        return None
    else:
        return dict(safe_list_get(src, idx, {})).get(key)


def intlogspace(max=120, n=10):
    ns = np.logspace(0, 1, base=max+1, num=n).tolist()
    return map(lambda n: math.floor(n), ns)


def main(args, logLevel):
    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)

    # READ CSV
    data = pd.read_csv(args.input)

    # FORMAT DATA
    data['TIME'] = data['TIME'].map(toMinute)
    grouped = data.groupby(["TIME"])

    formatted_data = []
    for time, group in grouped:
        formatted_data.append({
            'TIME': time,
            'PRICE': group['PRICE'].iloc[-1],
            'VOLUME': group['VOLUME'].sum(),
            'NUM_TRADES': group['PRICE'].count()
        })

    # BUILD TIME SERIES
    now = data['TIME'][0]
    end = data['TIME'][data['TIME'].size - 1]

    output = []
    output_idx = 0

    while now <= end:
        previous_idx = max(0, output_idx - 1)

        trailing_deltas = intlogspace()
        trailing_idxs = map(lambda n: math.floor(output_idx - n), trailing_deltas)
        result = list(filter(lambda t: t['TIME'] == now, formatted_data))

        if result:
            new_entry = result[0]
        else:
            last_result = dict(output[previous_idx])
            last_result['NUM_TRADES'] = 0
            last_result['VOLUME'] = 0
            last_result['TIME'] = now
            new_entry = last_result

        trailing_price_entries = getTrailingPrices(
            trailing_idxs,
            "TRAILING_PRICE",
            lambda i: getTrailingPrice(output, i)
        )

        new_entry.update(trailing_price_entries)

        logging.info("Adding entry: " + str(new_entry['TIME']))
        output.append(new_entry)
        now += datetime.timedelta(minutes=WINDOW_INCREMENT_MINUTES)
        output_idx += 1

    for idx, val in enumerate(output):
        if idx+1 < len(output):
            val['PRICE'] = output[idx+1]['PRICE']
        else:
            val['PRICE'] = None

    # Output data
    output_data = pd.DataFrame(output)
    logging.info("Output to " + args.output)
    output_data.to_csv(args.output, index=False)


# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Transform bitcoin CSV to useable data",
        fromfile_prefix_chars='@'
    )

    # TODO Specify your real parameters here.
    parser.add_argument("input", help="input csv file")
    parser.add_argument("-o", "--output", help="output csv file")
    parser.add_argument(
                      "-v",
                      "--verbose",
                      help="increase output verbosity",
                      action="store_true")
    args = parser.parse_args()

    # Setup logging
    if args.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    main(args, loglevel)

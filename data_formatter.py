# Import
import pandas as pd
import datetime

WINDOW_INCREMENT_MINUTES = 10
WINDOW_INCREMENT_SECONDS = WINDOW_INCREMENT_MINUTES * 60


# Import data
data = pd.read_csv('01_data/coinbase_subset.csv')


def safe_list_get (l, idx, default):
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
    if dt == None : dt = datetime.datetime.now()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

def toMinute(row):
    return roundTime(datetime.datetime.fromtimestamp(int(row)), roundTo=WINDOW_INCREMENT_SECONDS)

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
    trailing_idx_1 = output_idx - 2
    trailing_idx_2 = output_idx - 4
    trailing_idx_3 = output_idx - 8
    trailing_idx_4 = output_idx - 16

    # print("Trailing Indexes: ", previous_idx, trailing_idx_1, trailing_idx_2, trailing_idx_3, trailing_idx_4)

    result = list(filter(lambda t: t['TIME'] == now, formatted_data))

    if result:
        new_entry = result[0]
    else:
        last_result = dict(output[previous_idx])
        last_result['NUM_TRADES'] = 0
        last_result['VOLUME'] = 0
        last_result['TIME'] = now
        new_entry = last_result


    new_entry['TRAILING_PRICE_1'] = previous_idx < 0    if None else dict(safe_list_get(output, previous_idx, {})).get('PRICE')
    new_entry['TRAILING_PRICE_2'] = trailing_idx_1 < 0  if None else dict(safe_list_get(output, trailing_idx_1, {})).get('PRICE')
    new_entry['TRAILING_PRICE_3'] = trailing_idx_2 < 0  if None else dict(safe_list_get(output, trailing_idx_2, {})).get('PRICE')
    new_entry['TRAILING_PRICE_4'] = trailing_idx_3 < 0  if None else dict(safe_list_get(output, trailing_idx_3, {})).get('PRICE')
    new_entry['TRAILING_PRICE_5'] = trailing_idx_4 < 0  if None else dict(safe_list_get(output, trailing_idx_4, {})).get('PRICE')

    output.append(new_entry)
    now += datetime.timedelta(minutes=WINDOW_INCREMENT_MINUTES)
    output_idx += 1

output_data = pd.DataFrame(output)
output_data.to_csv("01_data/coinbase_subset_formatted.csv")

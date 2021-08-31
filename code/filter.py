import apache_beam as beam


def  mcap_more_than_ten_million(item):
    return item['mcap_in_million'] >  10

with beam.Pipeline() as p:
    filter_large_cap_stocks =(
        p
        |
        'create'>> beam.Create(
            [
                            {
                            'name':'ICICI',
                            'mcap_in_million':5,
                            'stock_price':550,
                            'date':'2021-03-25'
                            },
                           {
                            'name':'HDFC',
                            'mcap_in_million':15,
                            'stock_price':1550,
                            'date':'2021-03-25'
                            },
                            {
                            'name':'SBI',
                            'mcap_in_million':2,
                            'stock_price':350,
                            'date':'2021-03-28'
                            }
            ]
            |'filter'>> beam.Filter(mcap_more_than_ten_million)
            |'print'>>beam.Map(print)
        )
    )
import apache_beam as beam


class GetTimestamp(beam.DoFn):
    def process(self, element, timestamp = beam.DoFn.TimestampParam):
        yield '{} - {}'.format(timestamp.to_utc_datetime(),element['name']+' - Price '+str(element['price']))
        

with beam.Pipeline() as p:
    stocks = (
        p
        | 'create'>> beam.Create(
            [
                {
                'name':'icici',
                'timestamp':1585699200,
                'price':551.22
                },
                {
                'name':'hdfc',
                'timestamp':1590969600,
                'price':1251.22
                },
                {
                'name':'sbi',
                'timestamp':1598918400,
                'price':351.22
                }
            ]
        )
        |'with timestamp'>> beam.Map(
            lambda stock : beam.transforms.window.TimestampedValue(stock, stock['timestamp'])
        )
        |'Get timestamp'>>beam.ParDo(GetTimestamp())
        |'print'>>beam.Map(print)
    )
#Output
#2020-04-01 00:00:00 - icici - Price  551.22
#2020-06-01 00:00:00 - hdfc - Price  1251.22
#2020-09-01 00:00:00 - sbi - Price  351.22
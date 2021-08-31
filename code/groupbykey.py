import apache_beam as beam


with beam.Pipeline() as p:
    group_stocks_by_date_name = (
        p
        | 'create'>>beam.Create(
            [
                {
                    'name':'hfdc',
                    'date':'2020-03-21',
                    'price':1450
                },
                {
                    'name':'hfdc',
                    'date':'2020-03-21',
                    'price':1465
                },
                {
                    'name':'hdfc',
                    'date':'2020-04-21',
                    'price':1550
                },
                {
                    'name':'icici',
                    'date':'2020-03-21',
                    'price':550
                }
            ]
        )
        | 'selective details'>> beam.Map(lambda item : ((item['name'],item['date']),item['price']))
        | 'group by key'>>beam.GroupByKey()
        | 'print'>>beam.Map(print)
    )
#Output
#(('hfdc', '2020-03-21'), [1450, 1465])
#(('hdfc', '2020-04-21'), [1550])
#(('icici', '2020-03-21'), [550])
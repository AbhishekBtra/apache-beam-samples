import apache_beam as beam


with beam.Pipeline() as p:
    stocks = (
        p |
        'Tech Stocks' >> beam.Create([
            '           TCS ',
            '   Infosys             ',
            'Wipro                          '            
        ])
        |'Strip'>>beam.Map(str.strip)
        |'print'>>beam.Map(print)
    )
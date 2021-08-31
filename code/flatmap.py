import apache_beam as beam


with beam.Pipeline() as p:
    stocks = (
        p
        | 'create' >> beam.Create(
            [
                'TCS Infosys           Wipro',
                'HUL HDFC',
                'ICICI'
            ]
        )
        |'split'>>beam.FlatMap(str.split)
        |'print'>>beam.Map(print)
    )
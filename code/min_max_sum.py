import apache_beam as beam


with beam.Pipeline() as p:
    random = (
        p
        |'create'>> beam.Create([50,55,40,6,25])
        |'min'>> beam.CombineGlobally(lambda item : min(item or [-1]))
        |'print'>>beam.Map(print)
    )
#Output = 6